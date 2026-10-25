import asyncio
import dataclasses
import gc
import uuid
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, Interceptor, WorkflowFailureError, WorkflowHandle
from temporalio.common import RawValue, RetryPolicy, streams
from temporalio.common.streams import _plugin
from temporalio.common.streams._extension import (
    ExtensionStreamBackend,
    _WorkflowInbound,
)
from temporalio.contrib import stream as redis
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.worker import (
    ExecuteWorkflowInput,
    Replayer,
    UnsandboxedWorkflowRunner,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)
from temporalio.worker import Interceptor as WorkerInterceptor
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)
from tests.contrib.streams._support import on_native
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.ActivityStream[Events]


def workflow_stream(
    handle: WorkflowHandle[Any, Any], name: str | None = None
) -> streams.ClientStream[Events]:
    return handle.stream(streams.with_name(Events, name) if name else Events)


async def read_all(stream: streams.ClientStream[Any]) -> list[Any]:
    return [item async for item in stream.subscribe(create_wait=timedelta(seconds=20))]


def records(items: list[Any]) -> list[streams.Record[Any]]:
    return [item for item in items if isinstance(item, streams.Record)]


@activity.defn
async def call_model(input: CallModelInput) -> str:
    tokens = input.stream.topic(Events.tokens)
    for word in input.prompt.split():
        await tokens.publish(word)
    return input.prompt.upper()


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        stream = await workflow.create_stream(Events)
        turns = stream.topic(Events.turns)
        await turns.publish(Turn("model"))
        reply = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await turns.publish(Turn("done", reply))
        await stream.close(result=reply)
        return reply


async def test_workflow_and_activity_write_one_stream(stream_client: Client):
    async with new_worker(stream_client, Agent, activities=[call_model]) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = workflow_stream(handle)
        items = await read_all(stream)
        assert await handle.result() == "HI THERE"
    data = [(item.topic, item.data) for item in records(items)]
    assert data == [
        ("turns", Turn("model")),
        ("tokens", "hi"),
        ("tokens", "there"),
        ("turns", Turn("done", "HI THERE")),
    ]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "HI THERE"
    only_turns = [item async for item in stream.subscribe(Events.turns)]
    assert [type(item) for item in only_turns] == [
        streams.Record,
        streams.Record,
        streams.Closed,
    ]


attempts: dict[str, int] = {}


@workflow.defn
class PublishThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(Events)
        turns = stream.topic(Events.tokens)
        await turns.publish("before")
        await workflow.sleep(timedelta(milliseconds=10))
        await turns.publish("after")
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")
        await stream.close()


async def test_failed_workflow_task_is_withdrawn(stream_client: Client):
    key = str(uuid.uuid4())
    # The test's counter must survive the retry of the task, so no sandbox.
    async with new_worker(
        stream_client,
        PublishThenFailTask,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            PublishThenFailTask.run,
            key,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await read_all(workflow_stream(handle))
    # The retried task writes "after" again, and withdraws the copy of the failed task.
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [record.data for record in written] == ["before", "after", "after"]
    withdrawn = [r for r in written if any(i.withdraws(r) for i in superseded)]
    assert withdrawn == [written[1]]
    assert isinstance(items[-1], streams.Closed)


async def test_resume_after_a_superseded_item_keeps_its_record(stream_client: Client):
    key = str(uuid.uuid4())
    async with new_worker(
        stream_client,
        PublishThenFailTask,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            PublishThenFailTask.run,
            key,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
    stream = workflow_stream(handle)
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    shared = superseded_of_the_retry(stream_client, items)
    reader = stream.subscribe(wait_new_records=False)
    async for item in reader:
        if item == shared:
            break
    resume_at = reader.next_offset
    await reader.aclose()
    assert resume_at == shared.offset
    resumed = [
        item
        async for item in stream.subscribe(offset=resume_at, wait_new_records=False)
    ]
    assert [r.data for r in records(resumed)][:1] == ["after"]


def superseded_of_the_retry(client: Client, items: list[Any]) -> streams.Superseded:
    # The retried task's batch is its Superseded item and then its Record. On Redis and memory
    # both are one entry with one offset. The native server gives each entry its own offset.
    after = max(
        i for i, item in enumerate(items) if getattr(item, "data", None) == "after"
    )
    shared = items[after - 1]
    assert isinstance(shared, streams.Superseded)
    if not on_native(client):
        assert shared.offset == items[after].offset
    return shared


@workflow.defn
class StopsAtASuperseded:
    @workflow.run
    async def run(
        self, stream: streams.WorkflowStream[Events], offset: int
    ) -> int | None:
        items = stream.subscribe(wait_new_records=False)
        async for item in items:
            if isinstance(item, streams.Superseded) and item.offset == offset:
                break
        return items.next_offset


async def test_a_workflow_read_resumes_at_a_superseded_item(stream_client: Client):
    # A Workflow read is a Subscription too, so a Workflow that stops after a Superseded item
    # and continues as new keeps the Record that shares its offset.
    key = str(uuid.uuid4())
    async with new_worker(
        stream_client,
        PublishThenFailTask,
        StopsAtASuperseded,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            PublishThenFailTask.run,
            key,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        stream = workflow_stream(handle)
        items = [item async for item in stream.subscribe(wait_new_records=False)]
        shared = superseded_of_the_retry(stream_client, items)
        assert (
            await stream_client.execute_workflow(
                StopsAtASuperseded.run,
                args=[
                    streams.WorkflowStream.from_handle(stream),
                    shared.offset,
                ],
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == shared.offset
        )


@activity.defn
async def noop() -> None:
    pass


@workflow.defn
class RandomAroundActivity:
    @workflow.run
    async def run(self) -> bool:
        before = workflow.random().getstate()
        handle = workflow.start_activity(
            noop, start_to_close_timeout=timedelta(seconds=30)
        )
        same = workflow.random().getstate() == before
        await handle
        return same


async def test_backend_leaves_the_workflow_random_alone(stream_client: Client):
    async with new_worker(
        stream_client, RandomAroundActivity, activities=[noop]
    ) as worker:
        assert await stream_client.execute_workflow(
            RandomAroundActivity.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )


@workflow.defn
class WritesThenContinues:
    @workflow.run
    async def run(self, write: bool) -> str:
        if write:
            stream = await workflow.create_stream(Events)
            await stream.topic(Events.tokens).publish("first run")
            workflow.continue_as_new(False)
        await workflow.stream_handle(Events).close(result="second run")
        return "second run"


async def test_later_run_closes_a_stream_it_did_not_write(stream_client: Client):
    async with new_worker(stream_client, WritesThenContinues) as worker:
        handle = await stream_client.start_workflow(
            WritesThenContinues.run,
            True,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "second run"
        items = await read_all(workflow_stream(handle))
    assert [i.data for i in records(items)] == ["first run"]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "second run"


@workflow.defn
class CancelledBlock:
    @workflow.run
    async def run(self) -> None:
        async with await workflow.create_stream(
            streams.with_name(Events, "turn")
        ) as turn:
            await turn.topic(Events.tokens).publish("started")
            await workflow.wait_condition(lambda: False)


async def test_cancelled_block_closes_with_a_failure(stream_client: Client):
    async with new_worker(stream_client, CancelledBlock) as worker:
        handle = await stream_client.start_workflow(
            CancelledBlock.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        turn = workflow_stream(handle, "turn")
        reader = turn.subscribe(create_wait=timedelta(seconds=20))
        assert isinstance(await anext(reader), streams.Record)
        await reader.aclose()
        await handle.cancel()
        items = await read_all(turn)
    closed = items[-1]
    assert isinstance(closed, streams.Closed)
    assert closed.failure is not None
    assert closed.status == streams.CloseStatus.CANCELED


@streams.defn(name="plain")
class Plain:
    t = streams.topic(str)


@workflow.defn
class PlainWriter:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Plain)
        await stream.topic(Plain.t).publish("plain")
        async with await workflow.create_stream(
            streams.with_name(Plain, "scoped")
        ) as scoped:
            await scoped.topic(Plain.t).publish("scoped")
        await stream.close(result="done")
        return "done"


async def test_temporal_only_passthrough_keeps_records(stream_client: Client):
    # Only the minimum modules and temporalio pass through. Values are plain, since a
    # dataclass from a sandbox with its own dataclasses module can't convert outside it.
    runner = SandboxedWorkflowRunner(
        restrictions=dataclasses.replace(
            SandboxRestrictions.default,
            passthrough_modules=SandboxRestrictions.passthrough_modules_with_temporal,
        )
    )
    async with new_worker(stream_client, PlainWriter, workflow_runner=runner) as worker:
        handle = await stream_client.start_workflow(
            PlainWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        items = await read_all(handle.stream(Plain))
        scoped = await read_all(handle.stream(streams.with_name(Plain, "scoped")))
    assert [i.data for i in records(items)] == ["plain"]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"
    assert [i.data for i in records(scoped)] == ["scoped"]
    assert isinstance(scoped[-1], streams.Closed)


@activity.defn
async def local_tokens(input: CallModelInput) -> None:
    await input.stream.topic(Events.tokens).publish(*input.prompt.split())


@workflow.defn
class LocalWriter:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Events)
        await workflow.execute_local_activity(
            local_tokens,
            CallModelInput("0 1", streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.close()


async def test_local_activity_writes_with_the_task_epoch(stream_client: Client):
    async with new_worker(stream_client, LocalWriter, activities=[local_tokens]) as w:
        handle = await stream_client.start_workflow(
            LocalWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=w.task_queue
        )
        await handle.result()
        items = await read_all(workflow_stream(handle))
    written = records(items)
    assert [record.data for record in written] == ["0", "1"]
    producer = written[0].producer
    assert producer is not None and producer.id.startswith("la:")
    assert (
        producer.epoch is not None and (producer.epoch.task_started_event_id or 0) > 0
    )


@workflow.defn
class LocalThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(Events)
        first = attempts.setdefault(key, 0) == 0
        await workflow.execute_local_activity(
            local_tokens,
            CallModelInput(
                "0 1" if first else "2 3", streams.ActivityStream.from_handle(stream)
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )
        if first:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")
        await stream.close()


async def test_rerun_local_activity_withdraws_its_failed_task(stream_client: Client):
    async with new_worker(
        stream_client,
        LocalThenFailTask,
        activities=[local_tokens],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            LocalThenFailTask.run,
            str(uuid.uuid4()),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await read_all(workflow_stream(handle))
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    visible = [r for r in written if not any(i.withdraws(r) for i in superseded)]
    assert [r.data for r in visible] == ["2", "3"]
    # The rerun keeps the Producer id and moves to the retry task's epoch.
    first, second = written[0].producer, visible[0].producer
    assert first is not None and second is not None and first.id == second.id
    assert first.epoch is not None and second.epoch is not None
    assert (first.epoch.task_attempt, first.epoch.task_started_event_id) < (
        second.epoch.task_attempt,
        second.epoch.task_started_event_id,
    )


@workflow.defn
class LocalOnlyInFailedTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(Events)
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            await workflow.execute_local_activity(
                local_tokens,
                CallModelInput("0 1", streams.ActivityStream.from_handle(stream)),
                start_to_close_timeout=timedelta(seconds=30),
            )
            raise RuntimeError("fail this Workflow Task once")
        await stream.topic(Events.tokens).publish("other")
        await stream.close()


async def test_later_write_withdraws_a_skipped_local_activity(stream_client: Client):
    # The retry task skips the Local Activity, so only its own write can withdraw "0 1". The
    # write also takes the skipped call's key.
    async with new_worker(
        stream_client,
        LocalOnlyInFailedTask,
        activities=[local_tokens],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            LocalOnlyInFailedTask.run,
            str(uuid.uuid4()),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await read_all(workflow_stream(handle))
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    visible = [r for r in written if not any(s.withdraws(r) for s in superseded)]
    assert [r.data for r in visible] == ["other"]
    assert isinstance(items[-1], streams.Closed)


@workflow.defn
class FailingRun:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Events)
        info = workflow.info()
        await stream.topic(Events.tokens).publish(f"run-{info.attempt}")
        if info.attempt == 1:
            raise ApplicationError("first run fails")
        await stream.close(result="done")
        return "done"


async def test_retry_run_withdraws_the_failed_run(stream_client: Client):
    async with new_worker(stream_client, FailingRun) as worker:
        handle = await stream_client.start_workflow(
            FailingRun.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            ),
        )
        assert await handle.result() == "done"
        items = await read_all(workflow_stream(handle))
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [record.data for record in written] == ["run-1", "run-2"]
    assert len(superseded) == 1
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["run-1"]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "done"


class _Rot13Codec(PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/rot"}, data=p.SerializeToString()[::-1]
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out = []
        for p in payloads:
            if p.metadata.get("encoding") != b"binary/rot":
                out.append(p)
                continue
            inner = Payload()
            inner.ParseFromString(p.data[::-1])
            out.append(inner)
        return out


async def test_records_go_through_the_payload_codec(stream_client: Client):
    config = stream_client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=_Rot13Codec()
    )
    codec_client = Client(**config)
    async with new_worker(codec_client, Agent, activities=[call_model]) as worker:
        handle = await codec_client.start_workflow(
            Agent.run, "a b", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        stream = workflow_stream(handle)
        decoded = await read_all(stream)
        # A Topic outside the definition reads the raw Payloads. The handle keeps the owner
        # fields, since a native server finds a Workflow's Stream only by them.
        plain = codec_client.stream(Plain, stream=stream)
        raw = [
            item
            async for item in plain.subscribe(
                plain.topic("tokens", type=RawValue), wait_new_records=False
            )
        ]
    assert [item.data for item in records(decoded)] == [
        Turn("model"),
        "a",
        "b",
        Turn("done", "A B"),
    ]
    first = raw[0]
    assert isinstance(first, streams.Record)
    assert first.data.payload.metadata["encoding"] == b"json/plain"


async def test_client_close_goes_through_the_payload_codec(stream_client: Client):
    config = stream_client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=_Rot13Codec()
    )
    codec_client = Client(**config)
    done = await codec_client.create_stream(Events, id=f"done-{uuid.uuid4()}")
    await done.close(result=Turn("done"))
    failed = await codec_client.create_stream(Events, id=f"failed-{uuid.uuid4()}")
    await failed.close(failure=ApplicationError("bad turn"))
    [closed] = [item async for item in done.subscribe()]
    assert isinstance(closed, streams.Closed)
    assert closed.result == {"kind": "done", "text": ""}
    [closed] = [item async for item in failed.subscribe()]
    assert isinstance(closed, streams.Closed) and str(closed.failure) == "bad turn"


@workflow.defn
class WaitsForSignal:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Events)
        turns = stream.topic(Events.tokens)
        await turns.publish("before restart")
        async with await workflow.create_stream(
            streams.with_name(Events, "scoped")
        ) as scoped:
            await scoped.topic(Events.tokens).publish("scoped")
        await workflow.wait_condition(lambda: self.go)
        await turns.publish("after restart")
        await stream.close()
        return "done"

    @workflow.signal
    def proceed(self) -> None:
        self.go = True


async def test_new_worker_replays_and_writes_each_record_once(stream_client: Client):
    task_queue = f"tq-{uuid.uuid4()}"
    async with new_worker(stream_client, WaitsForSignal, task_queue=task_queue):
        handle = await stream_client.start_workflow(
            WaitsForSignal.run, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        first = workflow_stream(handle)
        reader = first.subscribe(create_wait=timedelta(seconds=20))
        assert isinstance(await anext(reader), streams.Record)
        await reader.aclose()
    # The new Worker has no cache, so it replays the run and sends the creates and the close
    # of the scoped Stream again.
    async with new_worker(stream_client, WaitsForSignal, task_queue=task_queue):
        await handle.signal(WaitsForSignal.proceed)
        assert await handle.result() == "done"
        items = await read_all(first)
        scoped = await read_all(workflow_stream(handle, "scoped"))
    assert [i.data for i in records(items)] == ["before restart", "after restart"]
    assert isinstance(items[-1], streams.Closed)
    assert [i.data for i in records(scoped)] == ["scoped"]
    assert [type(i) for i in scoped].count(streams.Closed) == 1


async def test_backend_replays_a_history_made_without_it(
    client: Client, stream_client: Client
):
    async with new_worker(client, RandomAroundActivity, activities=[noop]) as worker:
        handle = await client.start_workflow(
            RandomAroundActivity.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result()
    history = await handle.fetch_history()
    replayer = Replayer(
        workflows=[RandomAroundActivity],
        stream_backends=stream_client.config()["stream_backends"],
    )
    await replayer.replay_workflow(history)


async def test_backend_shutdown_after_client_writes(stream_client: Client):
    backend = _plugin.backend_of(stream_client, None)
    assert isinstance(backend, ExtensionStreamBackend)
    stream = await stream_client.create_stream(Events, id=f"shutdown-{uuid.uuid4()}")
    async with streams.BufferedPublisher(stream.topic(Events.tokens)) as tokens:
        tokens.publish("buffered")
    await backend.shutdown()
    written = [
        item.data
        async for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, streams.Record)
    ]
    assert written == ["buffered"]


@workflow.defn
class WaitsForever:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("waiting")
        await workflow.wait_condition(lambda: False)


class _Suspends:
    def __await__(self) -> Generator[None, None, None]:
        yield


class _NeverEnds(WorkflowInboundInterceptor):
    def __init__(self) -> None:  # pyright: ignore[reportMissingSuperCall]
        pass

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        await _Suspends()


def test_closing_an_evicted_run_touches_no_runtime():
    # Python closes the coroutine of an evicted run with GeneratorExit, outside the
    # Workflow's loop. The interceptor must let it pass with no call to the runtime, or
    # Python prints "Exception ignored".
    inbound = _WorkflowInbound(_NeverEnds())
    run = inbound.execute_workflow(
        cast(ExecuteWorkflowInput, cast(object, SimpleNamespace(headers={})))
    )
    run.send(None)
    run.close()


@pytest.mark.parametrize("cache, safe", [(0, True), (10, True), (10, False)])
async def test_eviction_prints_nothing(
    stream_client: Client, capfd: pytest.CaptureFixture[str], cache: int, safe: bool
):
    # With no cache, each Workflow Task evicts the run. With a cache, the Worker's shutdown
    # evicts it. Either way the run's coroutine closes.
    async with new_worker(
        stream_client,
        WaitsForever,
        max_cached_workflows=cache,
        disable_safe_workflow_eviction=not safe,
    ) as w:
        handle = await stream_client.start_workflow(
            WaitsForever.run, id=f"wf-{uuid.uuid4()}", task_queue=w.task_queue
        )
        reader = workflow_stream(handle).subscribe(create_wait=timedelta(seconds=20))
        assert isinstance(await anext(reader), streams.Record)
        await reader.aclose()
    await handle.terminate()
    gc.collect()
    captured = capfd.readouterr()
    assert "Exception ignored" not in captured.err + captured.out


@workflow.defn
class AlwaysFails:
    @workflow.run
    async def run(self, non_retryable: bool) -> None:
        attempt = workflow.info().attempt
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish(f"run-{attempt}")
        raise ApplicationError(f"attempt {attempt} fails", non_retryable=non_retryable)


@pytest.mark.parametrize("non_retryable", [False, True])
async def test_final_failure_under_a_retry_policy(
    stream_client: Client, non_retryable: bool
):
    async with new_worker(stream_client, AlwaysFails) as worker:
        handle = await stream_client.start_workflow(
            AlwaysFails.run,
            non_retryable,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=3
            ),
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        stream = workflow_stream(handle)
        items = [item async for item in stream.subscribe(wait_new_records=False)]
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    # The end of the chain closes only a native Stream, with the last run's failure.
    closes = [item for item in items if isinstance(item, streams.Closed)]
    assert len(closes) == (1 if on_native(stream_client) else 0)
    if non_retryable:
        # A non-retryable failure ends the chain on attempt 1.
        assert [r.data for r in written] == ["run-1"]
        assert superseded == []
        return
    # Each retry run withdraws the run before it.
    assert [r.data for r in written] == ["run-1", "run-2", "run-3"]
    assert len(superseded) == 2
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["run-1"]
    assert [r.data for r in written if superseded[1].withdraws(r)] == ["run-2"]
    assert not any(s.withdraws(written[-1]) for s in superseded)


async def test_reader_stops_on_its_own_timer_and_resumes(stream_client: Client):
    stream = await stream_client.create_stream(Events, id=f"timer-{uuid.uuid4()}")
    first = await stream.topic(Events.tokens).publish("one")
    seen: list[streams.Record[Any]] = []

    async def read_until_quiet() -> None:
        async for item in stream.subscribe():
            assert isinstance(item, streams.Record)
            seen.append(item)

    # A read has no idle end, so a reader that wants one sets its own timer.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(read_until_quiet(), timeout=0.5)
    assert [item.offset for item in seen] == [first]
    await stream.topic(Events.tokens).publish("two")
    resumed = [
        item.data
        async for item in stream.subscribe(
            offset=seen[-1].offset + 1, wait_new_records=False
        )
        if isinstance(item, streams.Record)
    ]
    assert resumed == ["two"]


async def test_two_backends_on_one_client(client: Client, redis_url: str):
    # A client may carry two Stream backends, such as during a move from one backend to
    # another. Each handle carries its backend, so a call goes to the backend that has it.
    config = client.config()
    config["stream_backends"] = [
        RedisStreamBackend(redis_url, key_prefix=f"t-{uuid.uuid4()}", default=True),
        MemoryStreamBackend(),
    ]
    stream_client = Client(**config)
    stream_id = f"two-{uuid.uuid4()}"
    on_redis = await redis.create(stream_client, Events, id=stream_id)
    on_memory = await redis.create(
        stream_client, Events, id=stream_id, backend="memory"
    )
    assert (on_redis.backend, on_memory.backend) == ("redis", "memory")
    await on_redis.topic(Events.tokens).publish("redis")
    await on_memory.topic(Events.tokens).publish("memory")
    for backend, expected in (("redis", "redis"), ("memory", "memory")):
        again = stream_client.stream(Events, id=stream_id, backend=backend)
        data = [
            item.data
            async for item in again.subscribe(wait_new_records=False)
            if isinstance(item, streams.Record)
        ]
        assert data == [expected]


failed_last_task: set[str] = set()


class _FailsTheLastTaskOnce(Interceptor, WorkerInterceptor):
    # On the client before the Stream backend's interceptor, so it is the outer Workflow
    # interceptor and fails the task after the backend drained the run's writes.
    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor]:
        return _FailsTheLastTaskOnceInbound


class _FailsTheLastTaskOnceInbound(WorkflowInboundInterceptor):
    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        result = await super().execute_workflow(input)
        workflow_id = workflow.info().workflow_id
        if workflow_id not in failed_last_task:
            failed_last_task.add(workflow_id)
            raise RuntimeError("fail the last Workflow Task once")
        return result


@workflow.defn
class PublishesClosesAndReturns:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("token")
        await stream.close(result="done")
        return "done"


async def close_activity_events(
    handle: WorkflowHandle[Any, Any],
) -> list[EventType.ValueType]:
    # The events of the close command, from its schedule on: the Activity on Redis, and the
    # System Nexus operation on native.
    history = await handle.fetch_history()
    scheduled = {
        event.event_id
        for event in history.events
        if event.activity_task_scheduled_event_attributes.activity_type.name
        == "__temporal_streams_close"
        or event.nexus_operation_scheduled_event_attributes.operation == "CloseStream"
    }
    return [
        event.event_type
        for event in history.events
        if event.event_id in scheduled
        or event.activity_task_started_event_attributes.scheduled_event_id in scheduled
        or event.activity_task_completed_event_attributes.scheduled_event_id
        in scheduled
        or event.nexus_operation_started_event_attributes.scheduled_event_id
        in scheduled
        or event.nexus_operation_completed_event_attributes.scheduled_event_id
        in scheduled
    ]


@pytest.mark.parametrize("cache", [0, 10])
async def test_close_survives_a_failed_last_task(stream_client: Client, cache: int):
    # The close is an Activity, committed with the Workflow Task that scheduled it. The last
    # task fails after it, and its retry replays the close and writes nothing more.
    config = stream_client.config()
    config["interceptors"] = [_FailsTheLastTaskOnce()]
    client = Client(**config)
    async with new_worker(
        client,
        PublishesClosesAndReturns,
        max_cached_workflows=cache,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await client.start_workflow(
            PublishesClosesAndReturns.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "done"
        stream = workflow_stream(handle)
        items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [r.data for r in records(items)] == ["token"]
    closes = [item for item in items if isinstance(item, streams.Closed)]
    assert len(closes) == 1 and items[-1] is closes[0]
    assert closes[0].result == "done"
    assert closes[0].status == streams.CloseStatus.COMPLETED
    assert await close_activity_events(handle) == (
        [
            EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
            EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
        ]
        if on_native(stream_client)
        else [
            EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
        ]
    )


@workflow.defn
class WritesToStream:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> str:
        try:
            await stream.topic(Events.tokens).publish("from workflow")
        except streams.StreamError as err:
            return err.reason
        return "wrote"


async def test_write_to_a_missing_stream_raises_at_the_await(stream_client: Client):
    async with new_worker(stream_client, WritesToStream) as worker:
        assert (
            await stream_client.execute_workflow(
                WritesToStream.run,
                streams.WorkflowStream(id=f"missing-{uuid.uuid4()}"),
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "stream_not_found"
        )


async def test_write_to_a_closed_stream_raises_at_the_await(stream_client: Client):
    stream = await stream_client.create_stream(Events, id=f"closed-{uuid.uuid4()}")
    await stream.close()
    async with new_worker(stream_client, WritesToStream) as worker:
        assert (
            await stream_client.execute_workflow(
                WritesToStream.run,
                streams.WorkflowStream.from_handle(stream),
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "stream_closed"
        )
    items = [item async for item in stream.subscribe()]
    assert len(items) == 1 and isinstance(items[0], streams.Closed)


@activity.defn
async def unknown_topic(input: CallModelInput) -> str:
    try:
        input.stream.topic("missing")
    except streams.StreamError as err:
        return err.reason
    return "found"


@workflow.defn
class UnknownTopic:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Events)
        return await workflow.execute_activity(
            unknown_topic,
            CallModelInput("", streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )


async def test_activity_refuses_a_topic_outside_the_definition(stream_client: Client):
    async with new_worker(stream_client, UnknownTopic, activities=[unknown_topic]) as w:
        assert (
            await stream_client.execute_workflow(
                UnknownTopic.run, id=f"wf-{uuid.uuid4()}", task_queue=w.task_queue
            )
            == "unknown_topic"
        )


@workflow.defn
class ContinuesThenRetries:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events] | None) -> str:
        if stream is None:
            stream = await workflow.create_stream(Events)
            await stream.topic(Events.tokens).publish("first")
            workflow.continue_as_new(stream)
        info = workflow.info()
        await stream.topic(Events.tokens).publish(f"attempt-{info.attempt}")
        if info.attempt == 1:
            raise ApplicationError("the continued run fails once")
        await stream.close(result="done")
        return "done"


async def test_retry_of_a_continued_run_writes_the_same_stream(stream_client: Client):
    async with new_worker(stream_client, ContinuesThenRetries) as worker:
        handle = await stream_client.start_workflow(
            ContinuesThenRetries.run,
            None,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            ),
        )
        assert await handle.result() == "done"
        items = await read_all(workflow_stream(handle))
    written = records(items)
    assert [r.data for r in written] == ["first", "attempt-1", "attempt-2"]
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["attempt-1"]
    closes = [item for item in items if isinstance(item, streams.Closed)]
    assert len(closes) == 1 and closes[0].result == "done"
