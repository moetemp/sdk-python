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

from temporalio import activity, common, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType, StreamErrorReason
from temporalio.client import Client, Interceptor, WorkflowFailureError, WorkflowHandle
from temporalio.common import RawValue, RetryPolicy
from temporalio.common._streams._extension import (
    ExtensionStreamBackend,
    _WorkflowInbound,
)
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
from tests.contrib.streams._support import workflow_stream
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@common.defn(name="tokens")
class Tokens(common.StreamDefinition[str]): ...


@common.defn(name="scoped")
class Scoped(common.StreamDefinition[str]): ...


@dataclass
class CallModelInput:
    prompt: str
    stream: common.ActivityStream[str]


async def read_all(stream: common.ClientStream[Any]) -> list[Any]:
    return [item async for item in stream.subscribe(create_wait=timedelta(seconds=20))]


def records(items: list[Any]) -> list[common.Record[Any]]:
    return [item for item in items if isinstance(item, common.Record)]


@activity.defn
async def call_model(input: CallModelInput) -> str:
    for word in input.prompt.split():
        await input.stream.publish(word)
    return input.prompt.upper()


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        turns = await workflow.create_stream(Turns)
        tokens = await workflow.create_stream(Tokens)
        await turns.publish(Turn("model"))
        reply = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, common.ActivityStream.from_handle(tokens)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await turns.publish(Turn("done", reply))
        await turns.close(result=reply)
        await tokens.close()
        return reply


attempts: dict[str, int] = {}


@workflow.defn
class PublishThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("before")
        await workflow.sleep(timedelta(milliseconds=10))
        await stream.publish("after")
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")
        await stream.close()


async def test_failed_workflow_task_is_withdrawn(stream_client: Client):
    key = str(uuid.uuid4())
    # The test's counter must keep its value across the task's retry, so the Workflow has no
    # sandbox.
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
        items = await read_all(workflow_stream(handle, Tokens))
    # The retried task writes "after" again, and withdraws the copy of the failed task.
    written = records(items)
    superseded = [item for item in items if isinstance(item, common.Superseded)]
    assert [record.data for record in written] == ["before", "after", "after"]
    withdrawn = [r for r in written if any(i.withdraws(r) for i in superseded)]
    assert withdrawn == [written[1]]
    assert isinstance(items[-1], common.Closed)


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
    stream = workflow_stream(handle, Tokens)
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    # The retried task's batch has its Superseded item and its first Record in one entry.
    record_offsets = {r.offset for r in records(items)}
    [shared] = [
        i
        for i in items
        if isinstance(i, common.Superseded) and i.offset in record_offsets
    ]
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
    assert [r.data for r in records(resumed) if r.offset == shared.offset] == ["after"]


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
            stream = await workflow.create_stream(Tokens)
            await stream.publish("first run")
            workflow.continue_as_new(False)
        await workflow.stream_handle(Tokens).close(result="second run")
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
        items = await read_all(workflow_stream(handle, Tokens))
    assert [i.data for i in records(items)] == ["first run"]
    closed = items[-1]
    assert isinstance(closed, common.Closed) and closed.result == "second run"


@workflow.defn
class CancelledBlock:
    @workflow.run
    async def run(self) -> None:
        async with await workflow.create_stream(Scoped) as turn:
            await turn.publish("started")
            await workflow.wait_condition(lambda: False)


async def test_cancelled_block_closes_with_a_failure(stream_client: Client):
    async with new_worker(stream_client, CancelledBlock) as worker:
        handle = await stream_client.start_workflow(
            CancelledBlock.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        turn = workflow_stream(handle, Scoped)
        reader = turn.subscribe(create_wait=timedelta(seconds=20))
        assert isinstance(await anext(reader), common.Record)
        await reader.aclose()
        await handle.cancel()
        items = await read_all(turn)
    closed = items[-1]
    assert isinstance(closed, common.Closed)
    assert closed.failure is not None
    assert closed.status == common.CloseStatus.CANCELED


@common.defn(name="plain")
class Plain(common.StreamDefinition[str]): ...


@workflow.defn
class PlainWriter:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Plain)
        await stream.publish("plain")
        async with await workflow.create_stream(Scoped) as scoped:
            await scoped.publish("scoped")
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
        items = await read_all(workflow_stream(handle, Plain))
        scoped = await read_all(workflow_stream(handle, Scoped))
    assert [i.data for i in records(items)] == ["plain"]
    assert isinstance(items[-1], common.Closed) and items[-1].result == "done"
    assert [i.data for i in records(scoped)] == ["scoped"]
    assert isinstance(scoped[-1], common.Closed)


@activity.defn
async def local_tokens(input: CallModelInput) -> None:
    await input.stream.publish(*input.prompt.split())


@workflow.defn
class LocalThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(Tokens)
        first = attempts.setdefault(key, 0) == 0
        await workflow.execute_local_activity(
            local_tokens,
            CallModelInput(
                "0 1" if first else "2 3", common.ActivityStream.from_handle(stream)
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
        items = await read_all(workflow_stream(handle, Tokens))
    written = records(items)
    superseded = [item for item in items if isinstance(item, common.Superseded)]
    visible = [r for r in written if not any(i.withdraws(r) for i in superseded)]
    assert [r.data for r in visible] == ["2", "3"]
    # The rerun keeps the Producer id and uses the retry task's epoch.
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
        stream = await workflow.create_stream(Tokens)
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            await workflow.execute_local_activity(
                local_tokens,
                CallModelInput("0 1", common.ActivityStream.from_handle(stream)),
                start_to_close_timeout=timedelta(seconds=30),
            )
            raise RuntimeError("fail this Workflow Task once")
        await stream.publish("other")
        await stream.close()


async def test_later_write_withdraws_a_skipped_local_activity(stream_client: Client):
    # The retry task skips the Local Activity, so only its own write can withdraw "0 1". The
    # write also uses the skipped call's key.
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
        items = await read_all(workflow_stream(handle, Tokens))
    written = records(items)
    superseded = [item for item in items if isinstance(item, common.Superseded)]
    visible = [r for r in written if not any(s.withdraws(r) for s in superseded)]
    assert [r.data for r in visible] == ["other"]
    assert isinstance(items[-1], common.Closed)


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


@common.defn(name="tokens")
class RawTokens(common.StreamDefinition[RawValue]): ...


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
        turns = await read_all(workflow_stream(handle, Turns))
        tokens = workflow_stream(handle, Tokens)
        decoded = await read_all(tokens)
        # A definition with RawValue as its payload type reads the raw Payloads.
        raw_tokens = codec_client.stream(RawTokens, id=tokens.id)
        raw = [item async for item in raw_tokens.subscribe(wait_new_records=False)]
    assert [item.data for item in records(turns)] == [
        Turn("model"),
        Turn("done", "A B"),
    ]
    assert [item.data for item in records(decoded)] == ["a", "b"]
    first = raw[0]
    assert isinstance(first, common.Record)
    assert first.data.payload.metadata["encoding"] == b"json/plain"


async def test_client_close_goes_through_the_payload_codec(stream_client: Client):
    config = stream_client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=_Rot13Codec()
    )
    codec_client = Client(**config)
    done = await codec_client.create_stream(Tokens, id=f"done-{uuid.uuid4()}")
    await done.close(result=Turn("done"))
    failed = await codec_client.create_stream(Tokens, id=f"failed-{uuid.uuid4()}")
    await failed.close(failure=ApplicationError("bad turn"))
    [closed] = [item async for item in done.subscribe()]
    assert isinstance(closed, common.Closed)
    assert closed.result == {"kind": "done", "text": ""}
    [closed] = [item async for item in failed.subscribe()]
    assert isinstance(closed, common.Closed) and str(closed.failure) == "bad turn"


@workflow.defn
class WaitsForSignal:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("before restart")
        async with await workflow.create_stream(Scoped) as scoped:
            await scoped.publish("scoped")
        await workflow.wait_condition(lambda: self.go)
        await stream.publish("after restart")
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
        first = workflow_stream(handle, Tokens)
        reader = first.subscribe(create_wait=timedelta(seconds=20))
        assert isinstance(await anext(reader), common.Record)
        await reader.aclose()
    # The new Worker has no cache, so it replays the run and sends the creates and the close
    # of the scoped Stream again.
    async with new_worker(stream_client, WaitsForSignal, task_queue=task_queue):
        await handle.signal(WaitsForSignal.proceed)
        assert await handle.result() == "done"
        items = await read_all(first)
        scoped = await read_all(workflow_stream(handle, Scoped))
    assert [i.data for i in records(items)] == ["before restart", "after restart"]
    assert isinstance(items[-1], common.Closed)
    assert [i.data for i in records(scoped)] == ["scoped"]
    assert [type(i) for i in scoped].count(common.Closed) == 1


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
    [backend] = stream_client.config()["stream_backends"]
    assert isinstance(backend, ExtensionStreamBackend)
    stream = await stream_client.create_stream(Tokens, id=f"shutdown-{uuid.uuid4()}")
    async with common.BufferedPublisher(stream) as tokens:
        tokens.publish("buffered")
    await backend.shutdown()
    written = [
        item.data
        async for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, common.Record)
    ]
    assert written == ["buffered"]


@workflow.defn
class WaitsForever:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("waiting")
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
    # Python closes an evicted run's coroutine with GeneratorExit, outside the Workflow's loop.
    # The interceptor must let it pass without a call to the runtime.
    inbound = _WorkflowInbound(_NeverEnds())
    run = inbound.execute_workflow(
        cast(ExecuteWorkflowInput, cast(object, SimpleNamespace(headers={}, args=[])))
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
        reader = workflow_stream(handle, Tokens).subscribe(
            create_wait=timedelta(seconds=20)
        )
        assert isinstance(await anext(reader), common.Record)
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
        stream = await workflow.create_stream(Tokens)
        await stream.publish(f"run-{attempt}")
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
        stream = workflow_stream(handle, Tokens)
        items = [item async for item in stream.subscribe(wait_new_records=False)]
    written = records(items)
    superseded = [item for item in items if isinstance(item, common.Superseded)]
    # The end of the chain doesn't close the Stream.
    assert not any(isinstance(item, common.Closed) for item in items)
    if non_retryable:
        # A non-retryable failure ends the chain on attempt 1.
        assert [r.data for r in written] == ["run-1"]
        assert superseded == []
        return
    # Each retry run withdraws the Records of the run before it.
    assert [r.data for r in written] == ["run-1", "run-2", "run-3"]
    assert len(superseded) == 2
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["run-1"]
    assert [r.data for r in written if superseded[1].withdraws(r)] == ["run-2"]
    assert not any(s.withdraws(written[-1]) for s in superseded)


async def test_reader_stops_on_its_own_timer_and_resumes(stream_client: Client):
    stream = await stream_client.create_stream(Tokens, id=f"timer-{uuid.uuid4()}")
    first = await stream.publish("one")
    seen: list[common.Record[Any]] = []

    async def read_until_quiet() -> None:
        async for item in stream.subscribe():
            assert isinstance(item, common.Record)
            seen.append(item)

    # A read has no idle timeout, so a reader that wants one uses its own timer.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(read_until_quiet(), timeout=0.5)
    assert [item.offset for item in seen] == [first]
    await stream.publish("two")
    resumed = [
        item.data
        async for item in stream.subscribe(
            offset=seen[-1].offset + 1, wait_new_records=False
        )
        if isinstance(item, common.Record)
    ]
    assert resumed == ["two"]


async def test_two_backends_on_one_client(client: Client, redis_url: str):
    # A client can have two Stream backends, such as during a migration to another backend. Each
    # handle stores its backend name, so a call goes to the backend that has the Stream.
    config = client.config()
    config["stream_backends"] = [
        RedisStreamBackend(redis_url, key_prefix=f"t-{uuid.uuid4()}", default=True),
        MemoryStreamBackend(),
    ]
    stream_client = Client(**config)
    stream_id = f"two-{uuid.uuid4()}"
    on_redis = await stream_client.create_stream(Tokens, id=stream_id)
    on_memory = await stream_client.create_stream(
        Tokens, id=stream_id, backend="memory"
    )
    assert (on_redis.backend, on_memory.backend) == ("redis", "memory")
    await on_redis.publish("redis")
    await on_memory.publish("memory")
    for backend, expected in (("redis", "redis"), ("memory", "memory")):
        again = stream_client.stream(Tokens, id=stream_id, backend=backend)
        data = [
            item.data
            async for item in again.subscribe(wait_new_records=False)
            if isinstance(item, common.Record)
        ]
        assert data == [expected]


failed_last_task: set[str] = set()


class _FailsTheLastTaskOnce(Interceptor, WorkerInterceptor):
    # It's on the client before the Stream backend's interceptor, so it's the outer Workflow
    # interceptor. It fails the task after the backend drained the run's writes.
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
        stream = await workflow.create_stream(Tokens)
        await stream.publish("token")
        await stream.close(result="done")
        return "done"


async def close_activity_events(
    handle: WorkflowHandle[Any, Any],
) -> list[EventType.ValueType]:
    # The events of the close Activity, from its schedule on.
    history = await handle.fetch_history()
    scheduled = {
        event.event_id
        for event in history.events
        if event.activity_task_scheduled_event_attributes.activity_type.name
        == "__temporal_streams_close"
    }
    return [
        event.event_type
        for event in history.events
        if event.event_id in scheduled
        or event.activity_task_started_event_attributes.scheduled_event_id in scheduled
        or event.activity_task_completed_event_attributes.scheduled_event_id
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
        stream = workflow_stream(handle, Tokens)
        items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [r.data for r in records(items)] == ["token"]
    closes = [item for item in items if isinstance(item, common.Closed)]
    assert len(closes) == 1 and items[-1] is closes[0]
    assert closes[0].result == "done"
    assert closes[0].status == common.CloseStatus.COMPLETED
    assert await close_activity_events(handle) == [
        EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
        EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
    ]


@workflow.defn
class WritesToStream:
    @workflow.run
    async def run(self, stream: common.WorkflowStream[str]) -> str:
        try:
            await stream.publish("from workflow")
        except common.StreamError as err:
            return StreamErrorReason.Name(err.reason)
        return "wrote"


async def test_write_to_a_closed_stream_raises_at_the_await(stream_client: Client):
    stream = await stream_client.create_stream(Tokens, id=f"closed-{uuid.uuid4()}")
    await stream.close()
    async with new_worker(stream_client, WritesToStream) as worker:
        assert (
            await stream_client.execute_workflow(
                WritesToStream.run,
                common.WorkflowStream.from_handle(stream),
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "STREAM_ERROR_REASON_CLOSED"
        )
    items = [item async for item in stream.subscribe()]
    assert len(items) == 1 and isinstance(items[0], common.Closed)


@workflow.defn
class ContinuesThenRetries:
    @workflow.run
    async def run(self, stream: common.WorkflowStream[str] | None) -> str:
        if stream is None:
            stream = await workflow.create_stream(Tokens)
            await stream.publish("first")
            workflow.continue_as_new(stream)
        info = workflow.info()
        await stream.publish(f"attempt-{info.attempt}")
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
        items = await read_all(workflow_stream(handle, Tokens))
    written = records(items)
    assert [r.data for r in written] == ["first", "attempt-1", "attempt-2"]
    superseded = [item for item in items if isinstance(item, common.Superseded)]
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["attempt-1"]
    closes = [item for item in items if isinstance(item, common.Closed)]
    assert len(closes) == 1 and closes[0].result == "done"
