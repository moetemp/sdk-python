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
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.common import RawValue, RetryPolicy
from temporalio.contrib import streams
from temporalio.contrib.streams import StreamPlugin
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import (
    RedisStreamPlugin,
    _ExtensionStreamPlugin,
    _WorkflowInbound,
)
from temporalio.contrib.streams.testing import MemoryStreamPlugin
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.worker import (
    ExecuteWorkflowInput,
    Replayer,
    UnsandboxedWorkflowRunner,
    WorkflowInboundInterceptor,
)
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)
from tests.helpers import assert_task_fail_eventually, new_worker


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
    client: Client, handle: WorkflowHandle[Any, Any], name: str | None = None
) -> streams.ClientStream[Events]:
    return streams.StreamClient(client).workflow_stream(handle, Events, name=name)


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
        stream = await stream_workflow.create_stream(Events)
        turns = stream.topic(Events.turns)
        await turns.publish(Turn("model"))
        reply = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await turns.publish(Turn("done", reply))
        return reply


async def test_workflow_and_activity_write_one_stream(stream_client: Client):
    async with new_worker(stream_client, Agent, activities=[call_model]) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = workflow_stream(stream_client, handle)
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
        turns = (await stream_workflow.create_stream(Events)).topic(Events.tokens)
        await turns.publish("before")
        await workflow.sleep(timedelta(milliseconds=10))
        await turns.publish("after")
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")


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
        items = await read_all(workflow_stream(stream_client, handle))
    # The failed task wrote nothing, so the reader gets each Record once.
    assert [record.data for record in records(items)] == ["before", "after"]
    assert not any(isinstance(item, streams.Superseded) for item in items)
    assert isinstance(items[-1], streams.Closed)


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


async def test_plugin_leaves_the_workflow_random_alone(stream_client: Client):
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
            stream = await stream_workflow.create_stream(Events)
            await stream.topic(Events.tokens).publish("first run")
            workflow.continue_as_new(False)
        return "second run"


async def test_last_run_closes_a_stream_it_did_not_write(stream_client: Client):
    async with new_worker(stream_client, WritesThenContinues) as worker:
        handle = await stream_client.start_workflow(
            WritesThenContinues.run,
            True,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "second run"
        items = await read_all(workflow_stream(stream_client, handle))
    assert [i.data for i in records(items)] == ["first run"]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "second run"


@workflow.defn
class CancelledBlock:
    @workflow.run
    async def run(self) -> None:
        async with await stream_workflow.create_stream(
            Events, name="turn", close_with_run=False
        ) as turn:
            await turn.topic(Events.tokens).publish("started")
            await workflow.wait_condition(lambda: False)


async def test_cancelled_block_closes_with_a_failure(stream_client: Client):
    async with new_worker(stream_client, CancelledBlock) as worker:
        handle = await stream_client.start_workflow(
            CancelledBlock.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        turn = workflow_stream(stream_client, handle, "turn")
        reader = turn.subscribe(create_wait=timedelta(seconds=20))
        assert isinstance(await anext(reader), streams.Record)
        await reader.aclose()
        await handle.cancel()
        items = await read_all(turn)
    closed = items[-1]
    assert isinstance(closed, streams.Closed)
    assert closed.failure is not None


@streams.defn(name="plain")
class Plain:
    t = streams.topic(str)


@workflow.defn
class PlainWriter:
    @workflow.run
    async def run(self) -> str:
        stream = await stream_workflow.create_stream(Plain)
        await stream.topic(Plain.t).publish("plain")
        async with await stream_workflow.create_stream(
            Plain, name="scoped", close_with_run=False
        ) as scoped:
            await scoped.topic(Plain.t).publish("scoped")
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
        plain = streams.StreamClient(stream_client)
        items = await read_all(plain.workflow_stream(handle, Plain))
        scoped = await read_all(plain.workflow_stream(handle, Plain, name="scoped"))
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
        stream = await stream_workflow.create_stream(Events)
        await workflow.execute_local_activity(
            local_tokens,
            CallModelInput("0 1", streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )


async def test_local_activity_writes_with_the_task_epoch(stream_client: Client):
    async with new_worker(stream_client, LocalWriter, activities=[local_tokens]) as w:
        handle = await stream_client.start_workflow(
            LocalWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=w.task_queue
        )
        await handle.result()
        items = await read_all(workflow_stream(stream_client, handle))
    written = records(items)
    assert [record.data for record in written] == ["0", "1"]
    producer = written[0].producer
    assert producer is not None and producer.id.startswith("la:")
    assert (
        producer.epoch is not None and (producer.epoch.task_started_event_id or 0) > 0
    )


@workflow.defn
class FailingRun:
    @workflow.run
    async def run(self) -> str:
        turns = (await stream_workflow.create_stream(Events)).topic(Events.tokens)
        info = workflow.info()
        await turns.publish(f"run-{info.attempt}")
        if info.attempt == 1:
            raise ApplicationError("first run fails")
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
        items = await read_all(workflow_stream(stream_client, handle))
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [record.data for record in written] == ["run-1", "run-2"]
    assert len(superseded) == 1
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["run-1"]
    # The failed run had retries left, so only the last run closes the Stream.
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
        stream = workflow_stream(codec_client, handle)
        decoded = await read_all(stream)
        # A Stream with no definition reads the raw Payloads.
        raw = [
            item
            async for item in streams.StreamClient(codec_client)
            .get_handle(Plain, id=stream.id)
            .topic("tokens", type=RawValue)
            .subscribe(wait_new_records=False)
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
    stream_client_ = streams.StreamClient(Client(**config))
    done = await stream_client_.create(Events, id=f"done-{uuid.uuid4()}")
    await done.close(result=Turn("done"))
    failed = await stream_client_.create(Events, id=f"failed-{uuid.uuid4()}")
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
        turns = (await stream_workflow.create_stream(Events)).topic(Events.tokens)
        await turns.publish("before restart")
        async with await stream_workflow.create_stream(
            Events, name="scoped", close_with_run=False
        ) as scoped:
            await scoped.topic(Events.tokens).publish("scoped")
        await workflow.wait_condition(lambda: self.go)
        await turns.publish("after restart")
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
        first = workflow_stream(stream_client, handle)
        reader = first.subscribe(create_wait=timedelta(seconds=20))
        assert isinstance(await anext(reader), streams.Record)
        await reader.aclose()
    # The new Worker has no cache, so it replays the run and sends the creates and the close
    # of the scoped Stream again.
    async with new_worker(stream_client, WaitsForSignal, task_queue=task_queue):
        await handle.signal(WaitsForSignal.proceed)
        assert await handle.result() == "done"
        items = await read_all(first)
        scoped = await read_all(workflow_stream(stream_client, handle, "scoped"))
    assert [i.data for i in records(items)] == ["before restart", "after restart"]
    assert isinstance(items[-1], streams.Closed)
    assert [i.data for i in records(scoped)] == ["scoped"]
    assert [type(i) for i in scoped].count(streams.Closed) == 1


async def test_plugin_replays_a_history_made_without_it(
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
        plugins=[
            p for p in stream_client.config()["plugins"] if isinstance(p, StreamPlugin)
        ],
    )
    await replayer.replay_workflow(history)


@workflow.defn
class QuickWriter:
    @workflow.run
    async def run(self) -> str:
        stream = await stream_workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("quick")
        return "done"


async def test_worker_shutdown_drains_closes(stream_client: Client):
    async with new_worker(stream_client, QuickWriter) as worker:
        handle = await stream_client.start_workflow(
            QuickWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
    # Calls after the Worker exits still work, and the drain waited for the run's close.
    items = [
        item
        async for item in workflow_stream(stream_client, handle).subscribe(
            wait_new_records=False
        )
    ]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"


async def test_plugin_shutdown_after_client_writes(stream_client: Client):
    [plugin] = [
        p
        for p in stream_client.config()["plugins"]
        if isinstance(p, _ExtensionStreamPlugin)
    ]
    stream = await streams.StreamClient(stream_client).create(
        Events, id=f"shutdown-{uuid.uuid4()}"
    )
    async with streams.BufferedPublisher(stream.topic(Events.tokens)) as tokens:
        tokens.publish("buffered")
    await plugin.shutdown()
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
        stream = await stream_workflow.create_stream(Events)
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
        reader = workflow_stream(stream_client, handle).subscribe(
            create_wait=timedelta(seconds=20)
        )
        assert isinstance(await anext(reader), streams.Record)
        await reader.aclose()
    await handle.terminate()
    gc.collect()
    captured = capfd.readouterr()
    assert "Exception ignored" not in captured.err + captured.out


async def test_workflow_calls_carry_the_owner(stream_client: Client):
    [plugin] = [
        p
        for p in stream_client.config()["plugins"]
        if isinstance(p, _ExtensionStreamPlugin)
    ]
    sent: list[bytes] = []
    call = plugin._backend.call

    def record(method: str, request: Any) -> bytes:
        if method == "BufferWorkflowCalls":
            sent.append(request.SerializeToString())
        return call(method, request)

    plugin._backend.call = record  # type: ignore[method-assign]
    async with new_worker(stream_client, QuickWriter) as worker:
        handle = await stream_client.start_workflow(
            QuickWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
    refs = []
    for data in sent:
        request = ext_pb.BufferWorkflowCallsRequest.FromString(data)
        for workflow_call in request.calls:
            if workflow_call.HasField("append"):
                refs.append(workflow_call.append.stream)
            if workflow_call.HasField("close"):
                refs.append(workflow_call.close.stream)
    assert refs
    for ref in refs:
        assert ref.workflow_id == handle.id
        assert ref.first_execution_run_id == handle.first_execution_run_id


@workflow.defn
class AlwaysFails:
    @workflow.run
    async def run(self, non_retryable: bool) -> None:
        attempt = workflow.info().attempt
        stream = await stream_workflow.create_stream(Events)
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
        items = await read_all(workflow_stream(stream_client, handle))
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    closes = [item for item in items if isinstance(item, streams.Closed)]
    # Only the run that ends the chain closes the Stream, with its failure, and it is last.
    assert len(closes) == 1 and items[-1] is closes[0]
    assert closes[0].failure is not None
    if non_retryable:
        # A non-retryable failure ends the chain on attempt 1, so it closes at once.
        assert [r.data for r in written] == ["run-1"]
        assert superseded == []
        return
    # Attempts 1 and 2 left the Stream open, so attempts 2 and 3 could write. Each retry run
    # withdraws the run before it.
    assert [r.data for r in written] == ["run-1", "run-2", "run-3"]
    assert len(superseded) == 2
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["run-1"]
    assert [r.data for r in written if superseded[1].withdraws(r)] == ["run-2"]
    assert not any(s.withdraws(written[-1]) for s in superseded)


async def test_missing_capability_fails_the_workflow_task(client: Client):
    # The Workflow flush runs in Core's before_complete, so a Store that lacks a capability
    # fails the Workflow Task with the capability's name, not a call in Workflow code.
    config = client.config()
    config["plugins"] = [MemoryStreamPlugin(durable_writes=1)]
    memory_client = Client(**config)
    async with new_worker(memory_client, QuickWriter) as worker:
        handle = await memory_client.start_workflow(
            QuickWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await assert_task_fail_eventually(handle, message_contains="replica_wait")
        await handle.terminate()


async def test_reader_stops_on_its_own_timer_and_resumes(stream_client: Client):
    stream = await streams.StreamClient(stream_client).create(
        Events, id=f"timer-{uuid.uuid4()}"
    )
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
            from_offset=seen[-1].offset + 1, wait_new_records=False
        )
        if isinstance(item, streams.Record)
    ]
    assert resumed == ["two"]


async def test_two_backends_on_one_client(client: Client, redis_url: str):
    # A client may carry two Stream plugins, such as during a move from one backend to
    # another. Each handle carries its backend, so a call goes to the backend that has it.
    config = client.config()
    config["plugins"] = [
        RedisStreamPlugin(redis_url, key_prefix=f"t-{uuid.uuid4()}"),
        MemoryStreamPlugin(),
    ]
    stream_client = streams.StreamClient(Client(**config))
    stream_id = f"two-{uuid.uuid4()}"
    on_redis = await stream_client.create(Events, id=stream_id)
    on_memory = await stream_client.create(Events, id=stream_id, backend="memory")
    assert (on_redis.backend, on_memory.backend) == ("redis", "memory")
    await on_redis.topic(Events.tokens).publish("redis")
    await on_memory.topic(Events.tokens).publish("memory")
    for backend, expected in (("redis", "redis"), ("memory", "memory")):
        again = stream_client.get_handle(Events, id=stream_id, backend=backend)
        data = [
            item.data
            async for item in again.subscribe(wait_new_records=False)
            if isinstance(item, streams.Record)
        ]
        assert data == [expected]


@workflow.defn
class WritesToStream:
    @workflow.run
    async def run(self, stream: stream_workflow.WorkflowStream[Events]) -> str:
        await stream.topic(Events.tokens).publish("from workflow")
        return "done"


async def test_write_to_a_stream_closed_elsewhere_is_dropped(stream_client: Client):
    stream = await streams.StreamClient(stream_client).create(
        Events, id=f"closed-{uuid.uuid4()}"
    )
    await stream.close()
    # This run did not close the Stream, so replay can't raise it in Workflow code. A live
    # run and its replay must act the same, so the backend drops the Records.
    async with new_worker(stream_client, WritesToStream) as worker:
        assert (
            await stream_client.execute_workflow(
                WritesToStream.run,
                stream_workflow.WorkflowStream.from_handle(stream),
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "done"
        )
    items = [item async for item in stream.subscribe()]
    assert len(items) == 1 and isinstance(items[0], streams.Closed)


async def test_store_error_fails_the_task_with_the_stream_id(client: Client):
    config = client.config()
    config["plugins"] = [MemoryStreamPlugin(durable_writes=1)]
    memory_client = Client(**config)
    stream = await streams.StreamClient(memory_client).create(
        Events, id=f"open-{uuid.uuid4()}"
    )
    async with new_worker(memory_client, WritesToStream) as worker:
        handle = await memory_client.start_workflow(
            WritesToStream.run,
            stream_workflow.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await assert_task_fail_eventually(handle, message_contains=stream.id)
        await handle.terminate()


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
        stream = await stream_workflow.create_stream(Events)
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
    async def run(self, stream: stream_workflow.WorkflowStream[Events] | None) -> str:
        if stream is None:
            stream = await stream_workflow.create_stream(Events)
            await stream.topic(Events.tokens).publish("first")
            workflow.continue_as_new(stream)
        info = workflow.info()
        await stream.topic(Events.tokens).publish(f"attempt-{info.attempt}")
        if info.attempt == 1:
            raise ApplicationError("the continued run fails once")
        return "done"


async def test_retry_of_a_continued_run_keeps_the_close_with_run(stream_client: Client):
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
        items = await read_all(workflow_stream(stream_client, handle))
    written = records(items)
    assert [r.data for r in written] == ["first", "attempt-1", "attempt-2"]
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["attempt-1"]
    closes = [item for item in items if isinstance(item, streams.Closed)]
    assert len(closes) == 1 and closes[0].result == "done"
