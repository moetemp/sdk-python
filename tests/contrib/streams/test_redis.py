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

from temporalio import activity, streams, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType, StreamErrorReason
from temporalio.client import (
    BufferedAppender,
    Client,
    Interceptor,
    Stream,
    WorkflowFailureError,
    WorkflowHandle,
)
from temporalio.common import RawValue, RetryPolicy
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.streams._extension import ExtensionStreamBackend, _WorkflowInbound
from temporalio.streams.system import models
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
from tests.contrib.streams._support import created, only_backend, stream_status
from tests.helpers import new_worker


async def read_all(stream: Stream[Any]) -> list[Any]:
    return [item async for item in (await created(stream)).read()]


def records(items: list[Any]) -> list[streams.Record[Any]]:
    return [item for item in items if isinstance(item, streams.Record)]


attempts: dict[str, int] = {}


@workflow.defn
class PublishThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(
            f"{workflow.info().workflow_id}-tokens", payload_type=str
        )
        await stream.append("before")
        await workflow.sleep(timedelta(milliseconds=10))
        await stream.append("after")
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")
        await stream.close()


async def test_failed_workflow_task_republishes_its_records(stream_client: Client):
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
        stream_id = f"{handle.id}-tokens"
        items = await read_all(stream_client.get_stream(stream_id, payload_type=str))
        assert (
            await stream_status(stream_client, stream_id)
            == models.StreamStatus.StreamStatusClosed
        )
    # The retried task writes "after" again, and both copies stay visible.
    written = records(items)
    assert [record.data for record in written] == ["before", "after", "after"]


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.StreamRef[str]


@activity.defn
async def local_tokens(input: CallModelInput) -> None:
    await activity.client().get_stream(input.stream).append(*input.prompt.split())


@workflow.defn
class LocalThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(
            f"{workflow.info().workflow_id}-tokens", payload_type=str
        )
        first = attempts.setdefault(key, 0) == 0
        await workflow.execute_local_activity(
            local_tokens,
            CallModelInput("0 1" if first else "2 3", stream.ref),
            start_to_close_timeout=timedelta(seconds=30),
        )
        if first:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")
        await stream.close()


def _task_of(producer: streams.Producer | None) -> tuple[int, int]:
    assert producer is not None
    kind = producer.kind
    assert isinstance(kind, streams.ProducerKindLocalActivity)
    epoch = kind.value.epoch
    task = epoch.task if epoch else None
    assert task is not None
    return (task.attempt or 0, task.started_event_id or 0)


async def test_rerun_local_activity_keeps_both_attempts(stream_client: Client):
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
        items = await read_all(
            stream_client.get_stream(f"{handle.id}-tokens", payload_type=str)
        )
    written = records(items)
    # A Local Activity is never superseded, so both the failed task's and the rerun's Records
    # stay visible.
    assert [r.data for r in written] == ["0", "1", "2", "3"]
    # The rerun keeps the Producer id, and its epoch has the retry task, which orders it later.
    first, last = written[0].producer, written[-1].producer
    assert first is not None and last is not None and first.id == last.id
    assert _task_of(first) < _task_of(last)


@workflow.defn
class LocalOnlyInFailedTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(
            f"{workflow.info().workflow_id}-tokens", payload_type=str
        )
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            await workflow.execute_local_activity(
                local_tokens,
                CallModelInput("0 1", stream.ref),
                start_to_close_timeout=timedelta(seconds=30),
            )
            raise RuntimeError("fail this Workflow Task once")
        await stream.append("other")
        await stream.close()


async def test_skipped_local_activitys_records_stay_visible(stream_client: Client):
    # The retry task skips the Local Activity. A Local Activity is never superseded, so the
    # skipped call's "0 1" stay visible alongside the retry's own write.
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
        stream_id = f"{handle.id}-tokens"
        items = await read_all(stream_client.get_stream(stream_id, payload_type=str))
        assert (
            await stream_status(stream_client, stream_id)
            == models.StreamStatus.StreamStatusClosed
        )
    written = records(items)
    assert [r.data for r in written] == ["0", "1", "other"]


@dataclass
class Turn:
    kind: str
    text: str | None = None


def own_id(name: str = "tokens") -> str:
    # A Workflow picks the id of each Stream that it creates. These tests use its Workflow ID.
    return f"{workflow.info().workflow_id}-{name}"


def stream_of(
    client: Client, handle: WorkflowHandle[Any, Any], name: str = "tokens"
) -> Stream[Any]:
    return client.get_stream(f"{handle.id}-{name}")


async def closed(client: Client, handle: WorkflowHandle[Any, Any], name: str) -> bool:
    status = await stream_status(client, f"{handle.id}-{name}")
    return status == models.StreamStatus.StreamStatusClosed


@activity.defn
async def call_model(input: CallModelInput) -> str:
    stream = activity.client().get_stream(input.stream)
    for word in input.prompt.split():
        await stream.append(word)
    return input.prompt.upper()


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        turns = await workflow.create_stream(own_id("turns"), payload_type=Turn)
        tokens = await workflow.create_stream(own_id("tokens"), payload_type=str)
        await turns.append(Turn("model"))
        reply = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, tokens.ref),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await turns.append(Turn("done", reply))
        await turns.close()
        await tokens.close()
        return reply


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
    async def run(self, ref: streams.StreamRef[str] | None) -> str:
        if ref is None:
            stream = await workflow.create_stream(own_id(), payload_type=str)
            await stream.append("first run")
            workflow.continue_as_new(stream.ref)
        await workflow.get_stream(ref).close()
        return "second run"


async def test_later_run_closes_a_stream_it_did_not_write(stream_client: Client):
    async with new_worker(stream_client, WritesThenContinues) as worker:
        handle = await stream_client.start_workflow(
            WritesThenContinues.run,
            None,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "second run"
        items = await read_all(stream_of(stream_client, handle))
        assert await closed(stream_client, handle, "tokens")
    assert [i.data for i in records(items)] == ["first run"]


@workflow.defn
class CancelledBlock:
    @workflow.run
    async def run(self) -> None:
        async with await workflow.create_stream(
            own_id("scoped"), payload_type=str
        ) as turn:
            await turn.append("started")
            await workflow.wait_condition(lambda: False)


async def test_cancelled_block_closes_the_stream(stream_client: Client):
    async with new_worker(stream_client, CancelledBlock) as worker:
        handle = await stream_client.start_workflow(
            CancelledBlock.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        turn = stream_of(stream_client, handle, "scoped")
        reader = (await created(turn)).read()
        assert isinstance(await anext(reader), streams.Record)
        await reader.aclose()
        await handle.cancel()
        items = await read_all(turn)
        assert await closed(stream_client, handle, "scoped")
    assert [i.data for i in records(items)] == ["started"]


@workflow.defn
class PlainWriter:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(own_id("plain"), payload_type=str)
        await stream.append("plain")
        async with await workflow.create_stream(
            own_id("scoped"), payload_type=str
        ) as scoped:
            await scoped.append("scoped")
        await stream.close()
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
        items = await read_all(stream_of(stream_client, handle, "plain"))
        scoped = await read_all(stream_of(stream_client, handle, "scoped"))
        assert await closed(stream_client, handle, "plain")
        assert await closed(stream_client, handle, "scoped")
    assert [i.data for i in records(items)] == ["plain"]
    assert [i.data for i in records(scoped)] == ["scoped"]


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
        turns = codec_client.get_stream(f"{handle.id}-turns", payload_type=Turn)
        tokens = codec_client.get_stream(f"{handle.id}-tokens", payload_type=str)
        turn_items = await read_all(turns)
        decoded = await read_all(tokens)
        # RawValue as the payload type reads the raw Payloads.
        raw_tokens = codec_client.get_stream(tokens.ref.id, payload_type=RawValue)
        raw = [item async for item in raw_tokens.read(wait_new_records=False)]
    assert [item.data for item in records(turn_items)] == [
        Turn("model"),
        Turn("done", "A B"),
    ]
    assert [item.data for item in records(decoded)] == ["a", "b"]
    first = raw[0]
    assert isinstance(first, streams.Record)
    assert first.data.payload.metadata["encoding"] == b"json/plain"


@workflow.defn
class WaitsForSignal:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("before restart")
        async with await workflow.create_stream(
            own_id("scoped"), payload_type=str
        ) as scoped:
            await scoped.append("scoped")
        await workflow.wait_condition(lambda: self.go)
        await stream.append("after restart")
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
        first = stream_of(stream_client, handle)
        reader = (await created(first)).read()
        assert isinstance(await anext(reader), streams.Record)
        await reader.aclose()
    # The new Worker has no cache, so it replays the run. A replay sends no create, which would
    # fail on the Streams that exist, and no second close.
    async with new_worker(stream_client, WaitsForSignal, task_queue=task_queue):
        await handle.signal(WaitsForSignal.proceed)
        assert await handle.result() == "done"
        items = await read_all(first)
        scoped = await read_all(stream_of(stream_client, handle, "scoped"))
        assert await closed(stream_client, handle, "tokens")
        assert await closed(stream_client, handle, "scoped")
    assert [i.data for i in records(items)] == ["before restart", "after restart"]
    assert [i.data for i in records(scoped)] == ["scoped"]


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
        stream_config=stream_client.config()["stream_config"],
    )
    await replayer.replay_workflow(history)


async def test_backend_shutdown_after_client_writes(stream_client: Client):
    backend = only_backend(stream_client)
    assert isinstance(backend, ExtensionStreamBackend)
    stream = await stream_client.create_stream(
        f"shutdown-{uuid.uuid4()}", payload_type=str
    )
    async with BufferedAppender(stream) as tokens:
        tokens.append("buffered")
    await backend.shutdown()
    written = [
        item.data
        async for item in stream.read(wait_new_records=False)
        if isinstance(item, streams.Record)
    ]
    assert written == ["buffered"]


@workflow.defn
class WaitsForever:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("waiting")
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
        reader = (await created(stream_of(stream_client, handle))).read()
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
        # Each retry run sends the create of the first run, so it gets the same Stream.
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append(f"run-{attempt}")
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
        stream = stream_of(stream_client, handle)
        items = [item async for item in stream.read(wait_new_records=False)]
        # The end of the chain doesn't close the Stream.
        assert not await closed(stream_client, handle, "tokens")
    written = records(items)
    if non_retryable:
        # A non-retryable failure ends the chain on attempt 1.
        assert [r.data for r in written] == ["run-1"]
        return
    # Each retry run's Records stay visible alongside the runs before it.
    assert [r.data for r in written] == ["run-1", "run-2", "run-3"]


async def test_reader_stops_on_its_own_timer_and_resumes(stream_client: Client):
    stream = await stream_client.create_stream(
        f"timer-{uuid.uuid4()}", payload_type=str
    )
    first = await stream.append("one")
    seen: list[streams.Record[Any]] = []
    reader = stream.read()

    async def read_until_quiet() -> None:
        async for item in reader:
            assert isinstance(item, streams.Record)
            seen.append(item)

    # A read has no idle timeout, so a reader that wants one uses its own timer.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(read_until_quiet(), timeout=0.5)
    assert [item.offset for item in seen] == [first]
    resume_at = reader.last_offset
    await stream.append("two")
    resumed = [
        item.data
        async for item in stream.read(offset=resume_at, wait_new_records=False)
        if isinstance(item, streams.Record)
    ]
    assert resumed == ["two"]


async def test_two_backends_on_one_client(client: Client, redis_url: str):
    # A client can have two Stream backends, such as during a migration to another backend. Each
    # ref stores its backend name, so a call goes to the backend that has the Stream.
    redis = RedisStreamBackend(redis_url, key_prefix=f"t-{uuid.uuid4()}")
    memory = MemoryStreamBackend()
    config = client.config()
    config["stream_config"] = streams.StreamConfig(
        backends=[redis, memory],
        backend_selector=lambda ctx: memory if ctx.id.startswith("memory-") else redis,
    )
    stream_client = Client(**config)
    suffix = uuid.uuid4()
    on_redis = await stream_client.create_stream(f"redis-{suffix}", payload_type=str)
    on_memory = await stream_client.create_stream(f"memory-{suffix}", payload_type=str)
    assert (on_redis.ref.backend, on_memory.ref.backend) == ("redis", "memory")
    await on_redis.append("redis")
    await on_memory.append("memory")
    for ref, expected in ((on_redis.ref, "redis"), (on_memory.ref, "memory")):
        again = stream_client.get_stream(ref)
        data = [
            item.data
            async for item in again.read(wait_new_records=False)
            if isinstance(item, streams.Record)
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
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("token")
        await stream.close()
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
        stream = stream_of(client, handle)
        items = [item async for item in stream.read(wait_new_records=False)]
        assert await closed(client, handle, "tokens")
    assert [r.data for r in records(items)] == ["token"]
    assert await close_activity_events(handle) == [
        EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED,
        EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
    ]


@workflow.defn
class WritesToStream:
    @workflow.run
    async def run(self, ref: streams.StreamRef[str]) -> str:
        try:
            await workflow.get_stream(ref).append("from workflow")
        except streams.StreamError as err:
            return StreamErrorReason.Name(StreamErrorReason.ValueType(err.reason))
        return "wrote"


async def test_write_to_a_closed_stream_raises_at_the_await(stream_client: Client):
    stream = await stream_client.create_stream(
        f"closed-{uuid.uuid4()}", payload_type=str
    )
    await stream.close()
    async with new_worker(stream_client, WritesToStream) as worker:
        assert (
            await stream_client.execute_workflow(
                WritesToStream.run,
                stream.ref,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "STREAM_ERROR_REASON_CLOSED"
        )
    # The close wrote no Record, so a read of the closed Stream yields only the Closed item.
    items = [item async for item in stream.read()]
    assert len(items) == 1 and isinstance(items[0], streams.Closed)


@workflow.defn
class ContinuesThenRetries:
    @workflow.run
    async def run(self, ref: streams.StreamRef[str] | None) -> str:
        if ref is None:
            created = await workflow.create_stream(own_id(), payload_type=str)
            await created.append("first")
            workflow.continue_as_new(created.ref)
        stream = workflow.get_stream(ref)
        info = workflow.info()
        await stream.append(f"attempt-{info.attempt}")
        if info.attempt == 1:
            raise ApplicationError("the continued run fails once")
        await stream.close()
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
        items = await read_all(stream_of(stream_client, handle))
        assert await closed(stream_client, handle, "tokens")
    written = records(items)
    assert [r.data for r in written] == ["first", "attempt-1", "attempt-2"]
