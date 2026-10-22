import asyncio
import concurrent.futures
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
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import RawValue, RetryPolicy
from temporalio.contrib import streams
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import (
    StreamPlugin,
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


@activity.defn
async def call_model(prompt: str) -> str:
    tokens = streams.activity_stream().topic("tokens", type=str)
    for word in prompt.split():
        tokens.publish(word)
    return prompt.upper()


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        turns = streams.workflow_stream().topic("turns", type=Turn)
        turns.publish(Turn("model"))
        reply = await workflow.execute_activity(
            call_model, prompt, start_to_close_timeout=timedelta(seconds=30)
        )
        turns.publish(Turn("done", reply))
        return reply


async def read_all(stream: streams.OutsideStream) -> list[streams.Record | object]:
    return [item async for item in stream.subscribe(idle_timeout=timedelta(seconds=20))]


async def test_workflow_and_activity_write_the_default_stream(stream_client: Client):
    async with new_worker(stream_client, Agent, activities=[call_model]) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = streams.for_handle(handle, wait_for_create=timedelta(seconds=20))
        turns = stream.topic("turns", type=Turn)
        stream.topic("tokens", type=str)
        items = await read_all(stream)
        assert await handle.result() == "HI THERE"
    data = [
        (item.topic, item.data) for item in items if isinstance(item, streams.Record)
    ]
    assert data == [
        ("turns", Turn("model")),
        ("tokens", "hi"),
        ("tokens", "there"),
        ("turns", Turn("done", "HI THERE")),
    ]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "HI THERE"
    only_turns = [item async for item in turns.subscribe()]
    assert [type(item) for item in only_turns] == [
        streams.Record,
        streams.Record,
        streams.Closed,
    ]


@workflow.defn
class Chat:
    @workflow.run
    async def run(self, turns: int) -> None:
        index = streams.workflow_stream().topic("turns", type=int)
        for n in range(turns):
            with streams.create_workflow_stream(f"turn-{n}") as turn:
                index.publish(n)
                turn.topic("tokens", type=str).publish(f"token-{n}")
            await workflow.sleep(timedelta(milliseconds=10))


async def test_scoped_streams_close_at_the_end_of_their_block(stream_client: Client):
    async with new_worker(stream_client, Chat) as worker:
        handle = await stream_client.start_workflow(
            Chat.run, 2, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        turn = streams.for_handle(handle, name="turn-1")
        items = [item async for item in turn.topic("tokens", type=str).subscribe()]
    assert isinstance(items[0], streams.Record) and items[0].data == "token-1"
    assert isinstance(items[-1], streams.Closed)


@workflow.defn
class WriteToRef:
    @workflow.run
    async def run(self, ref: streams.StreamRef) -> None:
        streams.workflow_stream(ref).topic("t", type=str).publish("from workflow")


async def test_client_stream_takes_workflow_writes(stream_client: Client):
    stream = await streams.create_stream(stream_client, f"chat-{uuid.uuid4()}")
    stream.topic("t", type=str).publish("from client")
    await stream.flush()
    async with new_worker(stream_client, WriteToRef) as worker:
        await stream_client.execute_workflow(
            WriteToRef.run,
            stream.ref,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    await stream.close()
    items = [item async for item in stream.subscribe()]
    data = [item.data for item in items if isinstance(item, streams.Record)]
    assert data == ["from client", "from workflow"]
    assert isinstance(items[-1], streams.Closed)


attempts: dict[str, int] = {}


@activity.defn
async def flaky(key: str) -> None:
    attempts[key] = attempts.get(key, 0) + 1
    tokens = streams.activity_stream().topic("tokens", type=str)
    tokens.publish(f"attempt-{attempts[key]}")
    await streams.activity_stream().flush()
    if attempts[key] == 1:
        raise RuntimeError("first attempt fails")


@workflow.defn
class Retrying:
    @workflow.run
    async def run(self, key: str) -> None:
        streams.workflow_stream().topic("turns", type=str).publish("start")
        await workflow.execute_activity(
            flaky,
            key,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def test_activity_retry_withdraws_the_failed_attempt(stream_client: Client):
    key = str(uuid.uuid4())
    async with new_worker(stream_client, Retrying, activities=[flaky]) as worker:
        handle = await stream_client.start_workflow(
            Retrying.run, key, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        items = await read_all(streams.for_handle(handle))
    records = [item for item in items if isinstance(item, streams.Record)]
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [record.data for record in records] == ["start", "attempt-1", "attempt-2"]
    assert len(superseded) == 1
    withdrawn = [record.data for record in records if superseded[0].withdraws(record)]
    assert withdrawn == ["attempt-1"]


@workflow.defn
class PublishThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        turns = streams.workflow_stream().topic("turns", type=str)
        turns.publish("before")
        await workflow.sleep(timedelta(milliseconds=10))
        turns.publish("after")
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
        items = await read_all(streams.for_handle(handle))
    records = [item for item in items if isinstance(item, streams.Record)]
    # The failed task wrote nothing, so the reader gets each Record once.
    assert [record.data for record in records] == ["before", "after"]
    assert not any(isinstance(item, streams.Superseded) for item in items)
    assert isinstance(items[-1], streams.Closed)


@workflow.defn
class Reading:
    @workflow.run
    async def run(self) -> str:
        try:
            streams.workflow_stream().topic("t", type=str).subscribe()
        except TypeError as err:
            return str(err)
        return "read"


async def test_workflow_code_can_not_read(stream_client: Client):
    async with new_worker(stream_client, Reading) as worker:
        result = await stream_client.execute_workflow(
            Reading.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
    assert "can't read" in result
    await asyncio.sleep(0)


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


retries: dict[str, int] = {}


@activity.defn
async def fails_once(key: str) -> None:
    retries[key] = retries.get(key, 0) + 1
    if retries[key] == 1:
        raise RuntimeError("first attempt fails")


@workflow.defn
class NoStreams:
    @workflow.run
    async def run(self, key: str) -> None:
        await workflow.execute_activity(
            fails_once,
            key,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def test_workflow_without_streams_creates_none(stream_client: Client):
    async with new_worker(stream_client, NoStreams, activities=[fails_once]) as w:
        handle = await stream_client.start_workflow(
            NoStreams.run,
            str(uuid.uuid4()),
            id=f"wf-{uuid.uuid4()}",
            task_queue=w.task_queue,
        )
        await handle.result()
    # The retry withdrew nothing and the run's close found no Stream, so none exists.
    with pytest.raises(streams.StreamNotFoundError):
        await streams.for_handle(handle).get_offset()


@workflow.defn
class WritesThenContinues:
    @workflow.run
    async def run(self, write: bool) -> str:
        if write:
            streams.workflow_stream().topic("t", type=str).publish("first run")
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
        stream = streams.for_handle(handle)
        stream.topic("t", type=str)
        items = await read_all(stream)
    assert [i.data for i in items if isinstance(i, streams.Record)] == ["first run"]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "second run"


@workflow.defn
class CancelledBlock:
    @workflow.run
    async def run(self) -> None:
        with streams.create_workflow_stream("turn") as turn:
            turn.topic("t", type=str).publish("started")
            await workflow.wait_condition(lambda: False)


async def test_cancelled_block_closes_with_a_failure(stream_client: Client):
    async with new_worker(stream_client, CancelledBlock) as worker:
        handle = await stream_client.start_workflow(
            CancelledBlock.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        turn = streams.for_handle(
            handle, name="turn", wait_for_create=timedelta(seconds=20)
        )
        turn.topic("t", type=str)
        first = await anext(aiter(turn.subscribe()))
        assert isinstance(first, streams.Record)
        await handle.cancel()
        items = await read_all(turn)
    closed = items[-1]
    assert isinstance(closed, streams.Closed)
    assert closed.failure is not None


@activity.defn
def sync_tokens(count: int) -> None:
    tokens = streams.activity_stream().topic("tokens", type=int)
    for n in range(count):
        tokens.publish(n)


@workflow.defn
class SyncWriter:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            sync_tokens, 3, start_to_close_timeout=timedelta(seconds=30)
        )


async def test_sync_activity_publishes(stream_client: Client):
    async with new_worker(
        stream_client,
        SyncWriter,
        activities=[sync_tokens],
        activity_executor=concurrent.futures.ThreadPoolExecutor(2),
    ) as worker:
        handle = await stream_client.start_workflow(
            SyncWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        stream = streams.for_handle(handle)
        stream.topic("tokens", type=int)
        items = await read_all(stream)
    assert [i.data for i in items if isinstance(i, streams.Record)] == [0, 1, 2]


@workflow.defn
class PlainWriter:
    @workflow.run
    async def run(self) -> str:
        streams.workflow_stream().topic("t", type=str).publish("plain")
        with streams.create_workflow_stream("scoped") as scoped:
            scoped.topic("t", type=str).publish("scoped")
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
        items = await read_all(streams.for_handle(handle))
        scoped = await read_all(streams.for_handle(handle, name="scoped"))
    assert [i.data for i in items if isinstance(i, streams.Record)] == ["plain"]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"
    assert [i.data for i in scoped if isinstance(i, streams.Record)] == ["scoped"]
    assert isinstance(scoped[-1], streams.Closed)


@activity.defn
async def local_tokens(count: int) -> None:
    tokens = streams.activity_stream().topic("tokens", type=int)
    for n in range(count):
        tokens.publish(n)


@workflow.defn
class LocalWriter:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_local_activity(
            local_tokens, 2, start_to_close_timeout=timedelta(seconds=30)
        )


async def test_local_activity_writes_the_default_stream(stream_client: Client):
    async with new_worker(stream_client, LocalWriter, activities=[local_tokens]) as w:
        handle = await stream_client.start_workflow(
            LocalWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=w.task_queue
        )
        await handle.result()
        stream = streams.for_handle(handle)
        stream.topic("tokens", type=int)
        items = await read_all(stream)
    records = [item for item in items if isinstance(item, streams.Record)]
    assert [record.data for record in records] == [0, 1]
    producer = records[0].producer
    assert producer is not None and producer.id.startswith("la:")
    assert (
        producer.epoch is not None and (producer.epoch.task_started_event_id or 0) > 0
    )


@workflow.defn
class FailingRun:
    @workflow.run
    async def run(self) -> str:
        turns = streams.workflow_stream().topic("turns", type=str)
        info = workflow.info()
        turns.publish(f"run-{info.attempt}")
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
        stream = streams.for_handle(handle)
        stream.topic("turns", type=str)
        items = await read_all(stream)
    records = [item for item in items if isinstance(item, streams.Record)]
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [record.data for record in records] == ["run-1", "run-2"]
    assert len(superseded) == 1
    assert [r.data for r in records if superseded[0].withdraws(r)] == ["run-1"]
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
        stream = streams.for_handle(handle)
        stream.topic("turns", type=Turn)
        stream.topic("tokens", type=str)
        decoded = await read_all(stream)
        raw = [
            item
            async for item in streams.for_handle(handle)
            .topic("tokens", type=RawValue)
            .subscribe(wait_new_records=False)
        ]
    data = [item.data for item in decoded if isinstance(item, streams.Record)]
    assert data == [Turn("model"), "a", "b", Turn("done", "A B")]
    first = raw[0]
    assert isinstance(first, streams.Record)
    assert first.data.payload.metadata["encoding"] == b"json/plain"


async def test_read_ends_idle_and_resumes(stream_client: Client):
    stream = await streams.create_stream(stream_client, f"idle-{uuid.uuid4()}")
    stream.topic("t", type=int).publish(1)
    await stream.flush()
    items = [
        item
        async for item in stream.subscribe(idle_timeout=timedelta(milliseconds=200))
    ]
    assert isinstance(items[-1], streams.Idle)
    stream.topic("t", type=int).publish(2)
    await stream.flush()
    resumed = [
        item
        async for item in stream.subscribe(
            from_offset=items[-1].offset, wait_new_records=False
        )
    ]
    assert [item.data for item in resumed if isinstance(item, streams.Record)] == [2]
    assert await stream.get_offset() > items[-1].offset


@workflow.defn
class WaitsForSignal:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> str:
        turns = streams.workflow_stream().topic("turns", type=str)
        turns.publish("before restart")
        with streams.create_workflow_stream("scoped") as scoped:
            scoped.topic("t", type=str).publish("scoped")
        await workflow.wait_condition(lambda: self.go)
        turns.publish("after restart")
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
        first = streams.for_handle(handle, wait_for_create=timedelta(seconds=20))
        assert isinstance(await anext(aiter(first.subscribe())), streams.Record)
    # The new Worker has no cache, so it replays the run and sends the creates and the close
    # of the scoped Stream again.
    async with new_worker(stream_client, WaitsForSignal, task_queue=task_queue):
        await handle.signal(WaitsForSignal.proceed)
        assert await handle.result() == "done"
        items = await read_all(streams.for_handle(handle))
        scoped = await read_all(streams.for_handle(handle, name="scoped"))
    data = [i.data for i in items if isinstance(i, streams.Record)]
    assert data == ["before restart", "after restart"]
    assert isinstance(items[-1], streams.Closed)
    assert [i.data for i in scoped if isinstance(i, streams.Record)] == ["scoped"]
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
        streams.workflow_stream().topic("t", type=str).publish("quick")
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
        async for item in streams.for_handle(handle).subscribe(wait_new_records=False)
    ]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"


async def test_plugin_shutdown_flushes_client_writers(stream_client: Client):
    [plugin] = [
        p for p in stream_client.config()["plugins"] if isinstance(p, StreamPlugin)
    ]
    stream = await streams.create_stream(stream_client, f"shutdown-{uuid.uuid4()}")
    stream.topic("t", type=str).publish("unflushed")
    await plugin.shutdown()
    written = [
        item.data
        async for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, streams.Record)
    ]
    assert written == ["unflushed"]


@workflow.defn
class WaitsForever:
    @workflow.run
    async def run(self) -> None:
        streams.workflow_stream().topic("t", type=str).publish("waiting")
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
        stream = streams.for_handle(handle, wait_for_create=timedelta(seconds=20))
        assert isinstance(await anext(aiter(stream.subscribe())), streams.Record)
    await handle.terminate()
    gc.collect()
    captured = capfd.readouterr()
    assert "Exception ignored" not in captured.err + captured.out


@workflow.defn
class CreatesEmpty:
    @workflow.run
    async def run(self) -> None:
        streams.create_workflow_stream("empty")
        await workflow.sleep(timedelta(milliseconds=10))


async def test_get_offset_of_a_created_empty_stream(stream_client: Client):
    async with new_worker(stream_client, CreatesEmpty) as worker:
        handle = await stream_client.start_workflow(
            CreatesEmpty.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
    # A created Stream with no Record gives the start offset, so a reader can begin there.
    assert await streams.for_handle(handle, name="empty").get_offset() == 0
    # A Stream that nobody created is still an error.
    with pytest.raises(streams.StreamNotFoundError):
        await streams.for_handle(handle, name="missing").get_offset()


async def test_read_by_topic_prefix_on_redis(stream_client: Client):
    stream = await streams.create_stream(stream_client, f"prefix-{uuid.uuid4()}")
    for topic in ("agent/a", "agent/b", "other", "exact"):
        stream.topic(topic, type=str).publish(topic)
    await stream.flush()
    read = [
        item.topic
        async for item in stream.subscribe(
            topics=[stream.topic("exact", type=str)],
            topic_prefixes=["agent/"],
            wait_new_records=False,
        )
        if isinstance(item, streams.Record)
    ]
    assert read == ["agent/a", "agent/b", "exact"]


@workflow.defn
class LeavesStreamOpen:
    @workflow.run
    async def run(self) -> streams.StreamRef:
        # Not in a with block, so nothing closes it when the run ends.
        open_stream = streams.create_workflow_stream("open")
        open_stream.topic("t", type=str).publish("only")
        return open_stream.ref


async def test_for_ref_ends_with_the_owning_run(stream_client: Client):
    async with new_worker(stream_client, LeavesStreamOpen) as worker:
        handle = await stream_client.start_workflow(
            LeavesStreamOpen.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        ref = await handle.result()
    assert ref.workflow_id == handle.id
    assert ref.first_execution_run_id == handle.first_execution_run_id
    stream = streams.for_ref(stream_client, ref, end_with_run=True)
    items = [item async for item in stream.subscribe(type=str)]
    assert [i.data for i in items if isinstance(i, streams.Record)] == ["only"]
    # A ref to a Stream that the finished run never created ends at once, not after the
    # whole wait.
    missing = streams.StreamRef(
        ref.id + "-missing",
        workflow_id=ref.workflow_id,
        first_execution_run_id=ref.first_execution_run_id,
    )
    waiting = streams.for_ref(
        stream_client, missing, end_with_run=True, wait_for_create=timedelta(minutes=5)
    )
    start = asyncio.get_running_loop().time()
    assert [item async for item in waiting.subscribe()] == []
    assert asyncio.get_running_loop().time() - start < 30


async def test_idle_ends_a_read_that_ends_with_the_run(stream_client: Client):
    async with new_worker(stream_client, WaitsForever) as worker:
        handle = await stream_client.start_workflow(
            WaitsForever.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = streams.for_handle(
            handle, end_with_run=True, wait_for_create=timedelta(seconds=20)
        )
        # The read polls in short steps to look at the run, and the idle time still counts
        # across them.
        items = [
            item async for item in stream.subscribe(idle_timeout=timedelta(seconds=3))
        ]
        await handle.terminate()
    assert isinstance(items[0], streams.Record)
    assert isinstance(items[-1], streams.Idle)


async def test_workflow_calls_carry_the_owner(stream_client: Client):
    [plugin] = [
        p for p in stream_client.config()["plugins"] if isinstance(p, StreamPlugin)
    ]
    sent: list[bytes] = []
    call = plugin._temporal_stream_backend.call

    def record(method: str, request: Any) -> bytes:
        if method == "BufferWorkflowCalls":
            sent.append(request.SerializeToString())
        return call(method, request)

    plugin._temporal_stream_backend.call = record  # type: ignore[method-assign]
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
        streams.workflow_stream().topic("t", type=str).publish(f"run-{attempt}")
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
        reader = streams.for_handle(
            handle, end_with_run=True, wait_for_create=timedelta(seconds=20)
        )
        items = [item async for item in reader.subscribe(type=str)]
    records = [item for item in items if isinstance(item, streams.Record)]
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    closes = [item for item in items if isinstance(item, streams.Closed)]
    # Only the run that ends the chain closes the Stream, with its failure, and it is last.
    assert len(closes) == 1 and items[-1] is closes[0]
    assert closes[0].failure is not None
    if non_retryable:
        # A non-retryable failure ends the chain on attempt 1, so it closes at once.
        assert [r.data for r in records] == ["run-1"]
        assert superseded == []
        return
    # Attempts 1 and 2 left the Stream open, so attempts 2 and 3 could write. Each retry run
    # withdraws the run before it.
    assert [r.data for r in records] == ["run-1", "run-2", "run-3"]
    assert len(superseded) == 2
    assert [r.data for r in records if superseded[0].withdraws(r)] == ["run-1"]
    assert [r.data for r in records if superseded[1].withdraws(r)] == ["run-2"]
    assert not any(s.withdraws(records[-1]) for s in superseded)


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
