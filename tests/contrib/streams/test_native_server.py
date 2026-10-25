"""The native lane: the same calls against a server that serves the Stream service.

Set ``TEMPORAL_STREAMS_NATIVE_TARGET`` to its address to run it.
"""

import asyncio
import base64
import concurrent.futures
import gc
import json
import uuid
import weakref
from datetime import timedelta
from typing import Any, cast

import pytest

import temporalio.api.streamservice.v1 as stream_pb
from temporalio import activity, workflow
from temporalio.api.enums.v1 import WorkflowTaskFailedCause
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy, streams
from temporalio.common.streams import _native, _plugin
from temporalio.common.streams._native import POSITIONS_TYPE
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner
from tests.contrib.streams import test_redis
from tests.contrib.streams.test_workflow_reads import (
    Events,
    Reader,
    ReadInput,
    Writer,
)
from tests.helpers import new_worker


@activity.defn
async def write_native(stream: streams.ActivityStream[Events]) -> None:
    await stream.topic(Events.tokens).publish("activity")


@activity.defn
def write_native_sync(stream: streams.SyncActivityStream[Events]) -> None:
    stream.topic(Events.tokens).publish("sync activity")


@workflow.defn
class WritesNativeAndRedis:
    @workflow.run
    async def run(self) -> list[str | None]:
        on_redis = await workflow.create_stream(streams.with_name(Events, "old"))
        on_native = await workflow.create_stream(
            streams.with_name(Events, "new"), backend="native"
        )
        await on_redis.topic(Events.tokens).publish("redis")
        await on_native.topic(Events.tokens).publish("workflow 1", "workflow 2")
        # The Activities write after the run's own writes, as on Redis.
        await workflow.execute_activity(
            write_native,
            streams.ActivityStream.from_handle(on_native),
            start_to_close_timeout=timedelta(seconds=10),
        )
        await workflow.execute_activity(
            write_native_sync,
            streams.SyncActivityStream.from_handle(on_native),
            start_to_close_timeout=timedelta(seconds=10),
        )
        await on_native.topic(Events.tokens).publish("last")
        return [on_redis.backend, on_native.backend]


async def test_native_client_round_trip(native_client: Client):
    stream = await native_client.create_stream(Events, id=f"native-{uuid.uuid4()}")
    assert await stream.topic(Events.tokens).publish("a", "b")
    await stream.close(result="done")
    items = [item async for item in stream.subscribe()]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "a",
        "b",
    ]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"


async def test_native_workflow_reads_and_replays(native_client: Client):
    async with new_worker(native_client, Writer, Reader) as worker:
        writer = await native_client.start_workflow(
            Writer.run, None, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = writer.stream(Events)
        reader = await native_client.start_workflow(
            Reader.run,
            ReadInput(streams.WorkflowStream.from_handle(stream)),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await reader.result() == [
            "turn user hi",
            "a",
            "b",
            "c",
            "closed written",
        ]
    # A read is direct, so the reader's History holds positions and no page of Records.
    history = await reader.fetch_history()
    assert base64.b64encode(POSITIONS_TYPE).decode() in history.to_json()
    # The wake's answer is in History, and it holds offsets only.
    wakes = [
        stream_pb.ReadRecordsResponse.FromString(
            event.nexus_operation_completed_event_attributes.result.data
        )
        for event in history.events
        if event.HasField("nexus_operation_completed_event_attributes")
    ]
    assert wakes and not any(wake.records for wake in wakes)
    # A replay reads each page again on the connection of its stream_client, once the cache
    # is gone.
    replayer = Replayer(workflows=[Writer, Reader], stream_client=native_client)
    await replayer.replay_workflow(await writer.fetch_history())
    await replayer.replay_workflow(history)
    _native._pages.clear()
    await replayer.replay_workflow(history)


async def test_redis_and_native_on_a_native_server(
    native_client: Client, redis_url: str
):
    # The client registers only Redis, and has the native backend with no registration.
    config = native_client.config()
    config["stream_backends"] = [
        RedisStreamBackend(redis_url, key_prefix=f"t-{uuid.uuid4()}", default=True),
    ]
    two = Client(**config)
    async with new_worker(
        two,
        WritesNativeAndRedis,
        activities=[write_native, write_native_sync],
        activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=2),
    ) as worker:
        handle = await two.start_workflow(
            WritesNativeAndRedis.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == ["redis", "native"]
    new = handle.stream(streams.with_name(Events, "new"), backend="native")
    items = [item async for item in new.subscribe()]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "workflow 1",
        "workflow 2",
        "activity",
        "sync activity",
        "last",
    ]
    # The server closes a native Stream when its run chain ends, with the run's result.
    assert isinstance(items[-1], streams.Closed)
    assert items[-1].result == ["redis", "native"]


@workflow.defn
class LeavesItsStreamOpen:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("a")
        return "done"


async def test_the_server_closes_a_stream_when_its_run_chain_ends(
    native_client: Client,
):
    # A Workflow's create asks the server to close the Stream when the run chain ends, with
    # the result of its last run.
    async with new_worker(native_client, LeavesItsStreamOpen) as worker:
        handle = await native_client.start_workflow(
            LeavesItsStreamOpen.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "done"
    items = [item async for item in handle.stream(Events).subscribe()]
    assert [item.data for item in items if isinstance(item, streams.Record)] == ["a"]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"
    assert items[-1].status == streams.CloseStatus.COMPLETED


@workflow.defn
class WritesByBareId:
    @workflow.run
    async def run(self, stream_id: str) -> str:
        stream = streams.WorkflowStream[Events](id=stream_id, backend="native")
        try:
            await stream.topic(Events.tokens).publish("lost")
        except streams.StreamNotFoundError as err:
            return err.reason
        return "written"


async def test_a_write_without_the_owner_fields_raises(native_client: Client):
    # A Workflow's Stream nests in its Workflow, so a handle without the owner fields finds
    # nothing, and the write's Local Activity raises at the await.
    async with new_worker(native_client, Writer, WritesByBareId) as worker:
        writer = await native_client.start_workflow(
            Writer.run, None, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await writer.result()
        stream_id = f"{writer.id}:{writer.first_execution_run_id}:events"
        assert (
            await native_client.execute_workflow(
                WritesByBareId.run,
                stream_id,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "stream_not_found"
        )


async def test_a_rerun_local_activity_withdraws_its_failed_task(native_client: Client):
    # The retry task sends the failed task, so the server withdraws its Local Activity Records.
    async with new_worker(
        native_client,
        test_redis.LocalThenFailTask,
        activities=[test_redis.local_tokens],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await native_client.start_workflow(
            test_redis.LocalThenFailTask.run,
            str(uuid.uuid4()),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await test_redis.read_all(test_redis.workflow_stream(handle))
    withdrawn = [item for item in items if isinstance(item, streams.Superseded)]
    visible = [
        item.data
        for item in test_redis.records(items)
        if not any(w.withdraws(item) for w in withdrawn)
    ]
    assert visible == ["2", "3"]


@workflow.defn
class ReadsNewTokens:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> list[Any]:
        items = stream.subscribe(Events.tokens, offset=streams.END)
        return [
            item.data if isinstance(item, streams.Record) else "closed"
            async for item in items
        ]


async def until_wake(handle: WorkflowHandle[Any, Any]) -> None:
    # A reader with an empty page waits on the server with a System Nexus operation.
    for _ in range(100):
        history = await handle.fetch_history()
        if any(
            event.HasField("nexus_operation_scheduled_event_attributes")
            for event in history.events
        ):
            return
        await asyncio.sleep(0.2)
    raise AssertionError("The reader never waited")


async def test_a_native_read_from_the_end_keeps_what_comes_after_its_first_page(
    native_client: Client,
):
    # The server resolves END on each call, so the wait and the read after it start where the
    # first page ended. Else the Record that wakes the reader is behind its start.
    stream = await native_client.create_stream(Events, id=f"native-{uuid.uuid4()}")
    await stream.topic(Events.tokens).publish("old")
    async with new_worker(native_client, ReadsNewTokens) as worker:
        reader = await native_client.start_workflow(
            ReadsNewTokens.run,
            streams.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await until_wake(reader)
        await stream.topic(Events.tokens).publish("new")
        await stream.close()
        assert await reader.result() == ["new", "closed"]


@workflow.defn
class ClosesItsRead:
    def __init__(self) -> None:
        self._stop = False

    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> str:
        items = stream.subscribe(offset=streams.END)
        reading = asyncio.create_task(items.__anext__())
        await workflow.wait_condition(lambda: self._stop)
        await items.aclose()
        try:
            await reading
        except StopAsyncIteration:
            return "ended"
        return "read"

    @workflow.signal
    def stop(self) -> None:
        self._stop = True


async def test_closing_a_native_read_cancels_its_wake(native_client: Client):
    # A wake may park for days, so a read that its code closed must not keep it.
    stream = await native_client.create_stream(Events, id=f"native-{uuid.uuid4()}")
    async with new_worker(native_client, ClosesItsRead) as worker:
        reader = await native_client.start_workflow(
            ClosesItsRead.run,
            streams.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await until_wake(reader)
        await reader.signal(ClosesItsRead.stop)
        assert await reader.result() == "ended"
    history = await reader.fetch_history()
    assert any(
        event.HasField("nexus_operation_cancel_requested_event_attributes")
        for event in history.events
    )


@workflow.defn
class ReadsUntilHalfRetention:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> list[Any]:
        items = stream.subscribe(Events.tokens)
        seen = [item.data async for item in items if isinstance(item, streams.Record)]
        return [seen, items.continue_as_new_suggested, items.next_offset]


async def test_a_native_reader_of_a_client_stream_ends_at_half_its_retention(
    native_client: Client,
):
    # Retention trims a Stream that no Workflow owns under its readers, and a replay reads the
    # Stream again. So the read ends in time for the run to continue as new.
    stream = await native_client.create_stream(
        Events,
        id=f"native-{uuid.uuid4()}",
        retention=timedelta(seconds=6),
    )
    offset = await stream.topic(Events.tokens).publish("a")
    async with new_worker(native_client, ReadsUntilHalfRetention) as worker:
        seen, suggested, next_offset = await native_client.execute_workflow(
            ReadsUntilHalfRetention.run,
            streams.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert seen == ["a"] and suggested and next_offset > offset


@activity.defn
async def closes_its_stream() -> str:
    stream = await activity.create_stream(Events)
    await stream.topic(Events.tokens).publish("a", "b")
    await stream.close()
    return "done"


@activity.defn
async def leaves_its_stream_open() -> str:
    stream = await activity.create_stream(Events)
    await stream.topic(Events.tokens).publish("a")
    return "done"


async def test_a_standalone_activity_owns_its_stream(native_client: Client):
    # The Activity may close its own Stream. The server closes it when the Activity ends, with
    # its status and result, so a Stream that the Activity leaves open doesn't stay.
    task_queue = f"tq-{uuid.uuid4()}"
    async with new_worker(
        native_client,
        activities=[closes_its_stream, leaves_its_stream_open],
        task_queue=task_queue,
    ):
        closed = await native_client.start_activity(
            closes_its_stream,
            id=f"act-{uuid.uuid4()}",
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=30),
        )
        left_open = await native_client.start_activity(
            leaves_its_stream_open,
            id=f"act-{uuid.uuid4()}",
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=30),
        )
        assert await closed.result() == "done"
        assert await left_open.result() == "done"
    items = [item async for item in closed.stream(Events).subscribe()]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "a",
        "b",
    ]
    # The Activity's own close got past the early owner check, so its close has no result.
    assert isinstance(items[-1], streams.Closed) and items[-1].result is None
    left = [item async for item in left_open.stream(Events).subscribe()]
    assert [item.data for item in left if isinstance(item, streams.Record)] == ["a"]
    assert isinstance(left[-1], streams.Closed) and left[-1].result == "done"
    assert left[-1].status == streams.CloseStatus.COMPLETED


@activity.defn
async def streams_in_a_block() -> str:
    attempt = activity.info().attempt
    async with activity.create_stream(Events) as stream:
        await stream.topic(Events.tokens).publish(f"attempt {attempt}")
        if attempt == 1:
            raise RuntimeError("transient")
    return f"done on attempt {attempt}"


async def test_a_block_end_leaves_a_standalone_activitys_stream_open(
    native_client: Client,
):
    # A retry of the Activity creates its Stream again, so a close at the end of the failed
    # attempt's block would fail every retry.
    task_queue = f"tq-{uuid.uuid4()}"
    async with new_worker(
        native_client, activities=[streams_in_a_block], task_queue=task_queue
    ):
        handle = await native_client.start_activity(
            streams_in_a_block,
            id=f"act-{uuid.uuid4()}",
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100), maximum_attempts=2
            ),
        )
        assert await handle.result() == "done on attempt 2"
    items = [item async for item in handle.stream(Events).subscribe()]
    assert "attempt 2" in [
        item.data for item in items if isinstance(item, streams.Record)
    ]
    # The server's close carries the result of the attempt that ended the Activity.
    assert isinstance(items[-1], streams.Closed)
    assert items[-1].result == "done on attempt 2"


@workflow.defn
class ReadsAll:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> list[Any]:
        return [
            item.data if isinstance(item, streams.Record) else f"closed {item.result}"
            async for item in stream.subscribe()
            if not isinstance(item, streams.Superseded)
        ]


async def test_a_workflow_reads_a_standalone_activitys_stream(native_client: Client):
    # The server nests the Stream in its Activity, so the read names the Activity.
    task_queue = f"tq-{uuid.uuid4()}"
    async with new_worker(
        native_client,
        ReadsAll,
        activities=[leaves_its_stream_open],
        task_queue=task_queue,
    ):
        activity_handle = await native_client.start_activity(
            leaves_its_stream_open,
            id=f"act-{uuid.uuid4()}",
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=30),
        )
        await activity_handle.result()
        reader = await native_client.start_workflow(
            ReadsAll.run,
            streams.WorkflowStream.from_handle(activity_handle.stream(Events)),
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        assert await reader.result() == ["a", "closed done"]
    # The server keeps the reader's floor on a nested Stream, so the read asks no Retention.
    local_activities = [
        json.loads(
            event.marker_recorded_event_attributes.details["data"].payloads[0].data
        )["activity_type"]
        for event in (await reader.fetch_history()).events
        if event.HasField("marker_recorded_event_attributes")
    ]
    assert local_activities
    assert _native._describe_activity_name("native") not in local_activities


@workflow.defn
class CountsThenWaits:
    def __init__(self) -> None:
        self._go = False

    @workflow.signal
    def finish(self) -> None:
        self._go = True

    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> int:
        count = len([item async for item in stream.subscribe(wait_new_records=False)])
        await workflow.wait_condition(lambda: self._go)
        return count


async def until_marker(handle: WorkflowHandle[Any, Any]) -> None:
    for _ in range(100):
        history = await handle.fetch_history()
        if any(
            event.HasField("marker_recorded_event_attributes")
            for event in history.events
        ):
            return
        await asyncio.sleep(0.1)
    raise AssertionError("The reader never read")


async def until_task_failure(handle: WorkflowHandle[Any, Any]) -> Any:
    for _ in range(100):
        history = await handle.fetch_history()
        for event in history.events:
            if event.HasField("workflow_task_failed_event_attributes"):
                return event.workflow_task_failed_event_attributes
        await asyncio.sleep(0.1)
    raise AssertionError("No Workflow Task failed")


async def test_a_replay_that_reads_other_records_is_nondeterministic(
    native_client: Client, monkeypatch: pytest.MonkeyPatch
):
    # The codec step checks a replayed page against its digest, outside the Workflow. The
    # server and the Replayer still learn that the replay was nondeterministic.
    stream = await native_client.create_stream(Events, id=f"native-{uuid.uuid4()}")
    await stream.topic(Events.tokens).publish("a", "b")
    async with new_worker(
        native_client, CountsThenWaits, max_cached_workflows=0
    ) as worker:
        handle = await native_client.start_workflow(
            CountsThenWaits.run,
            streams.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await until_marker(handle)
        monkeypatch.setattr(_native, "_digest", lambda records: b"other")
        _native._pages.clear()
        await handle.signal(CountsThenWaits.finish)
        failed = await until_task_failure(handle)
        await handle.terminate()
    assert (
        failed.cause
        == WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR
    )
    assert "found other Records" in failed.failure.message
    replayer = Replayer(workflows=[CountsThenWaits], stream_client=native_client)
    result = await replayer.replay_workflow(
        await handle.fetch_history(), raise_on_replay_failure=False
    )
    assert isinstance(result.replay_failure, workflow.NondeterminismError)


async def test_each_client_of_a_native_backend_calls_on_its_own_connection():
    # One backend on two clients gives each its own runtime, so a call doesn't go on the
    # connection of the client that came last.
    native = streams.NativeStreamBackend()
    first = await Client.connect("127.0.0.1:1", lazy=True, stream_backends=[native])
    second = await Client.connect("127.0.0.1:2", lazy=True, stream_backends=[native])
    runtimes = [
        cast(Any, native._backend_for(client))._runtime for client in (first, second)
    ]
    assert runtimes[0] is not runtimes[1]
    assert [runtime._service() for runtime in runtimes] == [
        first.service_client,
        second.service_client,
    ]
    # The native backend that no client registers works the same, and makes no call.
    plain = await Client.connect("127.0.0.1:3", lazy=True)
    _, calls = _plugin.client_backend(plain, None)
    assert cast(Any, calls)._runtime._service() is plain.service_client
    assert _plugin.client_backend(plain, None)[1] is calls
    # No call built these runtimes, so a shutdown connects to none of them.
    await native.shutdown()


async def test_a_native_backend_keeps_no_client_alive():
    # The implicit backend lives as long as the process, and every Worker's client goes
    # through it.
    native = streams.NativeStreamBackend()
    registered = await Client.connect(
        "127.0.0.1:1", lazy=True, stream_backends=[native]
    )
    plain = await Client.connect("127.0.0.1:2", lazy=True)
    native._backend_for(registered)
    _plugin.client_backend(plain, None)
    services = [
        weakref.ref(registered.service_client),
        weakref.ref(plain.service_client),
    ]
    del registered, plain
    gc.collect()
    assert [service() for service in services] == [None, None]
    assert not native._connections
