"""Readers that sleep on the stream's notifier, against a server that has it.

The stock dev server has no stream notifier, so every case here skips there
with "no stream notifier". ``scripts/streams_live_lane.sh`` starts a server
with the notifier, the Workflow wake target and short recheck and poll
limits, and fails when any of these cases skips.

Both consumers are covered: a client reader that waits with a long poll,
and a Workflow reader that reads through Local Activities and wakes on a
Workflow Task.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payloads
from temporalio.api.enums.v1 import EventType, StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import (
    DescribeStreamNotifierRequest,
    DescribeStreamNotifierResponse,
)
from temporalio.bridge.proto.streams import ReadRangeRequest
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.streams import (
    RecordKind,
    StreamDriver,
    StreamPosition,
    StreamStorePlugin,
    close_workflow_stream,
    get_stream_handle,
    topic,
    workflow_reader,
    workflow_writer,
)
from temporalio.contrib.streams._replay_reads import POSITIONS_TYPE, ReadStates
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Replayer
from tests.contrib.streams._support import (
    connect_with,
    new_schema,
    new_workflow_id,
    postgres_streams,
)
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

TOKENS = topic("tokens", str)
# The lane sets streamnotifier.pollMaxWait and streamnotifier.recheckInitial to
# a few seconds, so the cases that wait for them end in seconds.
QUICK = 4.0


async def _first_run_id(client: Client, workflow_id: str) -> str:
    described = await client.get_workflow_handle(workflow_id).describe()
    return described.raw_description.workflow_execution_info.first_run_id


async def describe_notifier(
    client: Client, workflow_id: str, name: str = "tokens"
) -> DescribeStreamNotifierResponse | None:
    """The stream's notifier, or ``None`` before anything started it."""
    request = DescribeStreamNotifierRequest(
        namespace=client.namespace,
        stream_ref=StreamReference(
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
            workflow_id=workflow_id,
            run_id=await _first_run_id(client, workflow_id),
            topic=name,
        ),
    )
    try:
        return await client.workflow_service.describe_stream_notifier(request)
    except RPCError as error:
        if error.status == RPCStatusCode.NOT_FOUND:
            return None
        if error.status == RPCStatusCode.UNIMPLEMENTED:
            pytest.skip("the server has no stream notifier")
        raise


async def skip_without_notifier(client: Client) -> None:
    request = DescribeStreamNotifierRequest(
        namespace=client.namespace,
        stream_ref=StreamReference(
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
            workflow_id=f"probe-{uuid.uuid4()}",
            run_id=str(uuid.uuid4()),
            topic="tokens",
        ),
    )
    try:
        await client.workflow_service.describe_stream_notifier(request)
    except RPCError as error:
        if error.status == RPCStatusCode.UNIMPLEMENTED:
            pytest.skip("the server has no stream notifier")


def store(kind: str) -> StreamStorePlugin:
    if kind == "memory":
        return MemoryStreams()
    if kind == "postgres":
        return postgres_streams()
    if kind == "cluster":
        url = os.environ.get("STREAMS_REDIS_CLUSTER_URL", "")
        if not url:
            pytest.skip("set STREAMS_REDIS_CLUSTER_URL to run on a Redis Cluster")
        return RedisStreams(url, cluster=True, key_prefix=f"live-{uuid.uuid4().hex}")
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return RedisStreams(url, key_prefix=f"live-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis", "postgres"])
async def notifying(request: pytest.FixtureRequest, client: Client) -> Client:
    """A client whose store notifies, on a server with the notifier."""
    await skip_without_notifier(client)
    return await connect_with(client, store(request.param).notify_on_append())


@pytest.fixture(params=["memory", "redis", "cluster", "postgres"])
async def driven(
    request: pytest.FixtureRequest, client: Client
) -> AsyncIterator[Client]:
    """A client whose store keeps a notification log, with a StreamDriver running."""
    await skip_without_notifier(client)
    streams = await connect_with(
        client, store(request.param).notify_on_append(notification_log=True)
    )
    driver = asyncio.create_task(
        StreamDriver(streams, sweep_interval=timedelta(seconds=2)).run()
    )
    try:
        yield streams
    finally:
        driver.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await driver


async def until(check: Any, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while not await check():
        if time.monotonic() > deadline:
            raise AssertionError("the condition did not hold in time")
        await asyncio.sleep(0.1)


# Client readers.


async def test_live_an_idle_client_reader_wakes_on_the_next_append(
    notifying: Client,
):
    async with new_worker(notifying, Waits) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        handle = get_stream_handle(notifying, owner, topic=TOKENS)
        records = handle.read()
        first = asyncio.ensure_future(records.__anext__())
        # Past the idle wait, the read came back empty and the reader waits
        # on the notifier.
        await asyncio.sleep(3)
        started = time.monotonic()
        await handle.producer(producer_id="p", attempt=1).append("woke")
        assert (await asyncio.wait_for(first, 10)).value == "woke"
        # The notification woke it, not the poll limit.
        assert time.monotonic() - started < QUICK
        notifier = await describe_notifier(notifying, owner)
        assert notifier is not None and notifier.counter > 0
        await records.aclose()
        await notifying.get_workflow_handle(owner).signal(Waits.finish)


async def test_live_a_sleeping_client_wakes_through_the_driver(driven: Client):
    async with new_worker(driven, Waits) as worker:
        owner = await running_owner(driven, worker.task_queue)
        handle = get_stream_handle(driven, owner, topic=TOKENS)
        records = handle.read()
        first = asyncio.ensure_future(records.__anext__())
        # Past the idle wait, the read came back empty and the reader waits
        # on the notifier, which its poll started.
        await asyncio.sleep(3)
        started = time.monotonic()
        await handle.producer(producer_id="p", attempt=1).append("woke")
        assert (await asyncio.wait_for(first, 10)).value == "woke"
        # The driver's notification woke it, not the poll limit.
        assert time.monotonic() - started < QUICK
        await records.aclose()
        await driven.get_workflow_handle(owner).signal(Waits.finish)


async def test_live_streams_on_many_slots_all_wake_through_the_driver(
    driven: Client,
):
    # On a cluster each owner's chain hashes to its own slot, so its log
    # entries land on that slot's primary, and the driver drains them all.
    async with new_worker(driven, Waits) as worker:
        owners = [await running_owner(driven, worker.task_queue) for _ in range(8)]
        reads = [get_stream_handle(driven, o, topic=TOKENS).read() for o in owners]
        firsts = [asyncio.ensure_future(r.__anext__()) for r in reads]
        await asyncio.sleep(3)
        for owner in owners:
            await (
                get_stream_handle(driven, owner, topic=TOKENS)
                .producer(producer_id="p", attempt=1)
                .append(owner)
            )
        woke = await asyncio.wait_for(asyncio.gather(*firsts), 15)
        assert [r.value for r in woke] == owners
        for read in reads:
            await read.aclose()
        for owner in owners:
            await driven.get_workflow_handle(owner).signal(Waits.finish)


@pytest.mark.parametrize("kind", ["redis", "postgres"])
async def test_live_a_client_reader_finds_a_lost_notification_at_the_poll_limit(
    client: Client, kind: str
):
    # The producer's process never notifies, as after a crash between the
    # append and the notification. The reader's read at the poll limit
    # (streamnotifier.pollMaxWait, a few seconds in the lane) finds the record.
    reading, silent = await two_processes(client, kind)
    async with new_worker(reading, Waits) as worker:
        owner = await running_owner(reading, worker.task_queue)
        records = get_stream_handle(reading, owner, topic=TOKENS).read()
        first = asyncio.ensure_future(records.__anext__())
        await asyncio.sleep(3)
        await (
            get_stream_handle(silent, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("late")
        )
        assert (await asyncio.wait_for(first, 30)).value == "late"
        await records.aclose()
        await reading.get_workflow_handle(owner).signal(Waits.finish)


async def two_processes(client: Client, kind: str) -> tuple[Client, Client]:
    """A reader's client that notifies, and a producer's that never does, on one store."""
    await skip_without_notifier(client)
    if kind == "postgres":
        schema = new_schema()
        reading = await connect_with(
            client, postgres_streams(schema).notify_on_append()
        )
        return reading, await connect_with(client, postgres_streams(schema))
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to share a store across two clients")
    prefix = f"live-silent-{uuid.uuid4().hex}"
    reading = await connect_with(
        client, RedisStreams(url, key_prefix=prefix).notify_on_append()
    )
    silent = await connect_with(client, RedisStreams(url, key_prefix=prefix))
    return reading, silent


async def test_live_every_append_notifies(client: Client):
    await skip_without_notifier(client)
    streams = await connect_with(client, MemoryStreams().notify_on_append())
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        # A notification for a stream with no notifier is dropped, so a
        # sleeping reader starts the notifier first.
        sleeping = get_stream_handle(streams, owner, topic=TOKENS).read()
        reading = asyncio.ensure_future(sleeping.__anext__())
        await until(lambda: _has_notifier(streams, owner))
        producer = get_stream_handle(streams, owner, topic=TOKENS).producer(
            producer_id="p", attempt=1
        )
        counters: list[int] = []
        for n in range(3):
            await producer.append(str(n))
            await store_for(streams).flush_notifications()

            async def moved() -> bool:
                notifier = await describe_notifier(streams, owner)
                return notifier is not None and notifier.counter > (
                    counters[-1] if counters else 0
                )

            await until(moved)
            notifier = await describe_notifier(streams, owner)
            assert notifier is not None
            counters.append(notifier.counter)
        assert counters == sorted(counters) and len(set(counters)) == 3
        await reading
        await sleeping.aclose()
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def _has_notifier(client: Client, owner: str) -> bool:
    return await describe_notifier(client, owner) is not None


def store_for(client: Client) -> StreamStorePlugin:
    for plugin in client.config()["plugins"]:
        if isinstance(plugin, StreamStorePlugin):
            return plugin
    raise AssertionError("no store")


async def test_live_a_close_with_an_outcome_reaches_the_notifier(notifying: Client):
    async with new_worker(notifying, Waits) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        handle = get_stream_handle(notifying, owner, topic=TOKENS)
        records = handle.read()
        first = asyncio.ensure_future(records.__anext__())
        await asyncio.sleep(3)
        await handle.close(error=ApplicationError("gave up", type="GaveUp"))
        # The close wakes the sleeping reader, and its read ends with the error.
        with pytest.raises(ApplicationError) as raised:
            await asyncio.wait_for(first, 10)
        assert raised.value.type == "GaveUp"
        notifier = await describe_notifier(notifying, owner)
        assert notifier is not None and notifier.closed
        assert notifier.close_failure.application_failure_info.type == "GaveUp"
        await notifying.get_workflow_handle(owner).signal(Waits.finish)


# Workflow readers.


@workflow.defn
class Reads:
    """Reads an owner's ``tokens`` until it ends, then says what it saw."""

    @workflow.run
    async def run(
        self, owner: str, stop_after: int, record_batches: bool = False
    ) -> list[str]:
        seen: list[str] = []
        async with workflow_reader(
            owner, topic=TOKENS, replay_from_store=not record_batches
        ) as reader:
            async for batch in reader:
                for record in batch.records:
                    if record.kind is RecordKind.DATA:
                        seen.append(str(record.value))
                    else:
                        seen.append(record.kind.name)
                if stop_after and len(seen) >= stop_after:
                    break
        return seen


async def _sleeping(client: Client, reader_id: str, owner: str) -> None:
    """Wait until the reader attached and its read came back empty."""

    async def attached() -> bool:
        notifier = await describe_notifier(client, owner)
        return notifier is not None and any(
            c.workflow_target.workflow_id == reader_id for c in notifier.callbacks
        )

    await until(attached)
    # Past the attach, the next read waits the idle time and comes back empty.
    await asyncio.sleep(3)


@pytest.mark.parametrize("record_batches", [False, True])
async def test_live_a_workflow_reader_sleeps_and_wakes_on_an_append(
    notifying: Client, record_batches: bool
):
    async with new_worker(notifying, Waits, Reads) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        reader_id = new_workflow_id()
        reader = await notifying.start_workflow(
            Reads.run,
            args=[owner, 2, record_batches],
            id=reader_id,
            task_queue=worker.task_queue,
        )
        producer = get_stream_handle(notifying, owner, topic=TOKENS).producer(
            producer_id="p", attempt=1
        )
        await producer.append("first")
        await _sleeping(notifying, reader_id, owner)
        tasks_before = await _completed_tasks(reader)
        await producer.append("second")
        assert await asyncio.wait_for(reader.result(), 20) == ["first", "second"]
        # The sleep cost no task until the wake.
        assert tasks_before >= 1
        await notifying.get_workflow_handle(owner).signal(Waits.finish)
    # A replay from the store needs the store, so the Replayer takes the
    # client's. A cleared cache makes it read each range again.
    store = store_for(notifying)
    store._batches.clear()
    await Replayer(workflows=[Reads], plugins=[store]).replay_workflow(
        await reader.fetch_history()
    )


async def test_live_a_workflow_reader_wakes_through_the_driver(driven: Client):
    async with new_worker(driven, Waits, Reads) as worker:
        owner = await running_owner(driven, worker.task_queue)
        reader_id = new_workflow_id()
        reader = await driven.start_workflow(
            Reads.run,
            args=[owner, 2, False],
            id=reader_id,
            task_queue=worker.task_queue,
        )
        producer = get_stream_handle(driven, owner, topic=TOKENS).producer(
            producer_id="p", attempt=1
        )
        await producer.append("first")
        await _sleeping(driven, reader_id, owner)
        started = time.monotonic()
        await producer.append("second")
        assert await asyncio.wait_for(reader.result(), 20) == ["first", "second"]
        # The driver's notification woke it, not the recheck.
        assert time.monotonic() - started < QUICK
        await driven.get_workflow_handle(owner).signal(Waits.finish)


@workflow.defn
class ReadsQuickly:
    """Reads ``count`` records with a short idle wait, so it sleeps between them."""

    @workflow.run
    async def run(self, owner: str, count: int) -> int:
        seen = 0
        async with workflow_reader(
            owner, topic=TOKENS, idle_wait=timedelta(milliseconds=50)
        ) as reader:
            async for batch in reader:
                seen += len(batch.records)
                if seen >= count:
                    break
        return seen


async def test_live_a_busy_stream_wakes_a_workflow_reader_about_once_per_window(
    driven: Client,
):
    # The server delivers the first notification of a window at once and the
    # newest one at the window's end, so a busy stream wakes the reader about
    # once per window (streamnotifier.debounceWindow, 1 second by default).
    async with new_worker(driven, Waits, ReadsQuickly) as worker:
        owner = await running_owner(driven, worker.task_queue)
        producer = get_stream_handle(driven, owner, topic=TOKENS).producer(
            producer_id="p", attempt=1
        )
        await producer.append("first")
        reader = await driven.start_workflow(
            ReadsQuickly.run,
            args=[owner, 31],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await asyncio.sleep(2)
        started = time.monotonic()
        for n in range(30):
            await producer.append(str(n))
            await asyncio.sleep(0.1)
        elapsed = time.monotonic() - started
        assert await asyncio.wait_for(reader.result(), 30) == 31
        await driven.get_workflow_handle(owner).signal(Waits.finish)
    wakes = sum(
        len(e.workflow_task_scheduled_event_attributes.stream_wakes)
        for e in (await reader.fetch_history()).events
        if e.HasField("workflow_task_scheduled_event_attributes")
    )
    # One leading and one trailing delivery per window, plus the first wake.
    assert 1 <= wakes <= 2 * (int(elapsed) + 1) + 1, wakes


async def _completed_tasks(handle: Any) -> int:
    history = await handle.fetch_history()
    return sum(
        e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
        for e in history.events
    )


async def test_live_a_workflow_reader_on_an_empty_stream_wakes_on_the_first_append(
    notifying: Client,
):
    async with new_worker(notifying, Waits, Reads) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        reader_id = new_workflow_id()
        reader = await notifying.start_workflow(
            Reads.run, args=[owner, 1], id=reader_id, task_queue=worker.task_queue
        )
        await _sleeping(notifying, reader_id, owner)
        await (
            get_stream_handle(notifying, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("only")
        )
        assert await asyncio.wait_for(reader.result(), 20) == ["only"]
        await notifying.get_workflow_handle(owner).signal(Waits.finish)


async def test_live_two_readers_wake_from_one_notification(notifying: Client):
    async with new_worker(notifying, Waits, Reads) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        ids = [new_workflow_id(), new_workflow_id()]
        readers = [
            await notifying.start_workflow(
                Reads.run, args=[owner, 1], id=i, task_queue=worker.task_queue
            )
            for i in ids
        ]
        for i in ids:
            await _sleeping(notifying, i, owner)
        handle = get_stream_handle(notifying, owner, topic=TOKENS)
        client_records = handle.read()
        client_first = asyncio.ensure_future(client_records.__anext__())
        await asyncio.sleep(3)
        await handle.producer(producer_id="p", attempt=1).append("all")
        for reader in readers:
            assert await asyncio.wait_for(reader.result(), 20) == ["all"]
        assert (await asyncio.wait_for(client_first, 10)).value == "all"
        await client_records.aclose()
        notifier = await describe_notifier(notifying, owner)
        assert notifier is not None and notifier.counter > 0
        await notifying.get_workflow_handle(owner).signal(Waits.finish)


@workflow.defn
class PublishesAndCloses:
    def __init__(self) -> None:
        self.how: str | None = None

    @workflow.run
    async def run(self) -> None:
        writer = workflow_writer(TOKENS)
        writer.publish("a")
        writer.publish("b")
        await workflow.wait_condition(lambda: self.how is not None)
        if self.how == "error":
            await close_workflow_stream(
                TOKENS, error=ApplicationError("the agent gave up", type="GaveUp")
            )
        else:
            await close_workflow_stream(TOKENS, result="done")
        await workflow.wait_condition(lambda: self.how == "end")

    @workflow.signal
    def end(self, how: str) -> None:
        self.how = how


@pytest.mark.parametrize("how", ["result", "error"])
async def test_live_a_workflow_close_ends_both_readers_with_its_outcome(
    notifying: Client, how: str
):
    async with new_worker(notifying, PublishesAndCloses, Reads) as worker:
        owner_id = new_workflow_id()
        owner = await notifying.start_workflow(
            PublishesAndCloses.run, id=owner_id, task_queue=worker.task_queue
        )
        reader_id = new_workflow_id()
        reader = await notifying.start_workflow(
            Reads.run, args=[owner_id, 0], id=reader_id, task_queue=worker.task_queue
        )
        await _sleeping(notifying, reader_id, owner_id)
        client_read = asyncio.ensure_future(
            _read_to_the_end(get_stream_handle(notifying, owner_id, topic=TOKENS))
        )
        await owner.signal(PublishesAndCloses.end, how)
        records, ended_with = await asyncio.wait_for(client_read, 30)
        assert [r.kind for r in records] == [
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.FINISH,
        ]
        if how == "error":
            assert isinstance(ended_with, ApplicationError)
            assert ended_with.type == "GaveUp"
        else:
            assert ended_with is None
        if how == "error":
            with pytest.raises(WorkflowFailureError) as raised:
                await asyncio.wait_for(reader.result(), 30)
            cause = raised.value.cause
            assert isinstance(cause, ApplicationError) and cause.type == "GaveUp"
        else:
            assert await asyncio.wait_for(reader.result(), 30) == ["a", "b", "FINISH"]
        await owner.signal(PublishesAndCloses.end, "end")
        await owner.result()


async def _read_to_the_end(handle: Any) -> tuple[list[Any], BaseException | None]:
    """The records of a client read, and the error it ended with, if any."""
    records: list[Any] = []
    try:
        async for record in handle.read():
            records.append(record)
    except ApplicationError as error:
        return records, error
    return records, None


@workflow.defn
class ReadsAcrossContinueAsNew:
    @workflow.run
    async def run(
        self, owner: str, seen: list[str], after: StreamPosition | None
    ) -> list[str]:
        reader = workflow_reader(owner, topic=TOKENS, after=after or StreamPosition(""))
        async with reader:
            batch = await reader.__anext__()
            seen = seen + [str(r.value) for r in batch.records]
            position = batch.position
        if len(seen) >= 2:
            return seen
        workflow.continue_as_new(args=[owner, seen, position])


async def test_live_a_position_resumes_the_reader_after_continue_as_new(
    notifying: Client,
):
    async with new_worker(notifying, Waits, ReadsAcrossContinueAsNew) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        producer = get_stream_handle(notifying, owner, topic=TOKENS).producer(
            producer_id="p", attempt=1
        )
        await producer.append("one")
        reader = await notifying.start_workflow(
            ReadsAcrossContinueAsNew.run,
            args=[owner, [], None],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await asyncio.sleep(4)
        await producer.append("two")
        assert await asyncio.wait_for(reader.result(), 30) == ["one", "two"]
        await notifying.get_workflow_handle(owner).signal(Waits.finish)


@pytest.mark.parametrize("kind", ["redis", "postgres"])
async def test_live_a_recheck_finds_a_record_whose_notification_was_lost(
    client: Client, kind: str
):
    # The producer's process never notifies, so only the notifier's recheck
    # (streamnotifier.recheckInitial, a few seconds in the lane) wakes the reader.
    reading, silent = await two_processes(client, kind)
    async with new_worker(reading, Waits, Reads) as worker:
        owner = await running_owner(reading, worker.task_queue)
        reader_id = new_workflow_id()
        reader = await reading.start_workflow(
            Reads.run, args=[owner, 1], id=reader_id, task_queue=worker.task_queue
        )
        await _sleeping(reading, reader_id, owner)
        await (
            get_stream_handle(silent, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("found")
        )
        assert await asyncio.wait_for(reader.result(), 30) == ["found"]
        await reading.get_workflow_handle(owner).signal(Waits.finish)


# Positions-only Workflow readers.


@workflow.defn
class ReadsFromTheStore:
    """Reads with replay_from_store until the stream ends, then says what it saw."""

    @workflow.run
    async def run(self, owner: str) -> list[str]:
        seen: list[str] = []
        async with workflow_reader(
            owner, topic=TOKENS, replay_from_store=True
        ) as reader:
            async for batch in reader:
                seen.extend(
                    str(r.value) if r.kind is RecordKind.DATA else r.kind.name
                    for r in batch.records
                )
        return seen


def _positions_payloads(history: Any) -> list[Any]:
    out = []
    for event in history.events:
        if not event.HasField("marker_recorded_event_attributes"):
            continue
        for payloads in event.marker_recorded_event_attributes.details.values():
            for payload in payloads.payloads:
                if payload.metadata.get("messageType") == POSITIONS_TYPE:
                    out.append(payload)
    return out


async def _read_from_the_store(client: Client) -> tuple[Any, str, Any]:
    """Run a positions-only reader over an owner that publishes and closes."""
    # The owner runs on a Worker with a cache. With none, an eviction can
    # cancel its close's Local Activity, and the task's rerun shows readers a
    # SUPERSEDED before the second FINISH.
    async with (
        new_worker(client, PublishesAndCloses) as owners,
        new_worker(client, ReadsFromTheStore, max_cached_workflows=0) as worker,
    ):
        owner_id = new_workflow_id()
        owner = await client.start_workflow(
            PublishesAndCloses.run, id=owner_id, task_queue=owners.task_queue
        )
        reader = await client.start_workflow(
            ReadsFromTheStore.run,
            owner_id,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await asyncio.sleep(3)
        await owner.signal(PublishesAndCloses.end, "result")
        # With no cache, every task replays the reads before it, through Core.
        assert await asyncio.wait_for(reader.result(), 30) == ["a", "b", "FINISH"]
        await owner.signal(PublishesAndCloses.end, "end")
        await owner.result()
    return await reader.fetch_history(), owner_id, reader


async def test_live_a_positions_only_reader_records_ranges_and_replays_from_the_store(
    notifying: Client,
):
    history, _, _ = await _read_from_the_store(notifying)
    positions = _positions_payloads(history)
    assert positions and all(p.ByteSize() < 2000 for p in positions)
    store = store_for(notifying)
    store._batches.clear()
    await Replayer(workflows=[ReadsFromTheStore], plugins=[store]).replay_workflow(
        history
    )


async def test_live_a_replay_that_finds_other_records_is_nondeterminism(
    notifying: Client,
):
    history, _, _ = await _read_from_the_store(notifying)
    store = store_for(notifying)
    store._batches.clear()
    for payload in _positions_payloads(history):
        parts = Payloads.FromString(payload.data)
        range = ReadRangeRequest.FromString(parts.payloads[0].data)
        if range.stored_records:
            range.records_digest = bytes(32)
            parts.payloads[0].data = range.SerializeToString()
            payload.data = parts.SerializeToString()
    # The task fails with the nondeterminism cause. The Replayer reports a
    # failure that lang found as a failed activation, not as Core's
    # NondeterminismError.
    with pytest.raises(Exception) as raised:
        await Replayer(workflows=[ReadsFromTheStore], plugins=[store]).replay_workflow(
            history
        )
    assert "replayed other records" in str(raised.value)
    assert "NonDeterministicError" in str(raised.value)


async def test_live_a_replay_of_a_lost_range_fails_the_task(notifying: Client):
    history, owner_id, _ = await _read_from_the_store(notifying)
    store = store_for(notifying)
    store._batches.clear()
    await store.delete_workflow_streams(notifying.namespace, owner_id)
    with pytest.raises(Exception) as raised:
        await Replayer(workflows=[ReadsFromTheStore], plugins=[store]).replay_workflow(
            history
        )
    # A lost range fails the task for a retry, not as nondeterminism.
    assert "StreamReplayRangeLostError" in str(raised.value)
    assert "NonDeterministicError" not in str(raised.value)


@workflow.defn
class ReadsThroughATimeout:
    """Reads one batch, times out a read on the idle stream, then reads again."""

    def __init__(self) -> None:
        self.seen: list[str] = []

    @workflow.run
    async def run(self, owner: str) -> list[str]:
        reader = workflow_reader(owner, topic=TOKENS, idle_wait=timedelta(seconds=5))
        async with reader:
            self.seen.extend(str(r.value) for r in (await anext(reader)).records)
            try:
                await asyncio.wait_for(anext(reader), 1)
            except (asyncio.TimeoutError, ActivityError):
                self.seen.append("timed out")
            self.seen.extend(str(r.value) for r in (await anext(reader)).records)
        return self.seen

    @workflow.query
    def progress(self) -> list[str]:
        return self.seen


async def test_live_a_read_after_a_timed_out_read_replays_from_the_store(
    notifying: Client,
):
    async with new_worker(notifying, Waits, ReadsThroughATimeout) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        stream = get_stream_handle(notifying, owner, topic=TOKENS)
        await stream.producer(producer_id="p", attempt=1).append("a")
        reader = await notifying.start_workflow(
            ReadsThroughATimeout.run,
            owner,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )

        async def timed_out() -> bool:
            return "timed out" in await reader.query(ReadsThroughATimeout.progress)

        await until(timed_out)
        await stream.producer(producer_id="p", attempt=2).append("b")
        assert await asyncio.wait_for(reader.result(), 30) == ["a", "timed out", "b"]
        await notifying.get_workflow_handle(owner).signal(Waits.finish)
    # A Worker that never ran the reader replays it with no batch cached and
    # no answer state kept.
    store = store_for(notifying)
    store._batches.clear()
    store._read_states = ReadStates()
    await Replayer(workflows=[ReadsThroughATimeout], plugins=[store]).replay_workflow(
        await reader.fetch_history()
    )
