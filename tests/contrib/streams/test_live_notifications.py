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
import os
import time
import uuid
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.enums.v1 import EventType, StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import (
    DescribeStreamNotifierRequest,
    DescribeStreamNotifierResponse,
)
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.streams import (
    RecordKind,
    StreamPosition,
    StreamStorePlugin,
    close_workflow_stream,
    get_stream_handle,
    topic,
    workflow_reader,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Replayer
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
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
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return RedisStreams(url, key_prefix=f"live-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis"])
async def notifying(request: pytest.FixtureRequest, client: Client) -> Client:
    """A client whose store notifies, on a server with the notifier."""
    await skip_without_notifier(client)
    return await connect_with(client, store(request.param).notify_on_append())


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
        # Past the idle wait, the reader armed and waits on the notifier.
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


async def test_live_a_busy_stream_sends_no_notification(notifying: Client):
    async with new_worker(notifying, Waits) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        handle = get_stream_handle(notifying, owner, topic=TOKENS)
        producer = handle.producer(producer_id="p", attempt=1)
        records = handle.read()
        for n in range(5):
            await producer.append(str(n))
            assert (await asyncio.wait_for(records.__anext__(), 5)).value == str(n)
            await asyncio.sleep(0.2)
        await records.aclose()
        notifier = await describe_notifier(notifying, owner)
        assert notifier is None or notifier.counter == 0
        await notifying.get_workflow_handle(owner).signal(Waits.finish)


async def test_live_a_client_reader_finds_a_lost_notification_at_the_poll_limit(
    client: Client,
):
    # The producer's process never notifies, as after a crash between the
    # append and the notification. The reader's read at the poll limit
    # (streamnotifier.pollMaxWait, a few seconds in the lane) finds the record.
    reading, silent = await two_processes(client)
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


async def two_processes(client: Client) -> tuple[Client, Client]:
    """A reader's client that notifies, and a producer's that never does, on one Redis."""
    await skip_without_notifier(client)
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to share a store across two clients")
    prefix = f"live-silent-{uuid.uuid4().hex}"
    reading = await connect_with(
        client, RedisStreams(url, key_prefix=prefix).notify_on_append()
    )
    silent = await connect_with(client, RedisStreams(url, key_prefix=prefix))
    return reading, silent


async def test_live_every_append_mode_notifies_each_append(client: Client):
    await skip_without_notifier(client)
    streams = await connect_with(
        client, MemoryStreams().notify_on_append(every_append=True)
    )
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
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
        await streams.get_workflow_handle(owner).signal(Waits.finish)


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
        # The close wakes the sleeping reader, and its read ends.
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(first, 10)
        notifier = await describe_notifier(notifying, owner)
        assert notifier is not None and notifier.closed
        assert notifier.close_failure.application_failure_info.type == "GaveUp"
        await notifying.get_workflow_handle(owner).signal(Waits.finish)


# Workflow readers.


@workflow.defn
class Reads:
    """Reads an owner's ``tokens`` until it ends, then says what it saw."""

    @workflow.run
    async def run(self, owner: str, stop_after: int) -> list[str]:
        seen: list[str] = []
        async with workflow_reader(owner, topic=TOKENS) as reader:
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
    """Wait until the reader attached and its read armed the stream."""

    async def attached() -> bool:
        notifier = await describe_notifier(client, owner)
        return notifier is not None and any(
            c.workflow_target.workflow_id == reader_id for c in notifier.callbacks
        )

    await until(attached)
    # Past the attach, the next read waits the idle time and arms.
    await asyncio.sleep(3)


async def test_live_a_workflow_reader_sleeps_and_wakes_on_an_append(
    notifying: Client,
):
    async with new_worker(notifying, Waits, Reads) as worker:
        owner = await running_owner(notifying, worker.task_queue)
        reader_id = new_workflow_id()
        reader = await notifying.start_workflow(
            Reads.run, args=[owner, 2], id=reader_id, task_queue=worker.task_queue
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
    await Replayer(workflows=[Reads], plugins=[MemoryStreams()]).replay_workflow(
        await reader.fetch_history()
    )


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
            read_all(get_stream_handle(notifying, owner_id, topic=TOKENS).read(), 30)
        )
        await owner.signal(PublishesAndCloses.end, how)
        records = await client_read
        assert [r.kind for r in records] == [
            RecordKind.DATA,
            RecordKind.DATA,
            RecordKind.FINISH,
        ]
        if how == "error":
            with pytest.raises(WorkflowFailureError) as raised:
                await asyncio.wait_for(reader.result(), 30)
            cause = raised.value.cause
            assert isinstance(cause, ApplicationError) and cause.type == "GaveUp"
        else:
            assert await asyncio.wait_for(reader.result(), 30) == ["a", "b", "FINISH"]
        await owner.signal(PublishesAndCloses.end, "end")
        await owner.result()


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


async def test_live_a_recheck_finds_a_record_whose_notification_was_lost(
    client: Client,
):
    # The producer's process never notifies, so only the notifier's recheck
    # (streamnotifier.recheckInitial, a few seconds in the lane) wakes the reader.
    reading, silent = await two_processes(client)
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
