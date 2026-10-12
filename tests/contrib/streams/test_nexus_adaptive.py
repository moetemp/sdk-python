"""A stream notifies its callers only while a reader sleeps on it.

A reader whose read waits its idle wait and finds nothing arms the stream, and
the stream's next append notifies the callers once. A reader that keeps up
never arms, so a busy stream sends nothing. These live tests count the
notifier's moves with ``DescribeStreamNotifier`` and the progress Workflow
Tasks in the caller's History. They need a server with Nexus progress and the
stream notifier, and skip on any other server.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest

from temporalio import workflow
from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import DescribeStreamNotifierRequest
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.streams import StreamRef, StreamStorePlugin, get_stream_handle
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    ReadOutput,
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from tests.contrib.streams.test_nexus_follower import (
    _closed,
    _LiveFollower,
    _until,
)
from tests.contrib.streams.test_nexus_follower import _lane as _follower_lane
from tests.contrib.streams.test_nexus_reader import _skip_without_notifier, connect
from tests.helpers.nexus import make_nexus_endpoint_name

converter = DataConverter.default.payload_converter


@nexusrpc.service(name="AdaptiveChat")
class AdaptiveChat:
    chat: nexusrpc.Operation[str, str]


@dataclass
class AdaptiveSeen:
    items: list[str]
    result: str


@workflow.defn(name="AdaptiveReader")
class AdaptiveReader:
    """Reads the stream with the default idle wait, and counts what it read."""

    def __init__(self) -> None:
        self._count = 0

    @workflow.run
    async def run(self, endpoint: str) -> AdaptiveSeen:
        client = workflow.create_nexus_client(service=AdaptiveChat, endpoint=endpoint)
        handle = await client.start_operation(AdaptiveChat.chat, "hello")
        reader = StreamReader(handle, item_type=str, endpoint=endpoint)
        items: list[str] = []
        while (batch := await reader.next()) is not None:
            items += batch
            self._count = len(items)
        return AdaptiveSeen(items=items, result=await handle)

    @workflow.query
    def count(self) -> int:
        return self._count


@workflow.defn(name="AdaptiveOwner")
class AdaptiveOwner:
    """Owns a stream that the test appends to, until told to end."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


async def _counter(client: Client, ref: StreamRef) -> int:
    """The highest counter the stream's notifier took, zero before it exists."""
    run_id = (await client.get_workflow_handle(ref.workflow_id).describe()).run_id
    try:
        described = await client.workflow_service.describe_stream_notifier(
            DescribeStreamNotifierRequest(
                namespace=client.namespace,
                stream_ref=StreamReference(
                    owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                    workflow_id=ref.workflow_id,
                    run_id=run_id,
                    topic=ref.topic,
                ),
            )
        )
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            return 0
        raise
    return described.counter


async def _eventually(check: Callable[[], Awaitable[bool]], what: str) -> None:
    for _ in range(300):
        if await check():
            return
        await asyncio.sleep(0.1)
    pytest.fail(f"timed out waiting until {what}")


@dataclass
class _Lane:
    client: Client
    store: StreamStorePlugin
    task_queue: str
    endpoint: str
    ref: StreamRef

    @contextlib.asynccontextmanager
    async def worker(self) -> AsyncIterator[None]:
        ref = self.ref

        async def open_stream(_ctx: Any, _prompt: str) -> StreamRef:
            # The test starts the owner with own(), since an append needs it.
            return ref

        @nexusrpc.handler.service_handler(service=AdaptiveChat)
        class Chat:
            @nexusrpc.handler.operation_handler
            def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
                return StreamOperationHandler(open_stream)

        async with Worker(
            self.client,
            task_queue=self.task_queue,
            workflows=[AdaptiveReader, AdaptiveOwner],
            nexus_service_handlers=[Chat(), TemporalStreamsHandler()],
        ):
            await self.client.start_workflow(
                AdaptiveOwner.run, id=self.ref.workflow_id, task_queue=self.task_queue
            )
            yield

    async def read(self) -> WorkflowHandle[Any, AdaptiveSeen]:
        """Start a reader, and wait until its operation started."""
        caller = await self.client.start_workflow(
            AdaptiveReader.run,
            self.endpoint,
            id=f"adaptive-reader-{uuid.uuid4()}",
            task_queue=self.task_queue,
        )

        async def started() -> bool:
            history = await caller.fetch_history()
            return any(
                event.HasField("nexus_operation_started_event_attributes")
                for event in history.events
            )

        await _eventually(started, "the stream operation started")
        return caller

    async def sleeping(self, caller: WorkflowHandle[Any, Any]) -> None:
        """Wait until the reader's last read armed and no read is in flight."""

        async def armed() -> bool:
            history = await caller.fetch_history()
            reads = {
                event.event_id
                for event in history.events
                if event.nexus_operation_scheduled_event_attributes.operation == "read"
            }
            answers = [
                event.nexus_operation_completed_event_attributes.result
                for event in history.events
                if event.nexus_operation_completed_event_attributes.scheduled_event_id
                in reads
            ]
            if not answers or len(answers) < len(reads):
                return False
            return bool(converter.from_payload(answers[-1], ReadOutput).armed)

        await _eventually(armed, "the reader armed the stream")

    async def read_count(self, caller: WorkflowHandle[Any, Any], count: int) -> None:
        async def reached() -> bool:
            return await caller.query(AdaptiveReader.count) >= count

        await _eventually(reached, f"the reader read {count} records")

    async def counter(self) -> int:
        return await _counter(self.client, self.ref)

    def producer(self) -> Any:
        return get_stream_handle(self.client, self.ref).producer(
            producer_id="writer", attempt=1
        )

    async def close(self, caller: WorkflowHandle[Any, AdaptiveSeen]) -> AdaptiveSeen:
        await self.store.close_stream(self.client, self.ref, "done")
        seen = await asyncio.wait_for(caller.result(), 60)
        await self.client.get_workflow_handle(self.ref.workflow_id).signal(
            AdaptiveOwner.done
        )
        return seen


def _store(backing: str, *, every_append: bool = False) -> StreamStorePlugin:
    if backing == "memory":
        return MemoryStreams().notify_on_append(every_append=every_append)
    url = os.environ.get("STREAMS_REDIS_URL")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to read over Redis")
    from temporalio.contrib.streams.redis import RedisStreams

    return RedisStreams(
        url, key_prefix=f"adaptive-{uuid.uuid4().hex}"
    ).notify_on_append(every_append=every_append)


async def _lane(
    client: Client,
    env: WorkflowEnvironment,
    backing: str = "memory",
    *,
    every_append: bool = False,
) -> _Lane:
    ref = StreamRef.for_workflow(f"adaptive-owner-{uuid.uuid4()}", topic="tokens")
    await _skip_without_notifier(client, ref)
    task_queue = f"adaptive-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    store = _store(backing, every_append=every_append)
    return _Lane(await connect(client, store), store, task_queue, endpoint, ref)


def _progress_tasks(history: Any) -> int:
    return sum(
        bool(event.workflow_task_scheduled_event_attributes.nexus_operation_progress)
        for event in history.events
    )


async def _append_steadily(lane: _Lane, records: int) -> list[int]:
    """Append ``records`` records, 20 a second, and sample the counter as it goes."""
    producer = lane.producer()
    counters: list[int] = []
    for n in range(records):
        await producer.append(f"r{n}")
        if n % 10 == 9:
            counters.append(await lane.counter())
        await asyncio.sleep(0.05)
    return counters


@pytest.mark.parametrize("backing", ["memory", "redis"])
async def test_live_a_busy_stream_sends_no_notification(
    client: Client, env: WorkflowEnvironment, backing: str
) -> None:
    """A reader that keeps up finds records on each read, so it never arms.
    Only the first append, which found the arm the attach set, notifies."""
    lane = await _lane(client, env, backing)
    async with lane.worker():
        caller = await lane.read()
        counters = await _append_steadily(lane, 200)
        seen = await lane.close(caller)

    assert seen.items == [f"r{n}" for n in range(200)]
    assert len({counter for counter in counters if counter}) <= 1, counters
    assert _progress_tasks(await caller.fetch_history()) <= 1


@pytest.mark.parametrize("backing", ["memory", "redis"])
async def test_live_an_idle_reader_wakes_once_per_burst(
    client: Client, env: WorkflowEnvironment, backing: str
) -> None:
    lane = await _lane(client, env, backing)
    async with lane.worker():
        caller = await lane.read()
        producer = lane.producer()
        moves = []
        for burst in range(3):
            await lane.sleeping(caller)
            before = await lane.counter()
            await producer.append(*(f"b{burst}-{n}" for n in range(5)))
            await lane.read_count(caller, 5 * (burst + 1))
            moved = await lane.counter()
            moves.append(moved > before)
            # The reader arms again after its idle wait, which sends nothing.
            await asyncio.sleep(5)
            assert await lane.counter() == moved
        seen = await lane.close(caller)

    assert moves == [True, True, True]
    assert seen.items == [f"b{burst}-{n}" for burst in range(3) for n in range(5)]
    assert _progress_tasks(await caller.fetch_history()) == 3


async def test_live_a_short_gap_costs_no_notification(
    client: Client, env: WorkflowEnvironment
) -> None:
    """A gap shorter than the reader's 2 second idle wait ends in a read that
    returns the record, so nothing arms and nothing notifies."""
    lane = await _lane(client, env)
    async with lane.worker():
        caller = await lane.read()
        await lane.sleeping(caller)
        producer = lane.producer()
        await producer.append("first")
        await lane.read_count(caller, 1)
        moved = await lane.counter()
        for n in range(8):
            await asyncio.sleep(1)
            await producer.append(f"r{n}")
        await lane.read_count(caller, 9)
        assert await lane.counter() == moved
        seen = await lane.close(caller)

    assert seen.items == ["first", *(f"r{n}" for n in range(8))]
    assert _progress_tasks(await caller.fetch_history()) == 1


async def test_live_two_readers_wake_from_one_notification(
    client: Client, env: WorkflowEnvironment
) -> None:
    lane = await _lane(client, env)
    async with lane.worker():
        callers = [await lane.read(), await lane.read()]
        for caller in callers:
            await lane.sleeping(caller)
        before = await lane.counter()
        await lane.producer().append("hello")
        for caller in callers:
            await lane.read_count(caller, 1)
        assert await lane.counter() > before
        await lane.store.close_stream(lane.client, lane.ref, "done")
        seen = [await asyncio.wait_for(caller.result(), 60) for caller in callers]
        await lane.client.get_workflow_handle(lane.ref.workflow_id).signal(
            AdaptiveOwner.done
        )

    assert [s.items for s in seen] == [["hello"], ["hello"]]
    for caller in callers:
        assert _progress_tasks(await caller.fetch_history()) == 1


async def test_live_a_close_reaches_an_unarmed_caller(
    client: Client, env: WorkflowEnvironment
) -> None:
    """The close is not gated by an arm, so a reader that is busy, and never
    armed, still gets every record and the close result."""
    lane = await _lane(client, env)
    async with lane.worker():
        caller = await lane.read()
        await _append_steadily(lane, 40)
        seen = await lane.close(caller)

    assert seen.items == [f"r{n}" for n in range(40)]
    assert seen.result == "done"


async def test_live_every_append_mode_notifies_each_append(
    client: Client, env: WorkflowEnvironment
) -> None:
    """For readers that can't arm, the store notifies after every append, so the
    counter moves while the stream is busy too."""
    lane = await _lane(client, env, every_append=True)
    async with lane.worker():
        caller = await lane.read()
        counters = await _append_steadily(lane, 60)
        seen = await lane.close(caller)

    assert seen.items == [f"r{n}" for n in range(60)]
    assert len(set(counters)) >= 3, counters


async def test_live_a_follower_arms_and_wakes(
    client: Client, env: WorkflowEnvironment
) -> None:
    """A follower reads its backlog until a read arms the stream, then sleeps,
    and the next append wakes it."""
    lane = await _follower_lane(client, env)
    follower = _LiveFollower(max_records=7)
    producer = get_stream_handle(lane.client, lane.ref).producer(
        producer_id="writer", attempt=1
    )
    async with lane.worker(follower):
        await lane.own()
        await producer.append(*[f"r{n}" for n in range(20)])
        await lane.follow()
        await _until(lambda: len(follower.ordered()) == 20)
        # The read that armed waited a second. Nothing moves while it sleeps.
        await asyncio.sleep(3)
        before = await _counter(lane.client, lane.ref)
        await asyncio.sleep(2)
        assert await _counter(lane.client, lane.ref) == before
        await producer.append("late")
        await _until(lambda: "late" in follower.ordered())
        assert await _counter(lane.client, lane.ref) > before
        await lane.close("done")
        await _closed(follower)

    assert follower.ordered() == [*(f"r{n}" for n in range(20)), "late"]


async def test_live_a_follower_on_an_empty_stream_wakes_on_the_first_append(
    client: Client, env: WorkflowEnvironment
) -> None:
    """The attach arms an empty stream, so the first append reaches a follower
    that has no record to read yet."""
    lane = await _follower_lane(client, env)
    follower = _LiveFollower()
    async with lane.worker(follower):
        await lane.own()
        await lane.follow()
        await asyncio.sleep(2)
        assert follower.ordered() == []
        await (
            get_stream_handle(lane.client, lane.ref)
            .producer(producer_id="writer", attempt=1)
            .append("first")
        )
        await _until(lambda: "first" in follower.ordered())
        await lane.close("done")
        await _closed(follower)

    assert follower.ordered() == ["first"]
    assert follower.outcomes[0].result == "done"
