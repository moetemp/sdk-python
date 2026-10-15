"""A Nexus operation hands its caller a stream that a standalone Activity owns.

These run on the Bridge live lane's server, which has the stream notifier,
Nexus operation progress, standalone Activities and the namespace
``streams-b``. A server without them skips the tests, and the lane's gate
turns that skip into a failure. Each case runs on memory, and again on Redis
when ``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.contrib.streams.nexus import (
    ClientTransport,
    FollowBatch,
    FollowOutcome,
    ReadInput,
    StreamAccess,
    StreamDirectReads,
    StreamFollowerService,
    follow_stream,
    stream_ref_from_token,
)
from temporalio.contrib.streams.nexus._direct import StreamServiceCallError
from temporalio.exceptions import ApplicationError, NexusOperationError
from temporalio.service import RPCError
from temporalio.worker import Worker
from tests.contrib.streams.samples import nexus_tool_call_stream as demo
from tests.contrib.streams.samples.scenarios._nexus import NAMESPACE_B, has_namespace
from tests.contrib.streams.test_live_notifications import skip_without_notifier
from tests.contrib.streams.test_nexus_scenario_demos import (  # noqa: F401
    new_store as new_store,
)
from tests.helpers.nexus import end_standalone_operation

# The lane checks owners every 2 seconds and debounces each stream's
# deliveries, so a close by the owner check lands in a few seconds.
OWNER_CHECK = 30.0


@pytest.fixture
async def lane(client: Client) -> Client:
    """``client``, once the server shows the stream notifier and ``streams-b``."""
    await skip_without_notifier(client)
    if not await has_namespace(client, NAMESPACE_B):
        pytest.skip(f"no stream notifier lane: the server has no {NAMESPACE_B!r}")
    return client


def _ids(name: str) -> tuple[str, str]:
    suffix = uuid.uuid4().hex
    return f"{name}-{suffix}", f"{name}-wf-{suffix}"


# Steps 1 to 5: each reader gets the retry as a supersession and the result.


@pytest.mark.parametrize("transport", ["client", "http", "recorded"])
async def test_live_a_caller_reads_a_tool_calls_activity_stream(
    lane: Client,
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
    transport: Any,
):
    task_queue, workflow_id = _ids(f"saa-{transport}")
    output = await demo.main(
        lane,
        task_queue,
        new_store=new_store,
        transport=transport,
        workflow_id=workflow_id,
    )
    assert output.records == ["a1", "a2", "a3", "b1", "b2"]
    assert output.supersessions == ["1 -> 2"]
    assert output.result == "done"


# Step 6: an idle reader wakes once per burst of the tool, not per record.


class _CountingTransport(ClientTransport):
    """Counts the reads that carry a wake token."""

    woken: int = 0

    async def read(self, request: ReadInput) -> Any:
        if request.wake_token:
            type(self).woken += 1
        return await super().read(request)


async def test_live_a_reader_wakes_at_most_once_per_burst_of_a_tool(
    lane: Client,
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
):
    task_queue, workflow_id = _ids("saa-wake")
    the_lane = await demo.lane(lane, task_queue, new_store)
    _CountingTransport.woken = 0
    caller = await Client.connect(
        lane.service_client.config.target_host,
        namespace=lane.namespace,
        plugins=[
            StreamDirectReads(
                {the_lane.streams: _CountingTransport(lane, the_lane.streams)}
            )
        ],
    )
    tools, streams, tool = demo.owner_workers(the_lane)
    async with tools, streams, tool:
        async with Worker(caller, task_queue=task_queue, workflows=[demo.CallsTool]):
            output = await caller.execute_workflow(
                demo.CallsTool.run,
                args=[the_lane.tools, the_lane.streams, "hello", False],
                id=workflow_id,
                task_queue=task_queue,
            )
    assert output.records == ["a1", "a2", "a3", "b1", "b2"]
    # The tool writes in three bursts, the first attempt, the retry and the
    # close, so the reader wakes at most three times for five records.
    assert 1 <= _CountingTransport.woken <= 3


# Step 7: a Worker follower hands over the records and the close.


class _Follower:
    def __init__(self) -> None:
        self.seen: dict[str, str] = {}
        self.supersessions: set[tuple[int, int]] = set()
        self.outcomes: list[FollowOutcome] = []
        self.closed = asyncio.Event()

    async def on_records(self, batch: FollowBatch[str]) -> None:
        # A batch can repeat, so each supersession counts once.
        self.supersessions.update(
            (mark.supersession.previous_attempt, mark.supersession.attempt)
            for mark in batch.supersessions
        )
        for record in batch.records:
            self.seen.setdefault(record.cursor.token, record.value or "")

    async def on_close(self, outcome: FollowOutcome) -> None:
        self.outcomes.append(outcome)
        self.closed.set()


async def _follow(
    lane: Client, make_store: Callable[[], StreamStorePlugin], call: str
) -> _Follower:
    task_queue, _ = _ids(f"saa-follow-{call}")
    the_lane = await demo.lane(lane, task_queue, make_store)
    follower = _Follower()
    tools, streams, tool = demo.owner_workers(the_lane)
    service = StreamFollowerService(
        {"tools": follower},
        client=lane,
        transport=ClientTransport(lane, the_lane.streams),
    )
    async with tools, streams, tool:
        async with Worker(
            lane, task_queue=task_queue, nexus_service_handlers=[service]
        ):
            handle = await follow_stream(
                lane,
                endpoint=the_lane.tools,
                service=demo.Tools,
                operation=demo.Tools.call_tool,
                arg=call,
                follower="tools",
                task_queue=task_queue,
                schedule_to_close_timeout=timedelta(minutes=2),
            )
            try:
                await asyncio.wait_for(follower.closed.wait(), OWNER_CHECK + 30)
            finally:
                await end_standalone_operation(lane, handle, follower.closed.is_set)
    return follower


async def test_live_a_worker_follows_a_tool_calls_activity_stream(
    lane: Client,
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
):
    follower = await _follow(lane, new_store, "hello")
    assert list(follower.seen.values()) == ["a1", "a2", "a3", "b1", "b2"]
    assert len(follower.supersessions) == 1
    [outcome] = follower.outcomes
    assert outcome.result == "done"
    assert outcome.stream is not None and outcome.stream.kind == "activity"


# Step 8: a tool that fails with no close fails the operation.


async def test_live_a_tool_that_fails_with_no_close_fails_the_operation(
    lane: Client,
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
):
    task_queue, workflow_id = _ids("saa-boom")
    with pytest.raises(WorkflowFailureError) as raised:
        await asyncio.wait_for(
            demo.main(
                lane,
                task_queue,
                new_store=new_store,
                call="boom",
                workflow_id=workflow_id,
            ),
            OWNER_CHECK + 30,
        )
    cause: BaseException | None = raised.value.cause
    while cause is not None and not (
        isinstance(cause, ApplicationError) and cause.type == "Boom"
    ):
        assert isinstance(cause, (NexusOperationError, ApplicationError))
        cause = cause.cause
    assert cause is not None, raised.value
    follower = await _follow(lane, new_store, "boom")
    [outcome] = follower.outcomes
    assert outcome.error is not None and "boom" in str(outcome.error)


# Step 9: an authorizer that takes only Activity streams from one namespace.


async def test_live_an_authorizer_takes_activity_reads_from_one_caller_namespace(
    lane: Client,
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
):
    task_queue, workflow_id = _ids("saa-authorize")
    the_lane = await demo.lane(lane, task_queue, new_store)
    seen: list[tuple[str, str | None]] = []

    async def authorize(access: StreamAccess) -> bool:
        seen.append((access.stream.kind, access.caller_namespace))
        return (
            access.stream.kind == "activity"
            and access.caller_namespace == lane.namespace
        )

    caller = await Client.connect(
        lane.service_client.config.target_host,
        namespace=lane.namespace,
        plugins=[demo.direct_reads(the_lane, "client")],
    )
    tools, streams, tool = demo.owner_workers(the_lane, authorize=authorize)
    async with tools, streams, tool:
        async with Worker(caller, task_queue=task_queue, workflows=[demo.CallsTool]):
            handle = await caller.start_workflow(
                demo.CallsTool.run,
                args=[the_lane.tools, the_lane.streams, "hello", False],
                id=workflow_id,
                task_queue=task_queue,
            )
            output = await handle.result()
        assert output.records == ["a1", "a2", "a3", "b1", "b2"]
        # The same read from the owner's namespace is refused.
        description = await caller.get_workflow_handle(workflow_id).fetch_history()
        token = _operation_token(description)
        ref = stream_ref_from_token(token)
        with pytest.raises((StreamServiceCallError, RPCError)) as refused:
            await ClientTransport(the_lane.owner, the_lane.streams).read(
                ReadInput(stream=ref, wait_ms=0)
            )
    assert "may not read" in str(refused.value)
    assert (ref.kind, ref.activity_id is not None) == ("activity", True)
    assert ("activity", lane.namespace) in seen
    assert ("activity", NAMESPACE_B) in seen


def _operation_token(history: Any) -> str:
    """The operation token the caller's History holds for its tool call."""
    for event in history.events:
        attributes = event.nexus_operation_started_event_attributes
        if attributes.operation_token:
            return attributes.operation_token
    raise AssertionError("the History has no started Nexus operation")
