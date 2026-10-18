"""A caller in another namespace follows an agent and its tool streams over Nexus.

These run on the Bridge live lane's server, which has the stream notifier,
Nexus operation progress and the namespace ``streams-b``. A server without
them skips the tests, and the lane's gate turns that skip into a failure.
Each case runs on memory, and again on Redis when ``STREAMS_REDIS_URL`` is
set.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.contrib.streams.nexus import (
    ClientTransport,
    FollowerBatch,
    FollowOutcome,
    StreamFollowerService,
    StreamReader,
    follow_stream,
)
from temporalio.worker import Worker
from tests.contrib.streams import _agent_links_nexus as demo
from tests.contrib.streams.test_live_activity_stream_nexus import lane as lane
from tests.contrib.streams.test_live_activity_stream_nexus import (  # noqa: F401
    new_store as new_store,
)
from tests.helpers.nexus import end_standalone_operation

EXPECTED = [
    "agent:plan",
    "agent:OPEN",
    "tool-0:OPENED",
    "tool-0:streams",
    "tool-0:results",
    "tool-0:DONE",
    "agent:JOIN",
    "agent:OPEN",
    "tool-1:OPENED",
    "tool-1:links",
    "tool-1:results",
    "tool-1:DONE",
    "agent:JOIN",
    "agent:answer",
]


def _ids(name: str) -> tuple[str, str, str]:
    suffix = uuid.uuid4().hex
    return f"{name}-{suffix}", f"{name}-wf-{suffix}", f"agent-{suffix}"


async def test_live_a_caller_follows_an_agent_and_its_tools_across_namespaces(
    lane: Client,  # noqa: F811
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
):
    task_queue, workflow_id, agent_id = _ids("links")
    output = await demo.main(
        lane,
        task_queue,
        new_store=new_store,
        workflow_id=workflow_id,
        agent_id=agent_id,
    )
    assert output.items == EXPECTED
    assert output.runs == 1


async def test_live_a_caller_resumes_its_follow_in_a_new_run(
    lane: Client,  # noqa: F811
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
):
    task_queue, workflow_id, agent_id = _ids("links-resume")
    output = await demo.main(
        lane,
        task_queue,
        new_store=new_store,
        stop_after=2,
        workflow_id=workflow_id,
        agent_id=agent_id,
    )
    # The position holds every open link, so the new run goes on with no loss
    # and no repeat.
    assert output.items == EXPECTED
    assert output.runs == 2


class _Follower:
    follow_links = True

    def __init__(self) -> None:
        self.items: dict[str, str] = {}
        self.outcomes: list[FollowOutcome] = []
        self.closed = asyncio.Event()

    async def on_records(self, batch: FollowerBatch[str]) -> None:
        for item in batch.items:
            if item.record is not None:
                # A batch can repeat, so each record counts once.
                key = f"{item.source.activity_id}/{item.record.cursor.token}"
                self.items.setdefault(key, demo.describe(item))

    async def on_close(self, outcome: FollowOutcome) -> None:
        self.outcomes.append(outcome)
        self.closed.set()


async def test_live_a_worker_follower_asks_for_the_agents_links(
    lane: Client,  # noqa: F811
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
):
    task_queue, _, agent_id = _ids("links-worker")
    the_lane = await demo.lane(lane, task_queue, new_store)
    follower = _Follower()
    service = StreamFollowerService(
        {"agents": follower},
        client=lane,
        transport=ClientTransport(lane, the_lane.streams),
    )
    agents, streams, agent = demo.owner_workers(the_lane)
    async with agents, streams, agent:
        async with Worker(
            lane, task_queue=task_queue, nexus_service_handlers=[service]
        ):
            handle = await follow_stream(
                lane,
                endpoint=the_lane.agents,
                service=demo.Agents,
                operation=demo.Agents.run_agent,
                arg=agent_id,
                follower="agents",
                task_queue=task_queue,
                schedule_to_close_timeout=timedelta(minutes=2),
            )
            try:
                await asyncio.wait_for(follower.closed.wait(), 90)
            finally:
                await end_standalone_operation(lane, handle, follower.closed.is_set)
    data = [v for v in follower.items.values() if v.split(":")[1].islower()]
    assert data == [
        "agent:plan",
        "tool-0:streams",
        "tool-0:results",
        "tool-1:links",
        "tool-1:results",
        "agent:answer",
    ]
    [outcome] = follower.outcomes
    assert outcome.succeeded


@workflow.defn
class FollowsLive:
    """Follows an agent and answers what it saw so far."""

    def __init__(self) -> None:
        self.items: list[str] = []

    @workflow.run
    async def run(self, agents: str, streams: str, agent_id: str) -> list[str]:
        client = workflow.create_nexus_client(service=demo.Agents, endpoint=agents)
        handle = await client.start_operation(demo.Agents.run_agent, agent_id)
        reader = StreamReader(
            handle, item_type=str, endpoint=streams, follow_links=True
        )
        while (batch := await reader.next_followed()) is not None:
            self.items.extend(demo.describe(item) for item in batch)
        await handle
        return self.items

    @workflow.query
    def seen(self) -> list[str]:
        return self.items


async def test_live_a_caller_sees_a_busy_tool_while_the_agent_is_idle(
    lane: Client,  # noqa: F811
    new_store: Callable[[], StreamStorePlugin],  # noqa: F811
):
    task_queue, workflow_id, agent_id = _ids("links-live")
    agent_id = f"slow-{agent_id}"
    the_lane = await demo.lane(lane, task_queue, new_store)
    caller = await Client.connect(
        lane.service_client.config.target_host,
        namespace=lane.namespace,
        plugins=[demo.direct_reads(the_lane)],
    )
    tokens = [f"tool-0:slow-{step}" for step in range(1, 5)]
    seen_at: dict[str, float] = {}
    agents, streams, agent = demo.owner_workers(the_lane)
    async with agents, streams, agent:
        async with Worker(caller, task_queue=task_queue, workflows=[FollowsLive]):
            handle = await caller.start_workflow(
                FollowsLive.run,
                args=[the_lane.agents, the_lane.streams, agent_id],
                id=workflow_id,
                task_queue=task_queue,
            )
            # The agent links the tool, then appends nothing until the tool
            # returns, while the tool appends a token every 3 seconds.
            deadline = time.monotonic() + 60
            while len(seen_at) < len(tokens) and time.monotonic() < deadline:
                seen = await handle.query(FollowsLive.seen)
                now = time.monotonic()
                for item in seen:
                    if item in tokens and item not in seen_at:
                        seen_at[item] = now
                await asyncio.sleep(0.2)
            items = await asyncio.wait_for(handle.result(), 60)
    appended = demo.appended_at[agent_id]
    lags = {token: seen_at[token] - appended[token] for token in tokens}
    assert all(lag < 5 for lag in lags.values()), lags
    # The agent's next record comes only after the tool.
    assert [i for i in items if i in tokens or i == "agent:answer"] == [
        *tokens,
        "agent:answer",
    ]
