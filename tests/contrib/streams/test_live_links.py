"""Link records and followers, against a server with the stream notifier.

An agent Workflow writes on its own stream and links a tool call that
streams on its own and a long-lived child agent. A client follower and a
Workflow follower read all of it in one order. The Workflow follower sleeps
while every source is idle and wakes through the notifier. The stock dev
server has no stream notifier, so every case here skips there.
``scripts/streams_live_lane.sh`` runs them on a server that has it, on
memory, standalone Redis and a Redis Cluster, each with a ``StreamDriver``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    FollowedItem,
    FollowPosition,
    LinkStateKind,
    RecordKind,
    StreamDriver,
    StreamRef,
    StreamStorePlugin,
    activity_handle,
    get_stream_handle,
    start_activity_with_stream,
    topic,
    workflow_follower,
    workflow_writer,
)
from temporalio.contrib.streams import _follow as follow_module
from temporalio.contrib.streams._record import Cursor
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.exceptions import ApplicationError
from tests.contrib.streams._support import connect_with, new_workflow_id
from tests.contrib.streams.test_live_notifications import skip_without_notifier
from tests.helpers import new_worker

EVENTS = topic("events", str)
TOKENS = topic("tokens", str)
# A link to a Workflow that never starts is unavailable after this grace.
GRACE = timedelta(seconds=3)


def _store(kind: str) -> StreamStorePlugin:
    if kind == "memory":
        return MemoryStreams(owner_start_grace=GRACE)
    env = "STREAMS_REDIS_CLUSTER_URL" if kind == "cluster" else "STREAMS_REDIS_URL"
    url = os.environ.get(env, "")
    if not url:
        pytest.skip(f"set {env} to run on Redis")
    return RedisStreams(
        url,
        cluster=kind == "cluster",
        key_prefix=f"live-{uuid.uuid4().hex}",
        owner_start_grace=GRACE,
    )


@pytest.fixture(params=["memory", "redis", "cluster"])
async def linked(
    request: pytest.FixtureRequest, client: Client
) -> AsyncIterator[Client]:
    """A client whose store keeps a notification log, with a StreamDriver running."""
    await skip_without_notifier(client)
    streams = await connect_with(
        client, _store(request.param).notify_on_append(notification_log=True)
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


def describe(item: FollowedItem[Any]) -> str:
    """One item as ``<source>:<what>``."""
    if item.source.activity_id:
        source = item.source.activity_id
    else:
        source = "agent" if item.depth == 0 else "child"
    if item.link_state is not None:
        state = item.link_state
        what = f"{state.kind.name} {state.link.name}"
        if state.reason:
            what += f" {state.reason}"
        return f"{source}:{what}"
    record = item.record
    assert record is not None
    if record.kind is RecordKind.LINK:
        assert record.link is not None
        return f"{source}:LINK {record.link.event.name} {record.link.name}"
    if record.kind is RecordKind.DATA:
        return f"{source}:{record.value}"
    return f"{source}:{record.kind.name}"


async def follow_all(
    client: Client, workflow_id: str, after: Any = None, timeout: float = 60
) -> list[FollowedItem[Any]]:
    handle = get_stream_handle(client, workflow_id, topic=EVENTS)
    items: list[FollowedItem[Any]] = []

    async def pull() -> None:
        kwargs = {} if after is None else {"after": after}
        async for item in handle.follow(**kwargs):
            items.append(item)

    await asyncio.wait_for(pull(), timeout)
    return items


@workflow.defn
class Follows:
    """Follows an agent's stream to its end and says what it saw."""

    @workflow.run
    async def run(self, agent_id: str) -> list[str]:
        seen: list[str] = []
        async with workflow_follower(agent_id, topic=EVENTS) as follower:
            async for batch in follower:
                seen.extend(describe(i) for i in batch.items)
        return seen


# Steps 1 and 2: the gates hold for a client follower and a Workflow follower.


@activity.defn
async def streams_tokens(tokens: list[str]) -> str:
    out = activity_handle().producer(topic=TOKENS)
    for token in tokens:
        await out.append(token)
        await asyncio.sleep(0.2)
    return "ok"


@workflow.defn
class ChildAgent:
    """A long-lived child that answers one turn per Signal."""

    def __init__(self) -> None:
        self.turns = 0

    @workflow.run
    async def run(self, turns: int) -> None:
        events = workflow_writer(EVENTS)
        events.publish("c0")
        for turn in range(1, turns + 1):
            await workflow.wait_condition(lambda: self.turns >= turn)
            events.publish(f"c{2 * turn - 1}")
            events.publish(f"c{2 * turn}")

    @workflow.signal
    def turn(self) -> None:
        self.turns += 1


@workflow.defn
class Agent:
    def __init__(self) -> None:
        self.until: str | None = None

    @workflow.run
    async def run(self, child_id: str, after: str) -> None:
        events = workflow_writer(EVENTS)
        events.publish("d1")
        tool, tool_ref = start_activity_with_stream(
            streams_tokens,
            ["a1", "a2", "a3"],
            activity_id="tool-a",
            start_to_close_timeout=timedelta(seconds=30),
            topic=TOKENS,
        )
        events.link(tool_ref, name="tool-a")
        events.link(
            StreamRef.for_workflow(child_id, topic=EVENTS),
            name="child",
            after=Cursor(after),
        )
        events.publish("d2")
        await tool
        events.join(name="tool-a")
        events.publish("d3")
        await workflow.wait_condition(lambda: self.until is not None)
        assert self.until is not None
        events.join(name="child", until=Cursor(self.until))
        events.publish("d4")

    @workflow.signal
    def child_turn_ended(self, cursor: str) -> None:
        self.until = cursor


def check_gates(seen: list[str]) -> None:
    at = {s: i for i, s in enumerate(seen)}
    for token in ("a1", "a2", "a3"):
        assert seen.count(f"tool-a:{token}") == 1
        assert at["agent:LINK OPEN tool-a"] < at[f"tool-a:{token}"]
    for record in ("c1", "c2"):
        assert seen.count(f"child:{record}") == 1
        assert at["agent:LINK OPEN child"] < at[f"child:{record}"]
    # The link opened strictly after c0.
    assert "child:c0" not in seen
    assert at["tool-a:DONE tool-a"] < at["agent:d3"]
    assert at["child:c2"] < at["agent:d4"]
    assert [s for s in seen if s.startswith("agent:d")] == [
        "agent:d1",
        "agent:d2",
        "agent:d3",
        "agent:d4",
    ]


async def test_live_both_followers_keep_the_gates_of_an_agent(linked: Client):
    async with new_worker(
        linked, Agent, ChildAgent, Follows, activities=[streams_tokens]
    ) as worker:
        child_id = new_workflow_id()
        child = await linked.start_workflow(
            ChildAgent.run, 2, id=child_id, task_queue=worker.task_queue
        )
        after = await _cursor_of_first(linked, child_id, "c0")
        agent = await linked.start_workflow(
            Agent.run,
            args=[child_id, after],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        follower = await linked.start_workflow(
            Follows.run, agent.id, id=new_workflow_id(), task_queue=worker.task_queue
        )
        client_items = asyncio.ensure_future(follow_all(linked, agent.id, timeout=90))
        await child.signal(ChildAgent.turn)
        until = await _cursor_of_first(linked, child_id, "c2")
        await agent.signal(Agent.child_turn_ended, until)
        await asyncio.wait_for(agent.result(), 60)
        seen = [describe(i) for i in await client_items]
        check_gates(seen)
        check_gates(await asyncio.wait_for(follower.result(), 60))
        await child.signal(ChildAgent.turn)
        await asyncio.wait_for(child.result(), 30)


async def _cursor_of_first(client: Client, workflow_id: str, value: str) -> str:
    handle = get_stream_handle(client, workflow_id, topic=EVENTS)
    records = handle.read()
    try:
        async for record in records:
            if record.value == value:
                return record.cursor.token
    finally:
        await records.aclose()
    raise AssertionError(f"{value} not found")


# Steps 3, 4 and 5: a failed tool, a missing child and a cycle never fail the
# follow.


@activity.defn
async def fails_with_boom() -> None:
    await activity_handle().producer(topic=TOKENS).append("x1")
    raise ApplicationError("boom", type="Boom", non_retryable=True)


@workflow.defn
class LinksTheHardCases:
    @workflow.run
    async def run(self, missing_id: str) -> None:
        events = workflow_writer(EVENTS)
        tool, tool_ref = start_activity_with_stream(
            fails_with_boom,
            activity_id="tool-boom",
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
            topic=TOKENS,
        )
        events.link(tool_ref, name="tool-boom")
        events.link(StreamRef.for_workflow(missing_id, topic=EVENTS), name="missing")
        cycle_id = f"{workflow.info().workflow_id}-cycle"
        await workflow.execute_child_workflow(
            LinksBack.run, workflow.info().workflow_id, id=cycle_id
        )
        events.link(StreamRef.for_workflow(cycle_id, topic=EVENTS), name="cycle")
        with contextlib.suppress(Exception):
            await tool
        events.publish("after")


@workflow.defn
class LinksBack:
    @workflow.run
    async def run(self, parent_id: str) -> None:
        events = workflow_writer(EVENTS)
        events.link(StreamRef.for_workflow(parent_id, topic=EVENTS), name="parent")
        events.publish("cycled")


async def test_live_a_failed_tool_a_missing_child_and_a_cycle_go_on(linked: Client):
    async with new_worker(
        linked, LinksTheHardCases, LinksBack, activities=[fails_with_boom]
    ) as worker:
        agent = await linked.start_workflow(
            LinksTheHardCases.run,
            f"missing-{uuid.uuid4().hex}",
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await asyncio.wait_for(agent.result(), 60)
        items = await follow_all(linked, agent.id, timeout=60)
    states = {
        i.link_state.link.name: i.link_state
        for i in items
        if i.link_state is not None and i.link_state.kind is not LinkStateKind.OPENED
    }
    boom = states["tool-boom"]
    assert boom.kind is LinkStateKind.DONE
    error: BaseException | None = boom.error
    while error is not None and not isinstance(error, ApplicationError):
        error = error.__cause__
    assert isinstance(error, ApplicationError) and error.type == "Boom"
    assert states["missing"].kind is LinkStateKind.UNAVAILABLE
    # The child's link back to the agent is not opened again.
    assert states["parent"].kind is LinkStateKind.SKIPPED
    assert states["parent"].reason == "cycle"
    seen = [describe(i) for i in items]
    assert "tool-boom:x1" in seen and "child:cycled" in seen
    # No JOIN holds the agent, so its last record need not wait for the children.
    assert "agent:after" in seen


# Step 6: a position saved inside the tool's segment resumes the rest of it once.


async def test_live_a_follower_resumes_inside_a_segment(
    linked: Client, monkeypatch: pytest.MonkeyPatch
):
    async with new_worker(
        linked, Agent, ChildAgent, activities=[streams_tokens]
    ) as worker:
        child_id = new_workflow_id()
        child = await linked.start_workflow(
            ChildAgent.run, 1, id=child_id, task_queue=worker.task_queue
        )
        after = await _cursor_of_first(linked, child_id, "c0")
        agent = await linked.start_workflow(
            Agent.run,
            args=[child_id, after],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await child.signal(ChildAgent.turn)
        until = await _cursor_of_first(linked, child_id, "c2")
        await agent.signal(Agent.child_turn_ended, until)
        await asyncio.wait_for(agent.result(), 60)
        monkeypatch.setattr(follow_module, "_MAX_RECORDS", 2)
        items = await follow_all(linked, agent.id)
        seen = [describe(i) for i in items]
        inside = [
            (n, i.position)
            for n, i in enumerate(items)
            if i.position is not None
            and "agent:LINK OPEN tool-a" in seen[: n + 1]
            and "tool-a:DONE tool-a" not in seen[: n + 1]
        ]
        assert inside, seen
        count, position = inside[0]
        assert isinstance(position, FollowPosition)
        rest = [describe(i) for i in await follow_all(linked, agent.id, after=position)]
        assert seen[: count + 1] + rest == seen
        tool_records = [s for s in seen[: count + 1] + rest if s.startswith("tool-a:a")]
        assert tool_records == ["tool-a:a1", "tool-a:a2", "tool-a:a3"]
        await asyncio.wait_for(child.result(), 30)
