"""Following a stream and the streams its links open, from a client and from a Workflow.

An agent Workflow publishes on its own stream, links two tool calls that
stream on their own, and links a child Workflow's stream. A follower reads
all of them in one order, which the stream runtime's ``Follow`` keeps by the link gates.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    FollowedItem,
    FollowPosition,
    RecordKind,
    StreamRef,
    activity_handle,
    start_activity_with_stream,
    stream_handle,
    stream_producer,
    topic,
    workflow_follower,
)
from temporalio.contrib.streams import _follow as follow_module
from temporalio.contrib.streams._plugin import store_for_client
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.worker import Replayer
from tests.contrib.streams._support import connect_with, new_workflow_id
from tests.helpers import new_worker

EVENTS = topic("events", str)
TOKENS = topic("tokens", str)


@activity.defn
async def tool(tokens: list[str]) -> str:
    await activity_handle().producer(topic=TOKENS).append(*tokens)
    return "ok"


@workflow.defn
class Child:
    @workflow.run
    async def run(self) -> None:
        events = stream_producer(EVENTS)
        events.publish("c1")
        events.publish("c2")


@workflow.defn
class Agent:
    @workflow.run
    async def run(self) -> None:
        events = stream_producer(EVENTS)
        timeout = timedelta(seconds=10)
        events.publish("d1")
        first, first_ref = start_activity_with_stream(
            tool,
            ["a1", "a2"],
            activity_id="tool-1",
            start_to_close_timeout=timeout,
            topic=TOKENS,
        )
        events.link(first_ref, name="tool-1")
        second, second_ref = start_activity_with_stream(
            tool,
            ["b1"],
            activity_id="tool-2",
            start_to_close_timeout=timeout,
            topic=TOKENS,
        )
        events.link(second_ref, name="tool-2")
        child_id = f"{workflow.info().workflow_id}-child"
        child = await workflow.start_child_workflow(Child.run, id=child_id)
        events.link(StreamRef.for_workflow(child_id, topic=EVENTS), name="sub")
        await first
        await second
        events.join(name="tool-1")
        events.publish("d2")
        await child
        events.join(name="sub")
        events.publish("d3")


def describe(item: FollowedItem[Any]) -> str:
    """One item as ``<source>:<what>``."""
    source = item.source.activity_id or ("agent" if item.depth == 0 else "child")
    if item.link_state is not None:
        return f"{source}:{item.link_state.kind.name} {item.link_state.link.name}"
    record = item.record
    assert record is not None
    if record.kind is RecordKind.LINK:
        assert record.link is not None
        return f"{source}:LINK {record.link.event.name} {record.link.name}"
    if record.kind is RecordKind.DATA:
        return f"{source}:{record.value}"
    return f"{source}:{record.kind.name}"


def check_order(seen: list[str]) -> None:
    """The gates hold, and each record comes once."""
    data = [
        s
        for s in seen
        if ":" in s
        and s.split(":")[1] in {"d1", "d2", "d3", "a1", "a2", "b1", "c1", "c2"}
    ]
    assert sorted(data) == sorted(
        [
            "agent:d1",
            "agent:d2",
            "agent:d3",
            "tool-1:a1",
            "tool-1:a2",
            "tool-2:b1",
            "child:c1",
            "child:c2",
        ]
    )
    at = {s: i for i, s in enumerate(seen)}
    # A child's records never come before its OPEN.
    assert at["agent:LINK OPEN tool-1"] < at["tool-1:a1"] < at["tool-1:a2"]
    assert at["agent:LINK OPEN tool-2"] < at["tool-2:b1"]
    assert at["agent:LINK OPEN sub"] < at["child:c1"] < at["child:c2"]
    # A parent record after a JOIN waits for the child's end.
    assert at["tool-1:a2"] < at["agent:d2"]
    assert at["child:c2"] < at["agent:d3"]
    assert at["agent:d1"] < at["agent:d2"] < at["agent:d3"]


async def run_agent(streams: Client, task_queue: str) -> str:
    agent_id = new_workflow_id()
    await streams.execute_workflow(Agent.run, id=agent_id, task_queue=task_queue)
    return agent_id


async def follow_all(streams: Client, agent_id: str, **kwargs: Any) -> list[Any]:
    handle = stream_handle(streams, agent_id, topic=EVENTS)
    items: list[Any] = []

    async def pull() -> None:
        async for item in handle.follow(**kwargs):
            items.append(item)

    await asyncio.wait_for(pull(), 30)
    return items


async def test_a_client_follows_an_agent_its_tools_and_its_child(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, Agent, Child, activities=[tool]) as worker:
        agent_id = await run_agent(streams, worker.task_queue)
        items = await follow_all(streams, agent_id)
    seen = [describe(i) for i in items]
    check_order(seen)
    # A link report belongs to the linked stream, once per state.
    for name in ("tool-1", "tool-2"):
        assert seen.count(f"{name}:OPENED {name}") == 1
        assert seen.count(f"{name}:DONE {name}") == 1
    assert seen.count("child:OPENED sub") == seen.count("child:DONE sub") == 1
    # A held JOIN comes after its child's DONE.
    assert seen.index("tool-1:DONE tool-1") < seen.index("agent:LINK JOIN tool-1")
    assert seen.index("child:DONE sub") < seen.index("agent:LINK JOIN sub")
    # A tool's stream closed with a null result when the agent saw it end.
    done = [i.link_state for i in items if i.link_state is not None]
    assert all(s.result is None and s.error is None for s in done)
    assert items[-1].position is not None


async def test_a_follower_resumes_from_any_batch_end_with_no_loss_or_repeat(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, Agent, Child, activities=[tool]) as worker:
        agent_id = await run_agent(streams, worker.task_queue)
        whole = [describe(i) for i in await follow_all(streams, agent_id)]
        monkeypatch.setattr(follow_module, "_MAX_RECORDS", 2)
        batches = await follow_all(streams, agent_id)
        ends = [(n, i.position) for n, i in enumerate(batches) if i.position]
        assert len(ends) > 2
        for count, position in ends[:-1]:
            assert isinstance(position, FollowPosition)
            rest = await follow_all(streams, agent_id, after=position)
            assert [describe(i) for i in batches[: count + 1]] + [
                describe(i) for i in rest
            ] == [describe(i) for i in batches]
    # Smaller steps read in another order, and the gates still hold.
    check_order([describe(i) for i in batches])
    check_order(whole)


@workflow.defn
class FollowsAnAgent:
    @workflow.run
    async def run(self, agent_id: str, record_batches: bool) -> list[str]:
        seen: list[str] = []
        async with workflow_follower(
            agent_id, topic=EVENTS, replay_from_store=not record_batches
        ) as follower:
            async for batch in follower:
                seen.extend(describe(i) for i in batch.items)
        return seen


@pytest.mark.parametrize("record_batches", [True, False])
async def test_a_workflow_follows_an_agent_and_replays(
    client: Client, record_batches: bool
):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(
        streams, Agent, Child, FollowsAnAgent, activities=[tool]
    ) as worker:
        agent_id = await run_agent(streams, worker.task_queue)
        handle = await streams.start_workflow(
            FollowsAnAgent.run,
            args=[agent_id, record_batches],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        seen = await asyncio.wait_for(handle.result(), 30)
    check_order(seen)
    history = await handle.fetch_history()
    if record_batches:
        # The markers hold the batches, so a replay needs no store and no server.
        await Replayer(
            workflows=[FollowsAnAgent], plugins=[MemoryStreams()]
        ).replay_workflow(history)
    else:
        store = store_for_client(streams)
        store._follows.clear()
        await Replayer(workflows=[FollowsAnAgent], plugins=[store]).replay_workflow(
            history
        )
