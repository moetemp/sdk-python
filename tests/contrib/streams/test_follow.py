"""Following a Workflow's stream and the streams its links open, in one order."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    FollowedItem,
    FollowPosition,
    LinkStateKind,
    RecordKind,
    StreamError,
    StreamRef,
    _follow,
    activity_handle,
    stream_handle,
    stream_producer,
    topic,
)
from temporalio.contrib.streams.memory import MemoryStreams
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

EVENTS = topic("events", str)


@workflow.defn
class Grandchild:
    @workflow.run
    async def run(self) -> None:
        events = stream_producer(EVENTS)
        events.append("g1")
        events.append("g2")


@workflow.defn
class Child:
    @workflow.run
    async def run(self, name: str, grandchild: bool) -> None:
        events = stream_producer(EVENTS)
        events.append(f"{name}1")
        if grandchild:
            child_id = f"{workflow.info().workflow_id}-g"
            events.link(StreamRef.for_workflow(child_id, topic=EVENTS), name="g")
            await workflow.execute_child_workflow(Grandchild.run, id=child_id)
            events.join(name="g")
        events.append(f"{name}2")


@workflow.defn
class Agent:
    @workflow.run
    async def run(self) -> None:
        events = stream_producer(EVENTS)
        agent_id = workflow.info().workflow_id
        events.append("d1")
        children = {}
        for name, grandchild in (("one", False), ("two", True)):
            child_id = f"{agent_id}-{name}"
            events.link(StreamRef.for_workflow(child_id, topic=EVENTS), name=name)
            children[name] = await workflow.start_child_workflow(
                Child.run, args=[name, grandchild], id=child_id
            )
        events.append("d2")
        await children["one"]
        events.join(name="one")
        events.append("d3")
        await children["two"]
        events.join(name="two")
        events.append("d4")


def shape(items: list[FollowedItem[Any]]) -> list[str]:
    """Each item as a word: a value, a link record's event, or a link report."""
    out = []
    for item in items:
        if item.link_state is not None:
            state = item.link_state
            reason = f":{state.reason}" if state.reason else ""
            out.append(f"{state.kind.name}:{state.link.name}{reason}")
        elif item.record is not None and item.record.kind is RecordKind.LINK:
            assert item.record.link is not None
            out.append(f"{item.record.link.event.name}:{item.record.link.name}")
        elif item.record is not None and item.record.kind is RecordKind.DATA:
            out.append(str(item.record.value))
    return out


def check_gates(order: list[str]) -> None:
    """Order within each stream, and the open and close gates."""
    assert len(set(order)) == len(order), order
    at = order.index
    assert at("OPENED:one") < at("one1")
    assert at("OPENED:two") < at("two1")
    assert at("OPENED:g") < at("g1")
    assert at("DONE:one") < at("JOIN:one") < at("d3")
    assert at("DONE:g") < at("JOIN:g") < at("two2")
    assert at("DONE:two") < at("JOIN:two") < at("d4")
    for a, b in (
        ("d1", "d2"),
        ("d2", "d3"),
        ("d3", "d4"),
        ("one1", "one2"),
        ("two1", "two2"),
        ("g1", "g2"),
    ):
        assert at(a) < at(b), (a, b, order)


async def collect(
    follow: AsyncGenerator[FollowedItem[Any], None],
) -> list[FollowedItem[Any]]:
    async def pull() -> list[FollowedItem[Any]]:
        return [item async for item in follow]

    return await asyncio.wait_for(pull(), 20)


async def run_agent(client: Client) -> str:
    workflow_id = new_workflow_id()
    async with new_worker(client, Agent, Child, Grandchild) as worker:
        await client.execute_workflow(
            Agent.run, id=workflow_id, task_queue=worker.task_queue
        )
    return workflow_id


async def test_a_follow_reads_the_agent_and_its_children_in_one_order(client: Client):
    streams = await connect_with(client, MemoryStreams())
    agent_id = await run_agent(streams)
    items = await collect(stream_handle(streams, agent_id, topic=EVENTS).follow())
    order = shape(items)
    check_gates(order)
    assert set(order) >= {"d1", "d2", "d3", "d4", "one1", "one2", "two1", "two2"}
    sources = {item.source.workflow_id: item.depth for item in items}
    assert sources == {
        agent_id: 0,
        f"{agent_id}-one": 1,
        f"{agent_id}-two": 1,
        f"{agent_id}-two-g": 2,
    }
    # A child's records keep the cursors a plain read of the child gives.
    one = stream_handle(streams, f"{agent_id}-one", topic=EVENTS)
    assert [
        item.record.cursor
        for item in items
        if item.record is not None and item.source == one.ref
    ] == [record.cursor for record in await read_all(one.read())]
    assert items[-1].position is not None


async def test_a_follow_resumed_at_each_batch_end_loses_and_repeats_nothing(
    client: Client, monkeypatch: pytest.MonkeyPatch
):
    streams = await connect_with(client, MemoryStreams())
    agent_id = await run_agent(streams)
    handle = stream_handle(streams, agent_id, topic=EVENTS)
    # One stored record per batch, so every record ends a batch.
    monkeypatch.setattr(_follow, "_MAX_RECORDS", 1)
    whole = shape(await collect(handle.follow()))
    check_gates(whole)

    resumed: list[FollowedItem[Any]] = []
    after: FollowPosition | None = None
    batches = 0
    while True:
        follow = handle.follow(after=after or BEGINNING)
        ended = True
        async for item in follow:
            resumed.append(item)
            if item.position is not None:
                after = item.position
                ended = False
                break
        await follow.aclose()
        if ended:
            break
        batches += 1
        assert batches < 200
    assert batches > len(whole) // 2
    assert shape(resumed) == whole


async def test_a_follow_skips_a_link_past_its_depth(client: Client):
    streams = await connect_with(client, MemoryStreams())
    agent_id = await run_agent(streams)
    handle = stream_handle(streams, agent_id, topic=EVENTS)
    order = shape(await collect(handle.follow(max_depth=1)))
    assert "SKIPPED:g:depth" in order
    assert not any(word.startswith("g") for word in order)
    # A JOIN of a skipped link waits for nothing.
    assert order.index("JOIN:g") < order.index("two2")
    with pytest.raises(ValueError, match="at least 1"):
        handle.follow(max_depth=0)
    with pytest.raises(ValueError, match="at least 1"):
        handle.follow(max_open=0)


async def test_a_link_back_to_an_open_stream_is_a_cycle(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, Waits) as worker:
        agent_id = await running_owner(streams, worker.task_queue)
        child_id = await running_owner(streams, worker.task_queue)
        agent = stream_handle(streams, agent_id, topic=EVENTS)
        child = stream_handle(streams, child_id, topic=EVENTS)
        writes = child.producer(producer_id="c", attempt=1)
        await writes.append("c1")
        await writes.link(agent.ref, name="back")
        await writes.append("c2")
        writes = agent.producer(producer_id="a", attempt=1)
        await writes.link(child.ref, name="child")
        await writes.join(name="child")
        await writes.append("d1")
        for workflow_id in (child_id, agent_id):
            owner = streams.get_workflow_handle(workflow_id)
            await owner.signal(Waits.finish)
            await owner.result()
        order = shape(await collect(agent.follow()))
    assert order == [
        "OPEN:child",
        "OPENED:child",
        "c1",
        "OPEN:back",
        "SKIPPED:back:cycle",
        "c2",
        "DONE:child",
        "JOIN:child",
        "d1",
    ]


async def test_a_retry_that_writes_the_same_open_resumes_the_child(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, Waits) as worker:
        agent_id = await running_owner(streams, worker.task_queue)
        child_id = await running_owner(streams, worker.task_queue)
        agent = stream_handle(streams, agent_id, topic=EVENTS)
        child = stream_handle(streams, child_id, topic=EVENTS)
        await child.producer(producer_id="c", attempt=1).append("c1", "c2")
        for attempt in (1, 2):
            writes = agent.producer(producer_id="tool", attempt=attempt)
            await writes.append(f"m{attempt}")
            await writes.link(child.ref, name="child")
        for workflow_id in (child_id, agent_id):
            owner = streams.get_workflow_handle(workflow_id)
            await owner.signal(Waits.finish)
            await owner.result()
        items = await collect(agent.follow())
    assert shape(items) == [
        "m1",
        "OPEN:child",
        "OPENED:child",
        "DONE:child:withdrawn",
        "m2",
        "OPEN:child",
        "OPENED:child:resumed",
        "c1",
        "c2",
        "DONE:child",
    ]
    states = [item for item in items if item.link_state is not None]
    assert [item.link_state.reason for item in states if item.link_state] == [
        "",
        "withdrawn",
        "resumed",
        "",
    ]
    assert len({item.source for item in states}) == 1, "the child keeps its source"
    assert any(
        item.record and item.record.kind is RecordKind.SUPERSEDED for item in items
    )


async def test_a_missing_child_is_unavailable_after_the_grace(client: Client):
    streams = await connect_with(client, MemoryStreams(owner_start_grace=timedelta(0)))
    async with new_worker(streams, Waits) as worker:
        agent_id = await running_owner(streams, worker.task_queue)
        agent = stream_handle(streams, agent_id, topic=EVENTS)
        writes = agent.producer(producer_id="a", attempt=1)
        gone = StreamRef.for_workflow(f"{agent_id}-never-started", topic=EVENTS)
        await writes.link(gone, name="gone")
        await writes.append("d1")
        owner = streams.get_workflow_handle(agent_id)
        await owner.signal(Waits.finish)
        await owner.result()
        items = await collect(agent.follow())
    assert shape(items) == ["OPEN:gone", "OPENED:gone", "d1", "UNAVAILABLE:gone"]
    unavailable = items[-1].link_state
    assert unavailable is not None and unavailable.kind is LinkStateKind.UNAVAILABLE
    assert isinstance(unavailable.error, StreamError)
    assert unavailable.link.ref == gone


@activity.defn
async def follow_own_stream() -> list[str]:
    # The scheduling Workflow is still running, so the follow stops at the last word.
    items = []
    async for item in activity_handle().follow(topic=EVENTS):
        items.append(item)
        if shape(items)[-1:] == ["end"]:
            break
    return shape(items)


@workflow.defn
class FollowsFromAnActivity:
    @workflow.run
    async def run(self) -> list[str]:
        events = stream_producer(EVENTS)
        child_id = f"{workflow.info().workflow_id}-g"
        events.link(StreamRef.for_workflow(child_id, topic=EVENTS), name="g")
        await workflow.execute_child_workflow(Grandchild.run, id=child_id)
        events.join(name="g")
        events.append("end")
        return await workflow.execute_activity(
            follow_own_stream, start_to_close_timeout=timedelta(seconds=20)
        )


async def test_an_activity_follows_its_workflows_stream(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(
        streams,
        FollowsFromAnActivity,
        Grandchild,
        activities=[follow_own_stream],
    ) as worker:
        order = await streams.execute_workflow(
            FollowsFromAnActivity.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert order == ["OPEN:g", "OPENED:g", "g1", "g2", "DONE:g", "JOIN:g", "end"]
