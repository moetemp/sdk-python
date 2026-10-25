"""Runs each example end to end on the memory Store and on Redis, so a broken one fails."""

import uuid

from temporalio import common
from temporalio.client import Client
from tests.contrib.streams.examples import (
    activity_streams_tokens,
    agent_publishes_turns,
    client_creates_stream,
    resume_from_offset,
    turn_scoped_stream,
    ui_backend_reads,
    worker_setup,
)


def task_queue() -> str:
    return f"tq-{uuid.uuid4()}"


async def test_ui_backend_reads(stream_client: Client):
    screen = await ui_backend_reads.main(stream_client, task_queue())
    assert [(record.topic, record.data) for record in screen.shown.values()] == [
        ("turns", ui_backend_reads.Turn("model")),
        ("tokens", "hello"),
        ("tokens", "there"),
        ("turns", ui_backend_reads.Turn("done", "HELLO THERE")),
    ]
    assert screen.result == "HELLO THERE"


async def test_resume_from_offset(stream_client: Client):
    assert await resume_from_offset.main(stream_client, task_queue()) == (
        ["a", "b"],
        [],
        ["c", "d"],
    )


async def test_client_creates_stream(stream_client: Client):
    turns = await client_creates_stream.main(stream_client, task_queue())
    assert [turn.kind for turn in turns] == ["model", "last"]


async def test_agent_publishes_turns(stream_client: Client):
    items = await agent_publishes_turns.main(stream_client, task_queue())
    data = [item.data for item in items if isinstance(item, common.Record)]
    Turn = agent_publishes_turns.Turn
    assert data == [Turn("model"), "find", "it", Turn("model"), "search", "result"] + [
        Turn("done", "the answer")
    ]
    assert isinstance(items[-1], common.Closed) and items[-1].result == "the answer"


async def test_activity_streams_tokens(stream_client: Client):
    offsets, records = await activity_streams_tokens.main(stream_client, task_queue())
    assert [record.data for record in records] == [
        "one",
        "two",
        "three",
        "summary: one two three",
    ]
    assert offsets == [records[0].offset, records[-1].offset]


async def test_turn_scoped_stream(stream_client: Client):
    assert await turn_scoped_stream.main(stream_client, task_queue()) == {
        0: ["a", "b"],
        1: ["c"],
        2: ["d", "e", "f"],
    }


async def test_worker_setup_on_memory(client: Client):
    target = client.service_client.config.target_host
    test_client = await worker_setup.connect_for_tests(target)
    assert await worker_setup.main(test_client, task_queue()) == [
        "hello ada",
        "bye ada",
    ]


async def test_worker_setup_on_redis(client: Client, redis_url: str):
    target = client.service_client.config.target_host
    redis_client = await worker_setup.connect_with_redis(target, redis_url)
    assert await worker_setup.main(redis_client, task_queue()) == [
        "hello ada",
        "bye ada",
    ]
    migrating = await worker_setup.connect_while_migrating(target, redis_url, redis_url)
    assert await worker_setup.create_on_second_backend(migrating) == "redis-b"
