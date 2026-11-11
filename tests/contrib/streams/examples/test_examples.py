"""Runs each example end to end on the memory Store and on Redis, so a broken one fails."""

import uuid

import pytest

from temporalio import streams
from temporalio.client import Client
from temporalio.contrib.stream.testing import MemoryStreamBackend
from tests.contrib.streams.examples import (
    activity_streams_tokens,
    agent_publishes_turns,
    client_creates_stream,
    resume_from_offset,
    ui_backend_reads,
    worker_setup,
)


def task_queue() -> str:
    return f"tq-{uuid.uuid4()}"


async def test_ui_backend_reads(stream_client: Client):
    screen = await ui_backend_reads.main(stream_client, task_queue())
    Turn = ui_backend_reads.Turn
    assert list(screen.shown.values()) == [Turn("model"), Turn("done", "HELLO THERE")]


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
    records = await agent_publishes_turns.main(stream_client, task_queue())
    Turn = agent_publishes_turns.Turn
    # The read ends at the close, after the done Turn that carries the reply.
    assert [record.data for record in records] == [
        Turn("model"),
        Turn("model"),
        Turn("done", "the answer"),
    ]


async def test_activity_streams_tokens(stream_client: Client):
    summary, records = await activity_streams_tokens.main(stream_client, task_queue())
    assert [record.data for record in records] == [
        "one",
        "two",
        "three",
        "summary: ONE TWO THREE",
    ]
    assert summary == str(records[-1].offset)


async def test_worker_setup_on_memory(client: Client):
    async with await worker_setup.start_test_environment() as env:
        assert await worker_setup.main(env.client, task_queue()) == [
            "hello ada",
            "bye ada",
        ]
    # The migration's selector, with memory backends in place of the two Redis backends.
    config = client.config()
    config["stream_config"] = worker_setup.migration_config(
        MemoryStreamBackend(name="redis-a"), MemoryStreamBackend(name="redis-b")
    )
    migrating = Client(**config)
    assert await worker_setup.create_while_migrating(migrating) == (
        "redis-b",
        "redis-a",
    )
    with pytest.raises(streams.StreamConfigError, match="client with one backend"):
        worker_setup.run_worker(migrating, task_queue())


async def test_worker_setup_on_redis(client: Client, redis_url: str):
    target = client.service_client.config.target_host
    redis_client = await worker_setup.connect(target, redis_url)
    assert await worker_setup.main(redis_client, task_queue()) == [
        "hello ada",
        "bye ada",
    ]
    migrating = await worker_setup.connect_while_migrating(target, redis_url, redis_url)
    assert await worker_setup.create_while_migrating(migrating) == (
        "redis-b",
        "redis-a",
    )
    with pytest.raises(streams.StreamConfigError, match="client with one backend"):
        worker_setup.run_worker(migrating, task_queue())
