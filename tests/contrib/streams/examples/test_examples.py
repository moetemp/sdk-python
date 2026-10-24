"""Runs each example end to end on the memory Store and on Redis, so none goes stale."""

import uuid

import pytest

from temporalio.client import Client
from temporalio.common import streams
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from tests.contrib.streams.conftest import NATIVE_NAMESPACE, NATIVE_TARGET
from tests.contrib.streams.examples import (
    activity_streams_tokens,
    agent_publishes_turns,
    client_creates_stream,
    migrate_redis_to_native,
    resume_from_offset,
    turn_scoped_stream,
    two_backends,
    ui_backend_reads,
    worker_setup,
    workflow_reader,
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
    data = [item.data for item in items if isinstance(item, streams.Record)]
    Turn = agent_publishes_turns.Turn
    assert data == [Turn("model"), "find", "it", Turn("model"), "search", "result"] + [
        Turn("done", "the answer")
    ]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "the answer"


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


# The Jetty v2 examples run with a native stand-in, a memory Store with the backend name
# `native`, and on a server with the Stream service when TEMPORAL_STREAMS_NATIVE_TARGET is set.
LANES = ["memory", "redis", "native"]


def lane_plugins(
    lane: str, request: pytest.FixtureRequest
) -> tuple[streams.StreamBackend, streams.StreamBackend]:
    """The plugins of a lane: one named `redis` and one named `native`."""
    if lane == "memory":
        return MemoryStreamBackend(name="redis", default=True), MemoryStreamBackend(
            name="native"
        )
    if lane == "native" and not NATIVE_TARGET:
        pytest.skip(
            "TEMPORAL_STREAMS_NATIVE_TARGET names no server with the Stream service"
        )
    redis_url = request.getfixturevalue("redis_url")
    redis = RedisStreamBackend(redis_url, key_prefix=f"t-{uuid.uuid4()}", default=True)
    if lane == "redis":
        return redis, MemoryStreamBackend(name="native")
    return redis, streams.NativeStreamBackend()


async def lane_client(
    lane: str, client: Client, plugins: list[streams.StreamBackend]
) -> Client:
    if lane != "native":
        config = client.config()
        config["stream_backends"] = plugins
        return Client(**config)
    if not NATIVE_TARGET:
        pytest.skip(
            "TEMPORAL_STREAMS_NATIVE_TARGET names no server with the Stream service"
        )
    return await Client.connect(
        NATIVE_TARGET, namespace=NATIVE_NAMESPACE, stream_backends=plugins
    )


@pytest.mark.parametrize("lane", LANES)
async def test_two_backends(lane: str, client: Client, request: pytest.FixtureRequest):
    two = await lane_client(lane, client, list(lane_plugins(lane, request)))
    assert await two_backends.main(two, task_queue()) == {
        "audit": ["started", "summarized"],
        "live": ["hello", "there", "summary"],
    }


@pytest.mark.parametrize("lane", LANES)
async def test_workflow_reader(
    lane: str, client: Client, request: pytest.FixtureRequest
):
    # Each lane reads from one backend: the memory Store, Redis or the server.
    plugin = {"memory": 0, "redis": 0, "native": 1}[lane]
    one = await lane_client(lane, client, [lane_plugins(lane, request)[plugin]])
    assert await workflow_reader.main(one, task_queue()) == [
        "find",
        "the",
        "answer",
        "agent said FIND THE ANSWER",
    ]


@pytest.mark.parametrize("lane", LANES)
async def test_migrate_redis_to_native(
    lane: str, client: Client, request: pytest.FixtureRequest
):
    redis, native = lane_plugins(lane, request)
    before = await lane_client(lane, client, [redis])
    during = await lane_client(lane, client, [redis, native])
    assert await migrate_redis_to_native.main(before, during, task_queue()) == [
        "redis: hello",
        "redis: old chat goes on",
        "native: hello",
        "native: new chat",
    ]
