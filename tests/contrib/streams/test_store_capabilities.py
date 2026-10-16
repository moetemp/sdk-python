"""A store reports its capabilities, and Core's fallbacks keep reads whole.

The masked memory store and Redis with ``plain=True`` lack producer state,
so a retry or a second producer object can land a copy. Readers must not
see it. Redis runs when ``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import dataclasses
import os
import uuid

import pytest

from temporalio.client import Client
from temporalio.contrib.streams import (
    StoreCapabilities,
    StreamProducerError,
    StreamStorePlugin,
    StreamUnsupportedError,
    get_stream_handle,
    topic,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with, read_all
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

TOKENS = topic("tokens", str)
# What a store without producer state looks like, on the memory store.
NO_PRODUCER_STATE = StoreCapabilities(producer_state=False, kept_last=False)


def redis_url() -> str:
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return url


def store(kind: str) -> StreamStorePlugin:
    if kind == "memory":
        return MemoryStreams()
    if kind == "masked":
        return MemoryStreams(capabilities=NO_PRODUCER_STATE)
    prefix = f"capabilities-{uuid.uuid4().hex}"
    return RedisStreams(redis_url(), key_prefix=prefix, plain=kind == "plain")


@pytest.fixture(params=["memory", "masked", "redis", "plain"])
async def streams(request: pytest.FixtureRequest, client: Client) -> Client:
    return await connect_with(client, store(request.param))


def capabilities_of(client: Client) -> StoreCapabilities:
    (plugin,) = client.config()["plugins"]
    assert isinstance(plugin, StreamStorePlugin)
    return plugin.capabilities


def test_capabilities_need_a_connected_store():
    with pytest.raises(ValueError, match="no client has connected"):
        MemoryStreams().capabilities


async def test_a_mask_turns_off_only_what_it_names(client: Client):
    full = capabilities_of(await connect_with(client, MemoryStreams()))
    assert full.producer_state and full.kept_last and full.delete_owner
    masked = capabilities_of(await connect_with(client, store("masked")))
    assert masked == dataclasses.replace(full, producer_state=False, kept_last=False)


async def test_plain_redis_has_no_store_side_state(client: Client):
    plain = capabilities_of(await connect_with(client, store("plain")))
    assert not (plain.producer_state or plain.kept_last)
    assert not (plain.holds or plain.change_feed)
    full = capabilities_of(await connect_with(client, store("redis")))
    assert full.producer_state and full.kept_last


async def test_a_reader_sees_one_copy_of_each_batch(streams: Client):
    has_state = capabilities_of(streams).producer_state
    async with new_worker(streams, Waits) as worker:
        workflow_id = await running_owner(streams, worker.task_queue)
        stream = get_stream_handle(streams, workflow_id, topic=TOKENS)
        first = await stream.producer(producer_id="p", attempt=1).append("a", "b")
        # A second object for the same session repeats the batch, as a retry
        # after a lost answer does.
        again = stream.producer(producer_id="p", attempt=1)
        assert (await again.append("a", "b") == first) == has_state
        divergent = stream.producer(producer_id="p", attempt=1)
        if has_state:
            with pytest.raises(StreamProducerError):
                await divergent.append("x", "y")
        else:
            await divergent.append("x", "y")
        await again.append("c")
        owner = streams.get_workflow_handle(workflow_id)
        await owner.signal(Waits.finish)
        await owner.result()
    records = await read_all(stream.read())
    assert [r.value for r in records] == ["a", "b", "c"]


async def test_a_store_that_cannot_delete_by_owner_says_so(client: Client):
    masked = MemoryStreams(capabilities=StoreCapabilities(delete_owner=False))
    await connect_with(client, masked)
    assert not masked.capabilities.delete_owner
    with pytest.raises(StreamUnsupportedError):
        await masked.delete_workflow_streams(client.namespace, "any")
