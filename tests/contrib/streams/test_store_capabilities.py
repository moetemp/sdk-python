"""A store reports its capabilities, and the fallbacks keep reads whole.

The masked memory store and Redis with ``plain=True`` lack producer state,
so a retry or a second producer object can land a copy. Readers must not
see it. Redis runs when ``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import dataclasses
import os
import uuid

import pytest

import temporalio.bridge.proto.streams
from temporalio.bridge.proto.streams import StreamFailure, StreamFailureKind
from temporalio.bridge.streams import StreamCallFailure
from temporalio.client import Client
from temporalio.contrib.streams import (
    StoreCapabilities,
    StreamProducerError,
    StreamStorePlugin,
    StreamUnsupportedError,
    stream_handle,
    topic,
)
from temporalio.contrib.streams._capabilities import describe
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with, read_all
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

TOKENS = topic("tokens", str)
# What a store without producer state looks like, on the memory store.
NO_PRODUCER_STATE = StoreCapabilities(producer_state=False, kept_last=False)
# A stream runtime from before DescribeStore can't mask a store or run Redis plain.
HAS_DESCRIBE = hasattr(temporalio.bridge.proto.streams, "DescribeStoreRequest")
NO_DESCRIBE = "this stream runtime can't describe or mask a store"
needs_describe = pytest.mark.skipif(not HAS_DESCRIBE, reason=NO_DESCRIBE)


def redis_url() -> str:
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return url


def store(kind: str) -> StreamStorePlugin:
    if kind in ("masked", "plain") and not HAS_DESCRIBE:
        pytest.skip(NO_DESCRIBE)
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


@needs_describe
async def test_a_mask_turns_off_only_what_it_names(client: Client):
    full = capabilities_of(await connect_with(client, MemoryStreams()))
    assert full.producer_state and full.kept_last and full.delete_owner
    masked = capabilities_of(await connect_with(client, store("masked")))
    assert masked == dataclasses.replace(full, producer_state=False, kept_last=False)


@needs_describe
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
        stream = stream_handle(streams, workflow_id, topic=TOKENS)
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


@needs_describe
async def test_a_store_that_cannot_delete_by_owner_says_so(client: Client):
    masked = MemoryStreams(capabilities=StoreCapabilities(delete_owner=False))
    await connect_with(client, masked)
    assert not masked.capabilities.delete_owner
    with pytest.raises(StreamUnsupportedError):
        await masked.delete_workflow_streams(client.namespace, "any")


class _RuntimeWithoutDescribe:
    """A store of a stream runtime that knows no DescribeStore call."""

    def __init__(self, kind: StreamFailureKind.ValueType) -> None:
        self.kind = kind

    async def call(self, rpc: str, _req: object, _resp_type: type) -> object:
        raise StreamCallFailure(StreamFailure(kind=self.kind, message=f"no {rpc}"))


async def test_a_runtime_without_describe_serves_every_capability(
    monkeypatch: pytest.MonkeyPatch,
):
    # The fake store never parses, so any message types stand in for the call's.
    proto = temporalio.bridge.proto.streams
    monkeypatch.setattr(proto, "DescribeStoreRequest", StreamFailure, raising=False)
    monkeypatch.setattr(proto, "DescribeStoreResponse", StreamFailure, raising=False)
    unknown = _RuntimeWithoutDescribe(StreamFailureKind.STREAM_FAILURE_KIND_UNSUPPORTED)
    capabilities = await describe(unknown)  # type: ignore[arg-type]
    assert capabilities.producer_state and capabilities.kept_last
    assert capabilities.delete_owner and capabilities.topic_listing
    # Any other failure is the store's, not a missing call.
    failing = _RuntimeWithoutDescribe(StreamFailureKind.STREAM_FAILURE_KIND_STORAGE)
    with pytest.raises(StreamCallFailure):
        await describe(failing)  # type: ignore[arg-type]


async def test_an_sdk_without_the_describe_protos_serves_every_capability(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delattr(
        temporalio.bridge.proto.streams, "DescribeStoreRequest", raising=False
    )
    store = _RuntimeWithoutDescribe(StreamFailureKind.STREAM_FAILURE_KIND_STORAGE)
    assert (await describe(store)).producer_state  # type: ignore[arg-type]
