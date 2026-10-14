"""Positions-only Workflow reads: the marker holds the range, and replay reads it again.

These cases drive the read Activity and the client's codec directly, so they
run on the stock dev server with Core's real store. The cases that run a
Workflow reader end to end need the stream notifier and are in the live
lane.
"""

from __future__ import annotations

import dataclasses
import uuid

import pytest

from temporalio.api.common.v1 import Payload, Payloads
from temporalio.bridge.proto.streams import ReadRangeRequest, ReadResponse
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamReplayRangeLostError,
    StreamStorePlugin,
    get_stream_handle,
    topic,
)
from temporalio.contrib.streams._replay_reads import (
    POSITIONS_TYPE,
    ReplayReadCodec,
    is_positions,
)
from temporalio.contrib.streams._workflow import _ReadInput, read_activity
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.converter import DataConverter
from temporalio.testing import ActivityEnvironment
from temporalio.workflow import NondeterminismError
from tests.contrib.streams._support import connect_with
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

TOKENS = topic("tokens", str)


def store(kind: str) -> StreamStorePlugin:
    if kind == "memory":
        return MemoryStreams()
    import os

    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return RedisStreams(url, key_prefix=f"replay-reads-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis"])
async def streams(request: pytest.FixtureRequest, client: Client) -> Client:
    return await connect_with(client, store(request.param))


def plugin_of(client: Client) -> StreamStorePlugin:
    [plugin] = [
        p for p in client.config()["plugins"] if isinstance(p, StreamStorePlugin)
    ]
    return plugin


def codec_of(client: Client) -> ReplayReadCodec:
    codec = client.data_converter.payload_codec
    assert isinstance(codec, ReplayReadCodec)
    return codec


async def read_positions(client: Client, owner: str, after: str = "") -> Payload:
    """Run the read Activity in positions-only mode, as a Workflow reader's Local Activity does."""
    env = ActivityEnvironment(client=client)
    env.info = dataclasses.replace(env.info, workflow_run_id=f"reader-{uuid.uuid4()}")
    raw = await env.run(
        read_activity,
        _ReadInput(
            workflow_id=owner,
            chain_run_id="",
            topic=TOKENS.name,
            after=after,
            state="",
            arm=False,
            wait_ms=100,
            max_records=10,
            positions_only=True,
            reader_id="1",
        ),
    )
    return raw.payload


def values(payload: Payload) -> list[str]:
    response = ReadResponse.FromString(payload.data)
    return [
        DataConverter.default.payload_converter.from_payloads([r.stored.body])[0]
        for r in response.records
    ]


async def test_the_marker_holds_the_range_not_the_records(streams: Client):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        await (
            get_stream_handle(streams, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("a" * 1000, "b" * 1000)
        )
        payload = await read_positions(streams, owner)
        assert payload.metadata["messageType"] == POSITIONS_TYPE
        # A few hundred bytes, whatever the batch holds.
        assert payload.ByteSize() < 1000
        range = ReadRangeRequest.FromString(
            Payloads.FromString(payload.data).payloads[0].data
        )
        assert range.stored_records == 2 and range.records_digest
        assert range.floor_holder.endswith("/1")
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_replay_reads_the_same_batch_from_the_store(streams: Client):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        await (
            get_stream_handle(streams, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("a", "b")
        )
        payload = await read_positions(streams, owner)
        # The live task takes the batch the Activity cached.
        [live] = await codec_of(streams).decode([payload])
        assert values(live) == ["a", "b"]
        # A replay in another process has no cache, and reads the range again.
        plugin_of(streams)._batches.clear()
        [replayed] = await codec_of(streams).decode([payload])
        assert not is_positions(replayed)
        assert replayed == live
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_other_records_in_the_range_are_nondeterminism(streams: Client):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        await (
            get_stream_handle(streams, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("a")
        )
        payload = await read_positions(streams, owner)
        parts = Payloads.FromString(payload.data)
        range = ReadRangeRequest.FromString(parts.payloads[0].data)
        range.records_digest = bytes(32)
        parts.payloads[0].data = range.SerializeToString()
        tampered = Payload()
        tampered.CopyFrom(payload)
        tampered.data = parts.SerializeToString()
        with pytest.raises(NondeterminismError, match="other records"):
            await codec_of(streams).decode([tampered])
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_a_range_the_store_lost_fails_for_a_retry(streams: Client):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        await (
            get_stream_handle(streams, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("a")
        )
        payload = await read_positions(streams, owner)
        plugin = plugin_of(streams)
        plugin._batches.clear()
        await plugin.delete_workflow_streams(streams.namespace, owner)
        with pytest.raises(StreamReplayRangeLostError):
            await codec_of(streams).decode([payload])
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_the_batch_names_the_run_that_read(streams: Client):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        await (
            get_stream_handle(streams, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("a")
        )
        payload = await read_positions(streams, owner)
        range = ReadRangeRequest.FromString(
            Payloads.FromString(payload.data).payloads[0].data
        )
        [batch] = await codec_of(streams).decode([payload])
        # A reset run compares this with its own run id.
        reader_run = range.floor_holder.split("/")[0]
        assert (
            batch.metadata["temporal.io/stream-read-by-run-id"] == reader_run.encode()
        )
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_the_codec_passes_other_payloads_through(client: Client):
    streams = await connect_with(client, MemoryStreams())
    [payload] = DataConverter.default.payload_converter.to_payloads(["plain"])
    assert await codec_of(streams).decode([payload]) == [payload]
    assert await codec_of(streams).encode([payload]) == [payload]
