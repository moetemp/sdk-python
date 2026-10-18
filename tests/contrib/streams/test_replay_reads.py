"""Positions-only Workflow reads: the marker holds the range, and replay reads it again.

These cases drive the read Activity and the client's codec directly, so they
run on the stock dev server with the stream runtime's real store. The cases that run a
Workflow reader end to end need the stream notifier and are in the live
lane.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio.api.common.v1 import Payload, Payloads
from temporalio.bridge.proto.streams import ReadRangeRequest, ReadResponse
from temporalio.client import Client
from temporalio.contrib.streams import (
    END,
    StoreCapabilities,
    StreamError,
    StreamReplayRangeLostError,
    StreamStorePlugin,
    stream_handle,
    topic,
)
from temporalio.contrib.streams._replay_reads import (
    POSITIONS_TYPE,
    BatchCache,
    ReadStates,
    ReplayReadCodec,
    is_positions,
)
from temporalio.contrib.streams._workflow import _ReadInput, read_activity
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.workflow import NondeterminismError
from tests.contrib.streams._support import connect_with
from tests.contrib.streams.test_outside_handles import (
    ActivityKeyedCodec,
    Waits,
    owns_a_stream,
    running_owner,
)
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


async def read_positions(
    client: Client,
    owner: str,
    after: str = "",
    wait_ms: int = 100,
    state: bytes = b"",
    read_seq: int = 0,
    run_id: str | None = None,
    chained: bool = False,
) -> Payload:
    """Run the read Activity in positions-only mode, as a Workflow reader's Local Activity does."""
    env = ActivityEnvironment(client=client)
    env.info = dataclasses.replace(
        env.info,
        workflow_run_id=run_id or f"reader-{uuid.uuid4()}",
        is_local=True,
    )
    raw = await env.run(
        read_activity,
        _ReadInput(
            workflow_id=owner,
            chain_run_id="",
            topic=TOKENS.name,
            after=after,
            state=base64.b64encode(state).decode(),
            wait_ms=wait_ms,
            max_records=10,
            positions_only=True,
            reader_id="1",
            read_seq=read_seq,
            chained=chained,
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
            stream_handle(streams, owner, topic=TOKENS)
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
            stream_handle(streams, owner, topic=TOKENS)
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
            stream_handle(streams, owner, topic=TOKENS)
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
            stream_handle(streams, owner, topic=TOKENS)
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


async def test_a_store_with_no_holds_refuses_positions_only_reads(client: Client):
    store = MemoryStreams(capabilities=StoreCapabilities(holds=False))
    streams = await connect_with(client, store)
    assert not store.capabilities.holds
    assert store.capabilities.replay_horizon is None
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        with pytest.raises(ApplicationError) as raised:
            await read_positions(streams, owner)
        assert raised.value.type == "StreamUnsupportedError"
        assert "replay_from_store=False" in str(raised.value)
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_a_store_with_a_replay_horizon_serves_positions_only_reads(
    client: Client,
):
    horizon = StoreCapabilities(holds=False, replay_horizon=timedelta(days=30))
    store = MemoryStreams(capabilities=horizon)
    streams = await connect_with(client, store)
    assert store.capabilities.replay_horizon == timedelta(days=30)
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        await (
            stream_handle(streams, owner, topic=TOKENS)
            .producer(producer_id="p", attempt=1)
            .append("a")
        )
        assert is_positions(await read_positions(streams, owner))
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_the_batch_names_the_run_that_read(streams: Client):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        await (
            stream_handle(streams, owner, topic=TOKENS)
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


async def test_a_read_from_the_end_replays_as_it_read(streams: Client):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        handle = stream_handle(streams, owner, topic=TOKENS)
        await handle.producer(producer_id="p", attempt=1).append("first")
        read = asyncio.create_task(
            read_positions(streams, owner, after=END.token, wait_ms=5000)
        )
        await asyncio.sleep(0.5)
        # A rerun of the producer appends while the read waits at the end.
        await handle.producer(producer_id="p", attempt=2).append("rerun")
        payload = await read
        [live] = await codec_of(streams).decode([payload])
        assert values(live) == ["rerun"]
        # The replay starts where the read resolved the end, so it observes
        # nothing more than the live read did.
        plugin_of(streams)._batches.clear()
        [replayed] = await codec_of(streams).decode([payload])
        assert ReadResponse.FromString(replayed.data) == ReadResponse.FromString(
            live.data
        )
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_a_later_read_replays_from_the_previous_reads_state(
    streams: Client,
):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        handle = stream_handle(streams, owner, topic=TOKENS)
        await handle.producer(producer_id="p", attempt=1).append("a")
        run_id = f"reader-{uuid.uuid4()}"
        first = await read_positions(streams, owner, read_seq=1, run_id=run_id)
        [first_live] = await codec_of(streams).decode([first])
        answer = ReadResponse.FromString(first_live.data)
        await handle.producer(producer_id="p", attempt=2).append("b")
        second = await read_positions(
            streams,
            owner,
            after=answer.cursor,
            state=answer.state,
            read_seq=2,
            run_id=run_id,
            chained=True,
        )
        # The second marker leaves out its input state, which the first holds.
        range = ReadRangeRequest.FromString(
            Payloads.FromString(second.data).payloads[0].data
        )
        assert not range.state
        [second_live] = await codec_of(streams).decode([second])
        live = ReadResponse.FromString(second_live.data)
        assert live.records[0].HasField("superseded")
        # A replay in another process decodes the markers in order.
        plugin = plugin_of(streams)
        plugin._batches.clear()
        plugin._read_states = ReadStates()
        replayed = await codec_of(streams).decode([first, second])
        assert ReadResponse.FromString(replayed[1].data) == live
        # Without the first read, the second can't replay in this process.
        plugin._batches.clear()
        plugin._read_states = ReadStates()
        with pytest.raises(StreamError, match="previous read"):
            await codec_of(streams).decode([second])
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_two_runs_of_one_workflow_chain_their_reads_apart(streams: Client):
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        handle = stream_handle(streams, owner, topic=TOKENS)
        a = await handle.producer(producer_id="p", attempt=1).append("a")
        await handle.producer(producer_id="q", attempt=1).append("x")
        # Both runs have reader 1, but the second run starts past "a", so its
        # state has not seen producer p.
        firsts = {
            "run-a": await read_positions(streams, owner, read_seq=1, run_id="run-a"),
            "run-b": await read_positions(
                streams, owner, after=a.token, read_seq=1, run_id="run-b"
            ),
        }
        answers = {
            run: ReadResponse.FromString((await codec_of(streams).decode([p]))[0].data)
            for run, p in firsts.items()
        }
        await handle.producer(producer_id="p", attempt=2).append("b")
        seconds = {
            run: await read_positions(
                streams,
                owner,
                after=answers[run].cursor,
                state=answers[run].state,
                read_seq=2,
                run_id=run,
                chained=True,
            )
            for run in firsts
        }
        live = {
            run: (await codec_of(streams).decode([p]))[0] for run, p in seconds.items()
        }
        # A replay that interleaves the two runs gives each read its own run's
        # input state.
        plugin = plugin_of(streams)
        plugin._batches.clear()
        plugin._read_states = ReadStates()
        _, _, a2, b2 = await codec_of(streams).decode(
            [firsts["run-a"], firsts["run-b"], seconds["run-a"], seconds["run-b"]]
        )
        assert ReadResponse.FromString(a2.data).records[0].HasField("superseded")
        assert a2.data == live["run-a"].data and b2.data == live["run-b"].data
        await streams.get_workflow_handle(owner).signal(Waits.finish)


def test_the_batch_cache_keeps_its_bytes_bounded():
    cache = BatchCache(size=10, max_bytes=3000)
    ranges = [ReadRangeRequest(after=str(i), to=str(i + 1)) for i in range(3)]
    big = ReadResponse(cursor="x" * 1400)
    for read in ranges:
        cache.put(read, big)
    assert cache.take(ranges[0]) is None
    assert cache.take(ranges[1]) == big and cache.take(ranges[2]) == big


async def test_a_first_range_trimmed_past_its_hold_is_lost_not_other_records(
    client: Client,
):
    import os

    settings: dict[str, Any] = {
        "retention": timedelta(milliseconds=300),
        "floor_lease": timedelta(milliseconds=100),
    }
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    # Retention trims as the next appends land, once the reader's hold lapsed.
    store = RedisStreams(url, key_prefix=f"replay-reads-{uuid.uuid4().hex}", **settings)
    streams = await connect_with(client, store)
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        producer = stream_handle(streams, owner, topic=TOKENS).producer(
            producer_id="p", attempt=1
        )
        await producer.append("a", "b")
        payload = await read_positions(streams, owner, read_seq=1)
        await asyncio.sleep(0.6)
        await producer.append("c")
        await producer.append("d")
        plugin_of(streams)._batches.clear()
        # The read began at the stream's start, so it has no cursor to find
        # gone. The replay still fails for a retry, not as nondeterminism.
        with pytest.raises(StreamReplayRangeLostError):
            await codec_of(streams).decode([payload])
        await streams.get_workflow_handle(owner).signal(Waits.finish)


async def test_the_codec_passes_other_payloads_through(client: Client):
    streams = await connect_with(client, MemoryStreams())
    [payload] = DataConverter.default.payload_converter.to_payloads(["plain"])
    assert await codec_of(streams).decode([payload]) == [payload]
    assert await codec_of(streams).encode([payload]) == [payload]


async def test_replay_reads_an_activity_stream_with_the_activitys_context(
    client: Client,
):
    keyed = await connect_with(
        client, MemoryStreams(), DataConverter(payload_codec=ActivityKeyedCodec())
    )
    activity_id = f"replay-reads-{uuid.uuid4().hex}"
    async with new_worker(keyed, activities=[owns_a_stream]) as worker:
        owner = await keyed.start_activity(
            owns_a_stream,
            ["a", "b"],
            id=activity_id,
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=10),
        )
        run_id = await owner.result()
    env = ActivityEnvironment(client=keyed)
    env.info = dataclasses.replace(
        env.info, workflow_run_id=f"reader-{uuid.uuid4()}", is_local=True
    )
    raw = await env.run(
        read_activity,
        _ReadInput(
            workflow_id="",
            owner_kind="activity",
            activity_id=activity_id,
            chain_run_id=run_id,
            topic=TOKENS.name,
            after="",
            state="",
            wait_ms=100,
            max_records=10,
            positions_only=True,
            reader_id="1",
        ),
    )
    plugin_of(keyed)._batches.clear()
    # The codec keys each body by the Activity, so only the Activity's context
    # decodes what the replay reads again.
    [replayed] = await codec_of(keyed).decode([raw.payload])
    assert values(replayed) == ["a", "b"]
