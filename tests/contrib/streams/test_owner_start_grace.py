"""A read can start before its owner Workflow does.

Until ``owner_start_grace`` runs out, the store answers a read of a Workflow that
Temporal doesn't know yet with an empty batch, so the read waits. After the
grace the read fails with ``StreamNotFoundError``.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from datetime import timedelta

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamNotFoundError,
    StreamStorePlugin,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with, new_workflow_id
from tests.helpers import new_worker

STEPS = topic("steps", str)


def store(kind: str, grace: timedelta) -> StreamStorePlugin:
    if kind == "memory":
        return MemoryStreams(owner_start_grace=grace)
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return RedisStreams(
        url, key_prefix=f"grace-{uuid.uuid4().hex}", owner_start_grace=grace
    )


@workflow.defn
class PublishesOnce:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(STEPS).publish("first")


@pytest.mark.parametrize("kind", ["memory", "redis"])
async def test_a_read_that_starts_first_gets_the_owners_first_record(
    client: Client, kind: str
):
    streams = await connect_with(client, store(kind, timedelta(seconds=30)))
    workflow_id = new_workflow_id()
    read = get_stream_handle(streams, workflow_id).read(topic=STEPS)
    first = asyncio.create_task(anext(read))
    await asyncio.sleep(5)
    # The read is still waiting, not failed.
    assert not first.done()
    async with new_worker(streams, PublishesOnce) as worker:
        await streams.execute_workflow(
            PublishesOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
        record = await asyncio.wait_for(first, 20)
    await read.aclose()
    assert record.value == "first"


@pytest.mark.parametrize("kind", ["memory", "redis"])
async def test_a_read_of_an_owner_that_never_starts_fails_after_the_grace(
    client: Client, kind: str
):
    streams = await connect_with(client, store(kind, timedelta(seconds=2)))
    read = get_stream_handle(streams, new_workflow_id()).read(topic=STEPS)
    started = time.monotonic()
    with pytest.raises(StreamNotFoundError):
        await asyncio.wait_for(anext(read), 30)
    assert time.monotonic() - started >= 2


async def test_a_zero_grace_fails_at_once(client: Client):
    streams = await connect_with(client, MemoryStreams(owner_start_grace=timedelta(0)))
    read = get_stream_handle(streams, new_workflow_id()).read(topic=STEPS)
    started = time.monotonic()
    with pytest.raises(StreamNotFoundError):
        await asyncio.wait_for(anext(read), 10)
    assert time.monotonic() - started < 2


def test_the_grace_rides_the_store_config_and_refuses_a_negative_value():
    assert not MemoryStreams()._config.HasField("owner_start_grace")
    config = RedisStreams("redis://x", owner_start_grace=timedelta(seconds=5))._config
    assert config.owner_start_grace.ToTimedelta() == timedelta(seconds=5)
    for make in (
        lambda: MemoryStreams(owner_start_grace=timedelta(seconds=-1)),
        lambda: RedisStreams("redis://x", owner_start_grace=timedelta(seconds=-1)),
    ):
        with pytest.raises(ValueError, match="owner_start_grace"):
            make()
