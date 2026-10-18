"""An archive behind a Redis or Postgres store keeps the records it trimmed readable.

A StreamDriver fills the archive, and it runs only on a server with the
stream notifier, so the live cases run in the Jetty lane. Each live case
runs on a local directory and on the S3 server that
``STREAMS_ARCHIVE_S3_URL`` names, with the ``AWS_*`` settings Core reads.
The lane starts MinIO in Docker for it. The Postgres cases need
``STREAMS_POSTGRES_URL``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    Cursor,
    StreamDriver,
    StreamExpiredError,
    StreamProducer,
    StreamStorePlugin,
    get_stream_handle,
)
from temporalio.contrib.streams.archive import StreamArchive
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.worker import Replayer
from tests.contrib.streams._support import (
    connect_with,
    new_schema,
    postgres_streams,
)
from tests.contrib.streams.test_live_notifications import (
    TOKENS,
    Reads,
    skip_without_notifier,
)
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

# The store trims a record this long after it was written, once nothing holds it.
RETENTION = timedelta(milliseconds=500)


def test_the_archive_rides_the_redis_config(tmp_path: Path):
    store = RedisStreams(
        "redis://x",
        archive=StreamArchive(
            tmp_path,
            segment_bytes=1024,
            segment_idle=timedelta(seconds=2),
            stall_limit=timedelta(hours=1),
        ),
    )
    archive = store._config.archive
    assert archive.url == tmp_path.resolve().as_uri()
    assert archive.segment_bytes == 1024
    assert archive.segment_idle.ToTimedelta() == timedelta(seconds=2)
    assert archive.stall_limit.ToTimedelta() == timedelta(hours=1)
    # Unset settings leave Core's defaults.
    plain = StreamArchive("s3://bucket/prefix")._config
    assert plain.url == "s3://bucket/prefix"
    assert not plain.segment_bytes and not plain.HasField("segment_idle")
    assert not RedisStreams("redis://x")._config.HasField("archive")


def test_an_archive_refuses_what_core_cannot_open():
    assert StreamArchive("file:///tmp/a").url == "file:///tmp/a"
    with pytest.raises(ValueError, match="s3:// or file://"):
        StreamArchive("gs://bucket")
    with pytest.raises(ValueError, match="location"):
        StreamArchive("")
    with pytest.raises(ValueError, match="segment_bytes"):
        StreamArchive("s3://b", segment_bytes=0)
    with pytest.raises(ValueError, match="segment_idle"):
        StreamArchive("s3://b", segment_idle=timedelta(0))
    with pytest.raises(ValueError, match="stall_limit"):
        StreamArchive("s3://b", stall_limit=timedelta(seconds=-1))


@dataclass
class Tiers:
    """A client whose store archives, with a driver running, and one on the same keys without."""

    archived: Client
    hot_only: Client
    store: StreamStorePlugin
    plain: StreamStorePlugin
    directory: Path | None


def archive_for(kind: str, tmp_path: Path) -> StreamArchive:
    settings: dict[str, Any] = {
        "segment_idle": timedelta(milliseconds=200),
        "stall_limit": timedelta(minutes=5),
    }
    if kind == "local":
        return StreamArchive(tmp_path / "archive", **settings)
    url = os.environ.get("STREAMS_ARCHIVE_S3_URL", "")
    if not url:
        pytest.skip("set STREAMS_ARCHIVE_S3_URL to run on S3")
    # Core reads the endpoint and the keys from the AWS_* environment.
    return StreamArchive(f"{url}/{uuid.uuid4().hex}", **settings)


@pytest.fixture(
    params=[
        ("redis", "local"),
        ("redis", "s3"),
        ("postgres", "local"),
        ("postgres", "s3"),
    ],
    ids=lambda p: "-".join(p),
)
async def tiers(
    request: pytest.FixtureRequest, client: Client, tmp_path: Path
) -> AsyncIterator[Tiers]:
    kind, where = request.param
    # A Workflow reader's hold lapses at once, so only the archive holds records.
    settings: dict[str, Any] = {
        "retention": RETENTION,
        "floor_lease": timedelta(milliseconds=100),
        "floor_release_grace": timedelta(milliseconds=100),
    }
    if kind == "postgres":
        # The janitor trims, so it runs often here.
        settings["janitor_interval"] = timedelta(milliseconds=100)
        schema = new_schema()

        def hot(archive: StreamArchive | None) -> StreamStorePlugin:
            return postgres_streams(schema, archive=archive, **settings)

    else:
        url = os.environ.get("STREAMS_REDIS_URL", "")
        if not url:
            pytest.skip("set STREAMS_REDIS_URL to run on Redis")
        settings["key_prefix"] = f"archive-{uuid.uuid4().hex}"

        def hot(archive: StreamArchive | None) -> StreamStorePlugin:
            return RedisStreams(url, archive=archive, **settings)

    await skip_without_notifier(client)
    archive = archive_for(where, tmp_path)
    store = hot(archive).notify_on_append(notification_log=True)
    plain = hot(None).notify_on_append(notification_log=True)
    archived = await connect_with(client, store)
    hot_only = await connect_with(client, plain)
    driver = asyncio.create_task(
        StreamDriver(archived, sweep_interval=timedelta(seconds=1)).run()
    )
    try:
        yield Tiers(
            archived,
            hot_only,
            store,
            plain,
            tmp_path / "archive" if where == "local" else None,
        )
    finally:
        driver.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await driver


async def trim_past(tiers: Tiers, owner: str, after: Cursor, producer: StreamProducer):
    """Append until the hot store alone no longer holds the record after ``after``."""
    deadline = time.monotonic() + 60
    filler = 0
    while True:
        records = get_stream_handle(tiers.hot_only, owner, topic=TOKENS).read(
            after=after
        )
        try:
            await asyncio.wait_for(records.__anext__(), 5)
        except StreamExpiredError:
            return
        finally:
            await records.aclose()
        if time.monotonic() > deadline:
            raise AssertionError("the store did not trim the archived records")
        # Redis trims on appends past the retention, and Postgres in its janitor.
        await asyncio.sleep(RETENTION.total_seconds())
        filler += 1
        await producer.append(f"filler-{filler}")


async def test_live_a_cursor_below_the_hot_floor_reads_the_archive(tiers: Tiers):
    archived = tiers.archived
    async with new_worker(archived, Waits) as worker:
        owner = await running_owner(archived, worker.task_queue)
        handle = get_stream_handle(archived, owner, topic=TOKENS)
        producer = handle.producer(producer_id="p", attempt=1)
        await producer.append("a", "b", "c")
        records = handle.read()
        first = await records.__anext__()
        await records.aclose()
        assert first.value == "a"
        await trim_past(tiers, owner, first.cursor, producer)
        # The same read, with no change, reads on from the archive into the store.
        seen = []
        records = handle.read(after=first.cursor)
        async for record in records:
            seen.append(record.value)
            if str(record.value).startswith("filler-"):
                break
        await records.aclose()
        assert seen == ["b", "c", "filler-1"]
        # From the start, the read finds the archived records too.
        records = handle.read(after=BEGINNING)
        assert (await records.__anext__()).value == "a"
        await records.aclose()
        if tiers.directory is not None:
            assert list(tiers.directory.rglob("seg-*.tsr"))
        await archived.get_workflow_handle(owner).signal(Waits.finish)


async def test_live_a_positions_only_replay_reads_a_trimmed_range_from_the_archive(
    tiers: Tiers,
):
    archived = tiers.archived
    async with new_worker(archived, Waits, Reads, max_cached_workflows=0) as worker:
        owner = await running_owner(archived, worker.task_queue)
        handle = get_stream_handle(archived, owner, topic=TOKENS)
        producer = handle.producer(producer_id="p", attempt=1)
        await producer.append("a", "b")
        reader = await archived.start_workflow(
            Reads.run,
            args=[owner, 2],
            id=f"reader-{uuid.uuid4().hex}",
            task_queue=worker.task_queue,
        )
        assert await asyncio.wait_for(reader.result(), 30) == ["a", "b"]
        history = await reader.fetch_history()
        records = handle.read()
        first = await records.__anext__()
        await records.aclose()
        await trim_past(tiers, owner, first.cursor, producer)
        # The hot store alone lost the range the reader recorded.
        tiers.plain._batches.clear()
        with pytest.raises(Exception) as raised:
            await Replayer(workflows=[Reads], plugins=[tiers.plain]).replay_workflow(
                history
            )
        assert "StreamReplayRangeLostError" in str(raised.value)
        # The archive gives the same records, with the same digest.
        tiers.store._batches.clear()
        await Replayer(workflows=[Reads], plugins=[tiers.store]).replay_workflow(
            history
        )
        await archived.get_workflow_handle(owner).signal(Waits.finish)
