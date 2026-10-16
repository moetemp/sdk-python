"""What the scenario demos share: the store, a printing backend and the command line."""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    StreamRecord,
    StreamStorePlugin,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams

#: How long a backend follows a stream before it gives up, so a demo never hangs.
FOLLOW_TIMEOUT = 30.0


def store(*, notify: bool = False) -> StreamStorePlugin:
    """Redis when ``STREAMS_REDIS_URL`` is set, else this process's memory.

    Each Redis store gets a key prefix of its own, so demos that share a
    Redis never see each other's records. ``notify`` turns on the
    notifications that wake a sleeping reader.
    """
    url = os.environ.get("STREAMS_REDIS_URL")
    plugin: StreamStorePlugin = (
        RedisStreams(url, key_prefix=f"scenario-{uuid.uuid4().hex}")
        if url
        else MemoryStreams()
    )
    return plugin.notify_on_append() if notify else plugin


def store_of(client: Client) -> StreamStorePlugin:
    """The stream store ``client`` carries."""
    for plugin in client.config().get("plugins") or []:
        if isinstance(plugin, StreamStorePlugin):
            return plugin
    raise ValueError("the client carries no stream store")


def describe(record: StreamRecord[Any]) -> str:
    """One line for a record, as a backend logs it."""
    who = record.producer_id or "workflow"
    if record.kind is RecordKind.SUPERSEDED and record.supersession is not None:
        change = record.supersession
        return (
            f"SUPERSEDED {change.producer_id} attempt {change.previous_attempt} "
            f"-> {change.attempt}"
        )
    if record.kind is RecordKind.FINISH:
        return f"FINISH {who}"
    stale = " (stale)" if record.stale else ""
    attempt = f" attempt {record.attempt}" if record.producer_id else ""
    return f"{who}{attempt}: {record.value!r}{stale}"


async def follow(
    name: str,
    records: AsyncGenerator[StreamRecord[Any], None],
    seen: list[StreamRecord[Any]],
    *,
    on_record: Callable[[StreamRecord[Any]], Awaitable[None] | None] | None = None,
    timeout: float = FOLLOW_TIMEOUT,
) -> list[StreamRecord[Any]]:
    """Print and keep each record until the read ends, as a backend does.

    The read ends when the owner's run chain closes and every record was
    delivered.
    """

    async def pull() -> None:
        async for record in records:
            print(f"[{name}] {describe(record)}")
            seen.append(record)
            if on_record is not None:
                outcome = on_record(record)
                if outcome is not None:
                    await outcome

    try:
        await asyncio.wait_for(pull(), timeout)
    finally:
        await records.aclose()
    print(f"[{name}] the read ended")
    return seen


def label(record: StreamRecord[Any]) -> str:
    """A short label for a record, for a demo's result.

    A producer's record carries its attempt, and the owner's carries none.
    """
    if record.kind is RecordKind.SUPERSEDED:
        return "SUPERSEDED"
    if record.kind is RecordKind.FINISH:
        return "FINISH"
    text = str(record.value)
    if record.producer_id:
        text = f"{text}@{record.attempt}"
    return f"{text} stale" if record.stale else text


def values(records: list[StreamRecord[Any]]) -> list[Any]:
    """The values of the data records, in order."""
    return [r.value for r in records if r.kind is RecordKind.DATA]


def run(main: Callable[..., Awaitable[Any]], *, notify: bool = False) -> None:
    """Run a demo's ``main`` against a dev server, from the command line.

    ``TEMPORAL_ADDRESS`` names the server (``localhost:7233`` by default),
    and ``STREAMS_REDIS_URL`` picks Redis over memory. ``notify`` is for the
    demos of readers that sleep, which need a server with the stream
    notifier.
    """

    async def go() -> None:
        client = await Client.connect(
            os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
            plugins=[store(notify=notify)],
        )
        name = os.path.basename(sys.argv[0]).removesuffix(".py")
        result = await main(
            client,
            f"{name}-{uuid.uuid4().hex}",
            workflow_id=f"{name}-{uuid.uuid4().hex}",
        )
        print(f"result: {result!r}")

    asyncio.run(go())
