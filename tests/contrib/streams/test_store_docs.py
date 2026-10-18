"""The store table in the capabilities page says what Core reports.

Redis rows run when ``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

import temporalio.bridge.proto.streams
from temporalio.client import Client
from temporalio.contrib.streams import StoreCapabilities, StreamStorePlugin
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with

_DOC = (
    Path(__file__).parents[3] / "temporalio/contrib/streams/docs/store-capabilities.md"
)


def _table() -> dict[str, dict[str, str]]:
    lines = _DOC.read_text().split("## The stores", 1)[1].splitlines()
    rows = [line.strip("|").split("|") for line in lines if line.startswith("| `")]
    header = next(line for line in lines if line.startswith("| Store"))
    columns = [c.strip() for c in header.strip("|").split("|")][1:]
    return {
        row[0].strip(): dict(zip(columns, (c.strip() for c in row[1:]))) for row in rows
    }


def _offered(capabilities: StoreCapabilities) -> dict[str, str]:
    def yn(value: bool) -> str:
        return "Y" if value else "N"

    return {
        "Producer state": yn(capabilities.producer_state),
        "Conditional": yn(
            capabilities.conditional_tail or capabilities.conditional_fence
        ),
        "Kept last": yn(capabilities.kept_last),
        "Holds": yn(capabilities.holds),
        "Change feed": yn(capabilities.change_feed),
        "Cheap logs": yn(capabilities.cheap_logs),
        "Delete": yn(capabilities.delete_owner),
        "Listing": yn(capabilities.topic_listing),
        "Long poll": yn(capabilities.long_poll),
    }


def _store(row: str) -> StreamStorePlugin:
    if row.startswith("`MemoryStreams"):
        return MemoryStreams()
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to check the Redis rows")
    prefix = f"store-docs-{uuid.uuid4().hex}"
    return RedisStreams(url, key_prefix=prefix, plain="plain=True" in row)


@pytest.mark.skipif(
    not hasattr(temporalio.bridge.proto.streams, "DescribeStoreRequest"),
    reason="this Core can't describe a store",
)
@pytest.mark.parametrize("row", list(_table()))
async def test_the_store_table_matches_core(client: Client, row: str):
    store = _store(row)
    await connect_with(client, store)
    assert _table()[row] == _offered(store.capabilities)
