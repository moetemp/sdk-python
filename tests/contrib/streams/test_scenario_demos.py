"""The Brook scenario demos run end to end on the dev server.

Each demo runs on memory, and on Redis when ``STREAMS_REDIS_URL`` is set.
The demos live in ``samples/scenarios``, and their README maps them to the
"Nexus streaming scenarios" page.
"""

from __future__ import annotations

import os
import uuid

import pytest

from temporalio.client import Client
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with
from tests.contrib.streams.samples.scenarios import (
    s2_dashcam_reference,
    s7b_retried_courier,
)


def redis_store() -> StreamStorePlugin:
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run the demos on Redis")
    return RedisStreams(url, key_prefix=f"scenario-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis"])
async def streams(request: pytest.FixtureRequest, client: Client) -> Client:
    store = MemoryStreams() if request.param == "memory" else redis_store()
    return await connect_with(client, store)


def ids(name: str) -> tuple[str, str]:
    # Ids of their own, so runs that share a dev server never collide.
    suffix = uuid.uuid4().hex
    return f"{name}-{suffix}", f"{name}-wf-{suffix}"


async def test_s2_every_segment_url_resolves_in_order(streams: Client):
    task_queue, workflow_id = ids("s2")
    fetched = await s2_dashcam_reference.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert fetched == [b"segment 0", b"segment 1", b"segment 2"]


async def test_s7b_a_retry_is_superseded_and_the_late_write_is_stale(streams: Client):
    task_queue, workflow_id = ids("s7b")
    labels = await s7b_retried_courier.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert labels == [
        "attempt 1 at the depot@1",
        "SUPERSEDED",
        "attempt 2 at the depot@2",
        "attempt 1 late@1 stale",
    ]
