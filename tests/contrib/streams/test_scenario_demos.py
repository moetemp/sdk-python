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
    s1a_order_tracking,
    s2_dashcam_reference,
    s3_chat_turn,
    s5a_late_dashboard,
    s6_location_topic,
    s7a_failed_delivery,
    s7b_retried_courier,
    s8_publisher_replay,
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


async def test_s1a_the_app_shows_the_workflow_and_courier_stages_in_order(
    streams: Client,
):
    task_queue, workflow_id = ids("s1a")
    stages = await s1a_order_tracking.main(streams, task_queue, workflow_id=workflow_id)
    assert stages == ["placed", "picked up", "out for delivery", "delivered"]


async def test_s2_every_segment_url_resolves_in_order(streams: Client):
    task_queue, workflow_id = ids("s2")
    fetched = await s2_dashcam_reference.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert fetched == [b"segment 0", b"segment 1", b"segment 2"]


async def test_s3_a_retry_takes_back_only_the_partial_answer(streams: Client):
    task_queue, workflow_id = ids("s3")
    outcome = await s3_chat_turn.main(streams, task_queue, workflow_id=workflow_id)
    assert outcome.turns == ["turn started", "turn ended"]
    assert outcome.shown == s3_chat_turn.ANSWER
    assert outcome.withdrawn == s3_chat_turn.ANSWER[:2]


async def test_s5a_a_late_dashboard_reads_from_the_start_and_a_live_view_from_the_end(
    streams: Client,
):
    task_queue, workflow_id = ids("s5a")
    outcome = await s5a_late_dashboard.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert outcome.dashboard == [
        "picked up",
        "at the hub",
        "out for delivery",
        "delivered",
    ]
    assert outcome.live == ["out for delivery", "delivered"]


async def test_s6_a_record_opens_a_second_read_from_the_start(streams: Client):
    task_queue, workflow_id = ids("s6")
    outcome = await s6_location_topic.main(streams, task_queue, workflow_id=workflow_id)
    assert outcome.events == ["picked up", "tracking started", "delivered"]
    assert outcome.locations == ["52.37,4.89", "52.36,4.90", "52.35,4.91"]


async def test_s7a_the_read_ends_and_the_workflow_gives_the_failure(streams: Client):
    task_queue, workflow_id = ids("s7a")
    outcome = await s7a_failed_delivery.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert outcome.stages == ["picked up", "at the hub"]
    assert outcome.failure_type == "CourierLost"


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


async def test_s8_a_publisher_replays_with_no_store(streams: Client):
    task_queue, workflow_id = ids("s8")
    outcome = await s8_publisher_replay.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert outcome.stages == ["placed", "packed", "shipped"]
    # One marker for each flush, and at least two flushes.
    assert outcome.markers >= 2
