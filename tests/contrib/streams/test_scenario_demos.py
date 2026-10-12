"""The Brook scenario demos run end to end on the dev server.

Each demo runs on memory, and on Redis when ``STREAMS_REDIS_URL`` is set.
The demos live in ``samples/scenarios``, and their README maps them to the
"Nexus streaming scenarios" page.
"""

from __future__ import annotations

import os
import uuid

import pytest

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with
from tests.contrib.streams.samples.scenarios import (
    s1a_order_tracking,
    s2_dashcam_reference,
    s3a_token_deltas,
    s3b_redis_turns,
    s3c_two_topics,
    s5a_late_dashboard,
    s5b_child_dashboard,
    s6_location_topic,
    s7a_failed_delivery,
    s7b_retried_courier,
    s8_publish_replay,
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


async def test_s1a_a_backend_sees_every_stage_in_order_then_the_end(streams: Client):
    task_queue, workflow_id = ids("s1a")
    seen = await s1a_order_tracking.main(streams, task_queue, workflow_id=workflow_id)
    assert seen == [
        ("workflow", "placed"),
        ("restaurant", "accepted"),
        ("restaurant", "cooking"),
        ("restaurant", "ready"),
        ("courier", "picked up"),
        ("courier", "delivered"),
        ("workflow", "FINISH"),
    ]


async def test_s2_every_segment_url_resolves_in_order(streams: Client):
    task_queue, workflow_id = ids("s2")
    fetched = await s2_dashcam_reference.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert fetched == [b"segment 0", b"segment 1", b"segment 2"]


async def test_s3a_the_text_matches_and_the_turn_is_visible_at_task_acceptance(
    streams: Client,
):
    task_queue, workflow_id = ids("s3a")
    seen = await s3a_token_deltas.main(streams, task_queue, workflow_id=workflow_id)
    assert seen.turns == ["turn started", "turn finished"]
    assert seen.text == seen.answer.text == "Here is an answer to streams"
    assert seen.answer.saw_turn_started


async def test_s3b_a_retry_and_a_failed_turn_read_in_one_order(client: Client):
    task_queue, workflow_id = ids("s3b")
    labels = await s3b_redis_turns.main(
        await connect_with(client, redis_store()), task_queue, workflow_id=workflow_id
    )
    assert labels == [
        "turn 1 started",
        "Hel@1",
        "SUPERSEDED",
        "Hel@2",
        "lo@2",
        "turn 1 finished",
        "turn 2 started",
        "Wor@1",
        "SUPERSEDED",
        "Wor@2",
        "turn 2 failed",
    ]


async def test_s3c_a_failed_last_attempt_leaves_its_deltas(streams: Client):
    task_queue, workflow_id = ids("s3c")
    seen = await s3c_two_topics.main(streams, task_queue, workflow_id=workflow_id)
    assert seen.turns == ["turn started", "turn failed"]
    assert seen.deltas == ["Hel@1", "SUPERSEDED", "Hel@2", "l@2"]


async def test_s5a_a_late_dashboard_reads_what_the_app_read(streams: Client):
    task_queue, workflow_id = ids("s5a")
    seen = await s5a_late_dashboard.main(streams, task_queue, workflow_id=workflow_id)
    expected = [
        "opened",
        "assigned",
        "investigating",
        "fix found",
        "fix shipped",
        "closed",
    ]
    assert seen.app == expected
    assert seen.dashboard == expected


async def test_s5b_a_dashboard_reads_every_record_of_the_child(streams: Client):
    task_queue, workflow_id = ids("s5b")
    stages = await s5b_child_dashboard.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert stages == ["assigned", "picked up", "on the way", "delivered"]


async def test_s6_positions_follow_the_wake_record(streams: Client):
    task_queue, workflow_id = ids("s6")
    seen = await s6_location_topic.main(streams, task_queue, workflow_id=workflow_id)
    assert seen.events == ["picked up", "tracking started", "arrived"]
    assert seen.events[seen.events_before_location - 1] == "tracking started"
    assert seen.positions == s6_location_topic.POSITIONS


async def test_s7a_the_read_ends_and_the_backend_learns_the_failure(streams: Client):
    task_queue, workflow_id = ids("s7a")
    outcome = await s7a_failed_delivery.main(
        streams, task_queue, workflow_id=workflow_id
    )
    assert outcome.stages == ["assigned", "picked up"]
    assert outcome.status == WorkflowExecutionStatus.FAILED
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


async def test_s8_a_publisher_replays_with_its_streams_deleted(streams: Client):
    task_queue, workflow_id = ids("s8")
    outcome = await s8_publish_replay.main(streams, task_queue, workflow_id=workflow_id)
    assert outcome.before_delete == ["one", "two"]
    assert outcome.keys_deleted > 0
    assert outcome.after_replay == []
