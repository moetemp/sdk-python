"""The Bridge scenario demos run end to end on the live lane's server.

They need a server with the stream notifier and Nexus operation progress, as
``scripts/streams_live_lane.sh`` builds, and the cross-namespace demos need
the namespace ``streams-b``, which the lane registers. A server without them
skips the tests, and the lane's gate turns that skip into a failure. Each
demo runs on memory, and again on Redis when ``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable

import pytest

from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import DescribeStreamNotifierRequest
from temporalio.client import Client
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.service import RPCError, RPCStatusCode
from tests.contrib.streams.samples.scenarios import (
    n1b_parent_relay,
    n1c_cross_namespace_relay,
    n2_dashcam_relay,
    n3d_inner_loop,
    n4_slack_connector,
    n5b_dashboard_and_relay,
    n5c_late_attach,
    n6_location_operation,
    n7a_failed_delivery,
    n7b_reader_marks,
    n8_replay_after_trim,
    s6_location_topic,
)
from tests.contrib.streams.samples.scenarios._nexus import NAMESPACE_B, has_namespace


def redis_store() -> StreamStorePlugin:
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run the demos on Redis")
    return RedisStreams(url, key_prefix=f"scenario-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis"])
def new_store(request: pytest.FixtureRequest) -> Callable[[], StreamStorePlugin]:
    if request.param == "redis":
        redis_store()
        return redis_store
    return MemoryStreams


@pytest.fixture
async def lane(client: Client) -> Client:
    """``client``, once the server shows it has the stream notifier."""
    try:
        await client.workflow_service.describe_stream_notifier(
            DescribeStreamNotifierRequest(
                namespace=client.namespace,
                stream_ref=StreamReference(
                    owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                    workflow_id=f"probe-{uuid.uuid4()}",
                    topic="probe",
                ),
            )
        )
    except RPCError as err:
        if err.status == RPCStatusCode.UNIMPLEMENTED:
            pytest.skip(f"server has no stream notifier: {err.message}")
        if err.status not in (RPCStatusCode.NOT_FOUND, RPCStatusCode.INVALID_ARGUMENT):
            raise
    return client


@pytest.fixture
async def two_namespaces(lane: Client) -> Client:
    """``lane``, once the server shows it has the namespace ``streams-b``."""
    if not await has_namespace(lane, NAMESPACE_B):
        pytest.skip(f"server has no stream notifier namespace {NAMESPACE_B}")
    return lane


def ids(name: str) -> tuple[str, str]:
    # Ids of their own, so runs that share a server never collide.
    suffix = uuid.uuid4().hex
    return f"{name}-{suffix}", f"{name}-wf-{suffix}"


async def test_n1b_a_parent_relays_every_record_and_gets_the_close_result(
    lane: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n1b")
    outcome = await n1b_parent_relay.main(
        lane, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    assert outcome.relayed == [
        f"delivery {stage}" for stage in n1b_parent_relay.DELIVERY_STAGES
    ]
    assert outcome.close_result == "4 stages"


async def test_n5b_a_dashboard_and_a_relay_both_get_every_record(
    lane: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n5b")
    outcome = await n5b_dashboard_and_relay.main(
        lane, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    assert outcome.dashboard == n1b_parent_relay.DELIVERY_STAGES
    assert outcome.relayed == [
        f"delivery {stage}" for stage in n1b_parent_relay.DELIVERY_STAGES
    ]
    assert outcome.close_result == "4 stages"


async def test_n1c_a_relay_across_namespaces_gets_every_record(
    two_namespaces: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n1c")
    outcome = await n1c_cross_namespace_relay.main(
        two_namespaces, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    assert outcome.relayed == [
        f"delivery {stage}" for stage in n1b_parent_relay.DELIVERY_STAGES
    ]
    assert outcome.close_result == "4 stages"
    assert outcome.delivery_namespace == NAMESPACE_B


async def test_n2_every_segment_url_crosses_namespaces_and_resolves(
    two_namespaces: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n2")
    fetched = await n2_dashcam_relay.main(
        two_namespaces, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    assert fetched == [b"segment 0", b"segment 1", b"segment 2"]


@pytest.mark.parametrize("b_store", ["memory", "redis"], ids=["3d", "3e"])
async def test_n3d_an_agent_hears_every_message_whatever_bs_store(
    two_namespaces: Client, b_store: str
):
    # 3d keeps B's stream in memory and 3e in Redis. The agent hears the same.
    task_queue, workflow_id = ids("n3d")
    heard = await n3d_inner_loop.main(
        two_namespaces,
        task_queue,
        workflow_id=workflow_id,
        new_store=MemoryStreams if b_store == "memory" else redis_store,
    )
    assert heard.messages == n3d_inner_loop.MESSAGES
    assert heard.close_result == "answered 'where is my order'"


async def test_n4_a_connector_posts_every_message_and_idles_with_no_task(
    two_namespaces: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n4")
    outcome = await n4_slack_connector.main(
        two_namespaces, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    assert outcome.posts == ["the build is green", "the release is out"]
    assert outcome.close_result == "2 messages"
    # The agent idles 10 s, and the connector has no Workflow Task meanwhile.
    assert outcome.longest_quiet_seconds >= 8, outcome.longest_quiet_seconds


async def test_n5c_followers_that_attach_late_get_every_record(
    lane: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n5c")
    outcome = await n5c_late_attach.main(
        lane, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    stages = n5c_late_attach.FIRST + n5c_late_attach.LAST
    for followed in (outcome.early, outcome.late, outcome.after_close):
        assert followed.stages == stages
        assert followed.close_result == "6 stages"
    # The notifier answers an attach after the close at once.
    assert outcome.after_close_seconds < 10, outcome.after_close_seconds


async def test_n6_a_second_operation_follows_the_location_after_the_wake(
    two_namespaces: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n6")
    outcome = await n6_location_operation.main(
        two_namespaces, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    assert outcome.tracked.events == ["picked up", "tracking started", "arrived"]
    assert outcome.tracked.positions == s6_location_topic.POSITIONS
    # At least one read per topic, each one a Nexus operation in History.
    assert outcome.reads >= 2, outcome.reads


@pytest.mark.parametrize(
    "how, failure",
    [
        ("fail", ["ApplicationError", "CourierLost"]),
        ("close-with-error", ["ApplicationError", "CourierLost"]),
        ("terminate", ["TerminatedError", ""]),
    ],
)
async def test_n7a_a_caller_reads_every_stage_then_gets_the_failure(
    lane: Client,
    new_store: Callable[[], StreamStorePlugin],
    how: str,
    failure: list[str],
):
    task_queue, workflow_id = ids("n7a")
    dispatched = await n7a_failed_delivery.main(
        lane, task_queue, workflow_id=workflow_id, new_store=new_store, how=how
    )
    assert dispatched.stages == ["assigned", "picked up"]
    assert dispatched.failure == failure


async def test_n7b_the_reader_marks_the_retry_and_the_stale_write(
    lane: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n7b")
    watched = await n7b_reader_marks.main(
        lane, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    assert watched.positions == ["attempt 1 at the depot", "attempt 2 at the depot"]
    assert watched.supersessions == ["1 -> 2"]
    assert watched.stale == ["attempt 1 late"]
    assert watched.close_result == "attempt 2"


async def test_n8_a_caller_replays_with_the_owners_streams_and_worker_gone(
    lane: Client, new_store: Callable[[], StreamStorePlugin]
):
    task_queue, workflow_id = ids("n8")
    outcome = await n8_replay_after_trim.main(
        lane, task_queue, workflow_id=workflow_id, new_store=new_store
    )
    assert outcome.first_run_entries == ["one", "two"]
    assert outcome.books.entries == ["one", "two", "three"]
    assert outcome.books.close_result == "3 entries"
    assert outcome.keys_deleted > 0
    assert outcome.runs_replayed == 2
