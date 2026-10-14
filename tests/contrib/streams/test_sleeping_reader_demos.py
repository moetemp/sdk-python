"""The demos of readers that sleep run end to end on a server with the stream notifier.

They skip on the stock dev server, which has no notifier, and the live lane
gates on them. Each demo runs on memory, and on Redis when
``STREAMS_REDIS_URL`` is set.
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
    j1_sleeping_client,
    j2_slack_connector,
    j3_ci_tail,
)
from tests.contrib.streams.test_live_notifications import skip_without_notifier


def redis_store() -> StreamStorePlugin:
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run the demos on Redis")
    return RedisStreams(url, key_prefix=f"scenario-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis"])
async def streams(request: pytest.FixtureRequest, client: Client) -> Client:
    await skip_without_notifier(client)
    store = MemoryStreams() if request.param == "memory" else redis_store()
    return await connect_with(client, store.notify_on_append())


def ids(name: str) -> tuple[str, str]:
    suffix = uuid.uuid4().hex
    return f"{name}-{suffix}", f"{name}-wf-{suffix}"


async def test_j1_a_client_wakes_on_each_burst_and_ends_at_the_close(
    streams: Client,
):
    task_queue, workflow_id = ids("j1")
    read = await j1_sleeping_client.main(streams, task_queue, workflow_id=workflow_id)
    assert read == [f"burst {b} update {n}" for b in range(3) for n in range(2)]


async def test_j2_a_connector_posts_every_message_once_in_order(streams: Client):
    task_queue, workflow_id = ids("j2")
    posted = await j2_slack_connector.main(streams, task_queue, workflow_id=workflow_id)
    assert posted == ["ana: hello", "bo: hi ana", "ana: lunch?"]


async def test_j3_a_tail_reads_every_line_of_a_sparse_log(streams: Client):
    task_queue, workflow_id = ids("j3")
    lines = await j3_ci_tail.main(streams, task_queue, workflow_id=workflow_id)
    assert lines == ["checkout: ok", "compile: ok", "test: ok"]
