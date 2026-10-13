"""The publish sample runs end to end on Redis and the dev server."""

from __future__ import annotations

import os
import uuid

import pytest

from temporalio.client import Client
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with
from tests.contrib.streams.samples import path_b_activity_publish

pytestmark = pytest.mark.skipif(
    not os.environ.get("STREAMS_REDIS_URL"),
    reason="set STREAMS_REDIS_URL to run the stream samples",
)


async def test_path_b_a_reader_drops_what_a_failed_attempt_wrote(client: Client):
    store = RedisStreams(
        os.environ["STREAMS_REDIS_URL"], key_prefix=f"sample-{uuid.uuid4().hex}"
    )
    text = await path_b_activity_publish.main(
        await connect_with(client, store),
        f"sample-b-{uuid.uuid4().hex}",
        workflow_id=f"answer-{uuid.uuid4().hex}",
    )
    assert text == "an answer to streams"
