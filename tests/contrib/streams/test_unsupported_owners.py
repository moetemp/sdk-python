"""Owners and readers this release does not support say so.

Only a Workflow owns a stream in this release. Every other owner raises
``StreamUnsupportedError``.
"""

from __future__ import annotations

import dataclasses

import pytest

from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRef,
    StreamUnsupportedError,
    activity_handle,
    get_stream_handle,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.testing import ActivityEnvironment
from tests.contrib.streams._support import connect_with


async def test_a_ref_refuses_owner_kinds_this_release_lacks_where_it_opens(
    client: Client,
):
    streams_client = await connect_with(client, MemoryStreams())
    for kind in ("activity", "standalone"):
        ref = StreamRef(kind, "x")  # type: ignore[arg-type]
        with pytest.raises(StreamUnsupportedError, match="only Workflow-owned"):
            get_stream_handle(streams_client, ref)


async def test_an_activity_with_no_workflow_has_no_stream():
    env = ActivityEnvironment()
    env.info = dataclasses.replace(
        ActivityEnvironment.default_info(), workflow_id=None, workflow_run_id=None
    )

    async def standalone() -> None:
        activity_handle()

    with pytest.raises(StreamUnsupportedError, match="owned by an Activity"):
        await env.run(standalone)
