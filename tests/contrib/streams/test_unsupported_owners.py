"""Owners this release does not support say so.

Only a Workflow or an Activity owns a stream in this release.
Every other owner raises ``StreamUnsupportedError``.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

import pytest

from temporalio.bridge.proto.streams import StreamOwnerKind
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

_STARTED = datetime(2026, 10, 14, tzinfo=timezone.utc)


async def test_a_ref_refuses_owner_kinds_this_release_lacks_where_it_opens(
    client: Client,
):
    streams_client = await connect_with(client, MemoryStreams())
    for kind in ("standalone", "nexus"):
        ref = StreamRef(kind, "x")  # type: ignore[arg-type]
        with pytest.raises(StreamUnsupportedError, match="a Workflow or an Activity"):
            get_stream_handle(streams_client, ref)


def _standalone_env(client: Client, run_id: str | None) -> ActivityEnvironment:
    env = ActivityEnvironment(client=client)
    env.info = dataclasses.replace(
        ActivityEnvironment.default_info(),
        activity_id="act",
        attempt=3,
        started_time=_STARTED,
        workflow_id=None,
        workflow_run_id=None,
        workflow_type=None,
        workflow_namespace=None,
        activity_run_id=run_id,
    )
    return env


async def test_a_standalone_activity_writes_its_own_stream(client: Client):
    streams_client = await connect_with(client, MemoryStreams())

    async def standalone() -> None:
        stream = activity_handle()
        assert stream.ref == StreamRef.for_activity("act", run_id="run")
        producer = stream.producer()
        assert producer.producer_id == "act@run"
        assert producer.attempt == int(_STARTED.timestamp()) * 1000 << 11 | 3
        identity = producer._activity
        assert identity is not None
        assert (identity.workflow_id, identity.run_id) == ("", "run")
        address = stream._inner._address("out")
        assert address.owner_kind == StreamOwnerKind.STREAM_OWNER_KIND_ACTIVITY
        assert (address.workflow_id, address.activity_id) == ("", "act")
        assert address.run_id == "run"

    await _standalone_env(streams_client, "run").run(standalone)


async def test_a_standalone_activity_with_no_run_id_has_no_stream(client: Client):
    streams_client = await connect_with(client, MemoryStreams())

    async def standalone() -> None:
        activity_handle()

    with pytest.raises(StreamUnsupportedError, match="no run id"):
        await _standalone_env(streams_client, None).run(standalone)
