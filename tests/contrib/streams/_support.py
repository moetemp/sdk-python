"""What the stream tests share."""

from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path
from typing import Any

import pytest

import temporalio
from temporalio.client import Client
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.contrib.streams.postgres import PostgresStreams
from temporalio.converter import DataConverter


async def connect_with(
    client: Client,
    store: StreamStorePlugin | None,
    converter: DataConverter | None = None,
) -> Client:
    """A client of the test server that carries ``store``.

    It connects, since connecting is what hands the store to Workers built
    from it.
    """
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        data_converter=converter or client.data_converter,
        plugins=[store] if store is not None else [],
    )


def postgres_streams(schema: str | None = None, **settings: Any) -> PostgresStreams:
    """A store on ``STREAMS_POSTGRES_URL``, in a schema of its own unless ``schema`` names one.

    Skips the test when the variable is unset.
    """
    url = os.environ.get("STREAMS_POSTGRES_URL", "")
    if not url:
        pytest.skip("set STREAMS_POSTGRES_URL to run on PostgreSQL")
    # The test process keeps every store it connects, so each pool stays small.
    settings.setdefault("max_connections", 4)
    return PostgresStreams(url, schema=schema or new_schema(), **settings)


def new_schema() -> str:
    return f"streams_{uuid.uuid4().hex}"


def new_workflow_id() -> str:
    return f"streams-{uuid.uuid4().hex}"


async def read_all(records: Any, timeout: float = 10.0) -> list:
    """Read until the stream ends, which is when its owner closes."""
    out = []

    async def pull() -> None:
        async for record in records:
            out.append(record)

    try:
        await asyncio.wait_for(pull(), timeout)
    finally:
        await records.aclose()
    return out


def vendored_core_nexus() -> Path:
    """The vendored Core's ``crates/streams/nexus``, with the contract and the token vectors.

    Raises:
        RuntimeError: The Core submodule isn't checked out, as in an installed
            wheel, so the tests that check against Core's files can't run.
    """
    path = Path(temporalio.__file__).parent / "bridge/sdk-core/crates/streams/nexus"
    if not path.is_dir():
        raise RuntimeError(
            f"the Core submodule isn't checked out at {path}; run "
            "`git submodule update --init` to test against Core's contract"
        )
    return path
