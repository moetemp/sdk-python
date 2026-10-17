"""What the stream tests share."""

from __future__ import annotations

import asyncio
import functools
import json
import subprocess
import uuid
from pathlib import Path
from typing import Any

from temporalio.client import Client
from temporalio.contrib.streams import StreamStorePlugin
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


@functools.cache
def streams_crate_dir() -> Path:
    """The checkout of the temporalio-streams crate the bridge builds against.

    Its vectors and docs are the ones the linked stream runtime follows.
    """
    manifest = Path(__file__).parents[3] / "temporalio" / "bridge" / "Cargo.toml"
    metadata = json.loads(
        subprocess.check_output(
            [
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--manifest-path",
                str(manifest),
            ]
        )
    )
    for package in metadata["packages"]:
        if package["name"] == "temporalio-streams":
            return Path(package["manifest_path"]).parent
    raise RuntimeError("temporalio-streams is not a dependency of the bridge")


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
