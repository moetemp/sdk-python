"""Finds the Stream crates that the bridge links, and describes a Stream in tests."""

from __future__ import annotations

import functools
import json
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any, TypeVar

import pytest

from temporalio.client import Client, Stream
from temporalio.streams import StreamBackend, StreamError, StreamErrorReason
from temporalio.streams._plugin import ref_backend
from temporalio.streams.system import models
from tests.helpers import assert_eventually

_StreamT = TypeVar("_StreamT", bound=Stream[Any])

_BRIDGE = Path(__file__).parents[3] / "temporalio" / "bridge"


@functools.cache
def crate_dirs() -> dict[str, Path]:
    try:
        output = subprocess.run(
            ["cargo", "metadata", "--offline", "--format-version", "1"],
            cwd=_BRIDGE,
            capture_output=True,
            check=True,
            timeout=120,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    return {
        package["name"]: Path(package["manifest_path"]).parent
        for package in json.loads(output)["packages"]
    }


def crate_file(relative: str, crate: str = "temporalio-streams") -> Path:
    directory = crate_dirs().get(crate)
    if directory is None:
        pytest.skip(f"The {crate} crate checkout is not available")
    return directory / relative


def vectors(name: str) -> object:
    return json.loads(crate_file(f"testdata/{name}").read_text(encoding="utf-8"))


def only_backend(client: Client) -> StreamBackend:
    """The one Stream backend of a client."""
    stream_config = client.config(active_config=True)["stream_config"]
    assert stream_config is not None
    [backend] = stream_config.backends
    return backend


async def created(stream: _StreamT) -> _StreamT:
    """Wait for the create of a Stream that a Workflow makes, which the test can't await."""

    async def exists() -> None:
        try:
            await stream.get_offset()
        except StreamError as err:
            assert err.reason != StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND, (
                "not created yet"
            )
            raise

    await assert_eventually(exists, timeout=timedelta(seconds=20))
    return stream


async def stream_status(
    client: Client, stream_id: str, backend: str | None = None
) -> models.StreamStatus:
    """The status that a describe gives, such as whether the Stream is closed."""
    _, calls = ref_backend(client, stream_id, backend)
    response = await calls.service(client.namespace).send_describe_stream(
        models.DescribeStreamRequest(ref=models.StreamRef(id=stream_id))
    )
    return response.status or models.StreamStatus.StreamStatusUnspecified
