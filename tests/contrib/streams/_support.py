"""Finds the Stream crates that the bridge links, and binds a Workflow's Stream in tests."""

from __future__ import annotations

import functools
import json
import subprocess
from pathlib import Path
from typing import Any, TypeVar

import pytest

from temporalio import common
from temporalio.client import WorkflowHandle
from temporalio.common._streams import _ids
from temporalio.common._streams._defn import definition_of

T = TypeVar("T")

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


def workflow_stream(
    handle: WorkflowHandle[Any, Any],
    definition: type[common.StreamDefinition[T]],
    backend: str | None = None,
) -> common.ClientStream[T]:
    """The Stream of ``definition`` that the Workflow of ``handle`` created."""
    # A Workflow's Stream id is <workflow id>:<first run id>:<name>.
    stream_id = _ids.scoped_stream_id(
        handle.id, handle.first_execution_run_id or "", definition_of(definition).name
    )
    return handle._client.stream(definition, id=stream_id, backend=backend)
