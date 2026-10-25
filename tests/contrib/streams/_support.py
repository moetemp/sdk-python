"""Finds the Stream crates that the bridge links, for the shared vectors and protos."""

from __future__ import annotations

import functools
import json
import subprocess
from pathlib import Path

import pytest

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
