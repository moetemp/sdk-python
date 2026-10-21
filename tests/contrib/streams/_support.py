"""Finds the temporalio-streams crate that the bridge links, for the shared vectors."""

from __future__ import annotations

import functools
import json
import subprocess
from pathlib import Path

import pytest

_BRIDGE = Path(__file__).parents[3] / "temporalio" / "bridge"


@functools.cache
def streams_crate_dir() -> Path | None:
    try:
        output = subprocess.run(
            ["cargo", "metadata", "--offline", "--format-version", "1"],
            cwd=_BRIDGE,
            capture_output=True,
            check=True,
            timeout=120,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    for package in json.loads(output)["packages"]:
        if package["name"] == "temporalio-streams":
            return Path(package["manifest_path"]).parent
    return None


def crate_file(relative: str) -> Path:
    crate = streams_crate_dir()
    if crate is None:
        pytest.skip("The temporalio-streams crate checkout is not available")
    return crate / relative


def vectors(name: str) -> object:
    return json.loads(crate_file(f"testdata/{name}").read_text())
