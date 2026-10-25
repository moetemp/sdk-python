"""Finds the temporalio-streams crate that the bridge links, for the shared vectors."""

from __future__ import annotations

import asyncio
import functools
import json
import subprocess
from pathlib import Path
from typing import Any

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
    return json.loads(crate_file(f"testdata/{name}").read_text(encoding="utf-8"))


def on_native(client: Any) -> bool:
    """Whether a client's default backend is the native server, whose rules differ in places."""
    from temporalio.common._streams import _plugin

    name, _ = _plugin.client_backend(client, None)
    return name == "native"


async def read_after_run_end(client: Any, stream: Any) -> list[Any]:
    """The items of a Stream whose owner's run chain ended.

    The native server closes the Stream at the chain's end, a moment after the run closes, so
    a native read waits for the close. A Redis read ends at the newest Record.
    """
    if not on_native(client):
        return [item async for item in stream.subscribe(wait_new_records=False)]

    async def to_close() -> list[Any]:
        return [item async for item in stream.subscribe()]

    return await asyncio.wait_for(to_close(), 30)
