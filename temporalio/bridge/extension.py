"""Worker extensions that this bridge build links. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from typing import Any

import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge


class WorkerExtension:
    """An extension that the bridge builds by its name."""

    def __init__(
        self, runtime: temporalio.bridge.runtime.Runtime, name: str, config: bytes
    ) -> None:
        """Build the extension with ``name`` from its encoded config."""
        self._ref: Any = temporalio.bridge.temporal_sdk_bridge.new_worker_extension(
            runtime._ref, name, config
        )

    @property
    def name(self) -> str:
        """Name the extension gives itself."""
        return self._ref.name
