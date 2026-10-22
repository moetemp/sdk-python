"""Worker extensions that this bridge build links. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from typing import Any

import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge

ExtensionCallError: type[Exception] = (
    temporalio.bridge.temporal_sdk_bridge.ExtensionCallError
)


class WorkerExtension:
    """An extension that the bridge builds by its name."""

    def __init__(
        self, runtime: temporalio.bridge.runtime.Runtime, name: str, config: bytes
    ) -> None:
        """Build the extension with ``name`` from its encoded config."""
        self._ref: Any = temporalio.bridge.temporal_sdk_bridge.new_worker_extension(
            runtime._ref, name, config
        )

    @staticmethod
    def combine(parts: list[WorkerExtension]) -> WorkerExtension:
        """One extension whose hooks run the hooks of each part, in order."""
        combined = WorkerExtension.__new__(WorkerExtension)
        combined._ref = temporalio.bridge.temporal_sdk_bridge.combine_worker_extensions(
            [part._ref for part in parts]
        )
        return combined

    @property
    def name(self) -> str:
        """Name the extension gives itself."""
        return self._ref.name

    def call(self, method: str, input: bytes) -> bytes:
        """Call ``method`` and wait for its output.

        Raises:
            ExtensionCallError: The call failed, with the gRPC code, message and details as args.
        """
        return self._ref.call(method, input)

    async def call_async(self, method: str, input: bytes) -> bytes:
        """Call ``method`` on the runtime and await its output.

        Raises:
            ExtensionCallError: The call failed, with the gRPC code, message and details as args.
        """
        return await self._ref.call_async(method, input)
