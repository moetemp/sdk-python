"""The Stream runtime that this bridge build links. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge

StreamCallError: type[Exception] = temporalio.bridge.temporal_sdk_bridge.StreamCallError


class StreamRuntime:
    """The runtime of the ``temporalio-streams`` crate on one Store."""

    @staticmethod
    def new_memory(
        runtime: temporalio.bridge.runtime.Runtime,
        *,
        name: str,
        retention: timedelta | None,
        last_write_ttl: timedelta | None,
    ) -> StreamRuntime:
        """Build the runtime on a new memory Store.

        Raises:
            RuntimeError: The bridge build has no Stream runtime.
        """
        new = getattr(
            temporalio.bridge.temporal_sdk_bridge, "new_memory_stream_runtime", None
        )
        if new is None:
            raise RuntimeError("This build of temporalio has no Stream runtime")
        return StreamRuntime(new(runtime._ref, name, retention, last_write_ttl))

    def __init__(self, ref: Any) -> None:
        """Wrap the bridge's runtime object. Use ``new_memory`` instead."""
        self._ref = ref

    async def call(self, operation: str, request: bytes) -> bytes:
        """Run one Stream operation on an encoded request and return the encoded response.

        Raises:
            StreamCallError: The call failed, with the gRPC code, message and details as args.
        """
        return await self._ref.call(operation, request)
