"""The Stream runtime that this bridge build links. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from typing import Any

import temporalio.bridge.client
import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge

StreamCallError: type[Exception] = temporalio.bridge.temporal_sdk_bridge.StreamCallError


class StreamRuntime:
    """The temporalio-streams runtime, on Redis, on the memory Store, or on the server."""

    def __init__(
        self, runtime: temporalio.bridge.runtime.Runtime, memory: bool, config: bytes
    ) -> None:
        """Build the runtime from its encoded ``RedisStreamConfig``.

        Raises:
            ValueError: The bridge build doesn't link the crate, or the config is not valid.
        """
        new = getattr(temporalio.bridge.temporal_sdk_bridge, "new_stream_runtime", None)
        if new is None:
            raise ValueError("This bridge build has no Stream runtime")
        self._ref: Any = new(runtime._ref, memory, config)

    @classmethod
    def on_client(
        cls,
        runtime: temporalio.bridge.runtime.Runtime,
        client: temporalio.bridge.client.Client,
        config: bytes,
    ) -> StreamRuntime:
        """Build the native runtime on a connected client's connection, from its encoded
        ``NativeStreamConfig``.

        Raises:
            ValueError: The bridge build doesn't link the crate, or the config is not valid.
        """
        new = getattr(
            temporalio.bridge.temporal_sdk_bridge, "new_native_stream_runtime", None
        )
        if new is None:
            raise ValueError("This bridge build has no Stream runtime")
        streams = cls.__new__(cls)
        streams._ref = new(runtime._ref, client._ref, config)
        return streams

    @property
    def name(self) -> str:
        """Name the runtime gives itself."""
        return self._ref.name

    async def call(self, method: str, input: bytes) -> bytes:
        """Call ``method`` on the runtime and await its output.

        Raises:
            StreamCallError: The call failed, with the gRPC code, message and details as args.
        """
        return await self._ref.call(method, input)

    def free_writer(self, writer_id: int) -> None:
        """Release one handle of a writer with no wait, such as from a finalizer."""
        self._ref.free_writer(writer_id)
