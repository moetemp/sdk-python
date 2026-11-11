"""The Stream runtime that this bridge build links. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from typing import Any

import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge

StreamCallError: type[Exception] = temporalio.bridge.temporal_sdk_bridge.StreamCallError


class StreamRuntime:
    """The temporalio-streams runtime, on Redis or on the memory Store."""

    def __init__(
        self, runtime: temporalio.bridge.runtime.Runtime, memory: bool, config: bytes
    ) -> None:
        """Build the runtime on Redis, or on memory with ``memory``, from its encoded config.

        Raises:
            StreamConfigError: The bridge build doesn't link the crate.
            ValueError: The config is not valid.
        """
        new = getattr(temporalio.bridge.temporal_sdk_bridge, "new_stream_runtime", None)
        if new is None:
            # Imported here, since temporalio.streams._errors imports this module.
            from temporalio.streams._errors import StreamConfigError

            raise StreamConfigError("This bridge build has no Stream runtime")
        self._ref: Any = new(runtime._ref, memory, config)

    @property
    def name(self) -> str:
        """Name that the runtime reports."""
        return self._ref.name

    async def call(self, method: str, input: bytes) -> bytes:
        """Call ``method`` on the runtime and await its output.

        Raises:
            StreamCallError: The call failed, with the gRPC code, message and details as args.
        """
        return await self._ref.call(method, input)

    async def open_writer(self, input: bytes) -> StreamWriter:
        """Open a writer from an encoded ``OpenWriterRequest``.

        Raises:
            StreamCallError: The open failed, with the gRPC code, message and details as args.
        """
        return StreamWriter(await self._ref.open_writer(input))


class StreamWriter:
    """One writer of the runtime. Dropping the last reference releases it."""

    def __init__(self, ref: Any) -> None:
        """Wrap the bridge's writer object. Use ``StreamRuntime.open_writer`` instead."""
        self._ref = ref

    async def write(self, input: bytes) -> bytes:
        """Write an encoded ``WriteRecordsRequest``, and return the encoded response.

        Raises:
            StreamCallError: The write failed, with the gRPC code, message and details as args.
        """
        return await self._ref.write(input)

    async def close(self) -> bytes:
        """Close the writer, and return the encoded ``CloseWriterResponse``.

        Raises:
            StreamCallError: The writer failed, with the gRPC code, message and details as args.
        """
        return await self._ref.close()
