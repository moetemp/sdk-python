"""A Stream Store in the memory of the process, for tests."""

from __future__ import annotations

from datetime import timedelta

import temporalio.bridge.streams
import temporalio.runtime
from temporalio.client import StreamStore


class MemoryStreamStore(StreamStore):
    """A Stream Store that keeps its Streams in the memory of this process, for tests.

    It runs the same Stream runtime and rules as every other Store on the bridge. A
    failed call raises :py:class:`temporalio.exceptions.StreamError`.

    .. warning::
        This API is experimental and subject to change.
    """

    def __init__(
        self,
        *,
        name: str = "memory",
        retention: timedelta | None = None,
        last_write_ttl: timedelta | None = None,
        runtime: temporalio.runtime.Runtime | None = None,
    ) -> None:
        """Create a memory Store. It builds its Stream runtime at its first call.

        Args:
            name: Name of the Store, which the refs of its Streams carry.
            retention: How long a closed Stream stays. ``None`` takes the default of
                the Stream runtime.
            last_write_ttl: How long an open Stream stays after its last write.
                ``None`` keeps an open Stream until its close.
            runtime: Runtime whose Tokio threads run the Store. ``None`` takes
                :py:meth:`temporalio.runtime.Runtime.default` at the first call.
        """
        for label, value in (
            ("retention", retention),
            ("last_write_ttl", last_write_ttl),
        ):
            if value is not None and value < timedelta():
                raise ValueError(f"{label} must not be negative")
        self._name = name
        self._retention = retention
        self._last_write_ttl = last_write_ttl
        self._runtime = runtime
        self._stream_runtime: temporalio.bridge.streams.StreamRuntime | None = None

    @property
    def name(self) -> str:
        """Name of the Store."""
        return self._name

    async def call(self, operation: str, request: bytes) -> bytes:
        """Answer one Stream operation with the Stream runtime of the bridge.

        Raises:
            temporalio.exceptions.StreamError: The call failed.
            RuntimeError: This build of temporalio has no Stream runtime.
        """
        if self._stream_runtime is None:
            runtime = self._runtime or temporalio.runtime.Runtime.default()
            self._stream_runtime = temporalio.bridge.streams.StreamRuntime.new_memory(
                runtime._core_runtime,
                name=self._name,
                retention=self._retention,
                last_write_ttl=self._last_write_ttl,
            )
        return await self._stream_runtime.call(operation, request)
