"""The Stream runtime that this bridge build links. (unstable)

Nothing in this module should be considered stable. The API may change.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import temporalio.bridge.runtime
import temporalio.bridge.temporal_sdk_bridge
import temporalio.client
import temporalio.exceptions
import temporalio.runtime

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

    @staticmethod
    def new_redis(
        runtime: temporalio.bridge.runtime.Runtime,
        *,
        urls: Sequence[str],
        cluster: bool,
        key_prefix: str | None,
        retention: timedelta | None,
        last_write_ttl: timedelta | None,
        response_timeout: timedelta | None,
        name: str,
        wait_replicas: int,
        wait_timeout: timedelta | None,
    ) -> StreamRuntime:
        """Build the runtime on Redis. It connects at its first call.

        Raises:
            RuntimeError: The bridge build has no Stream runtime.
            ValueError: An option is bad.
        """
        new = getattr(
            temporalio.bridge.temporal_sdk_bridge, "new_redis_stream_runtime", None
        )
        if new is None:
            raise RuntimeError("This build of temporalio has no Stream runtime")
        return StreamRuntime(
            new(
                runtime._ref,
                list(urls),
                cluster=cluster,
                key_prefix=key_prefix,
                retention=retention,
                last_write_ttl=last_write_ttl,
                response_timeout=response_timeout,
                store_name=name,
                wait_replicas=wait_replicas,
                wait_timeout=wait_timeout,
            )
        )

    def __init__(self, ref: Any) -> None:
        """Wrap the bridge's runtime object. Use ``new_memory`` or ``new_redis`` instead."""
        self._ref = ref

    async def call(self, operation: str, request: bytes) -> bytes:
        """Run one Stream operation on an encoded request and return the encoded response.

        Raises:
            temporalio.exceptions.StreamError: The call failed.
        """
        try:
            return await self._ref.call(operation, request)
        except StreamCallError as err:
            _, message, details = err.args
            raise temporalio.exceptions._stream_error_from_call(
                message, details
            ) from None


class _BridgeStreamStore(temporalio.client.StreamStore):  # pyright: ignore[reportUnusedClass]
    """A Store that runs on the Stream runtime of the bridge.

    It builds the runtime at its first call, so the Store can be built before the
    Runtime and its Tokio threads exist.
    """

    def __init__(
        self,
        *,
        name: str,
        runtime: temporalio.runtime.Runtime | None,
        call_options: temporalio.client.StreamStoreCallOptions | None = None,
    ) -> None:
        self._name = name
        self._runtime = runtime
        self._call_options = call_options
        self._stream_runtime: StreamRuntime | None = None

    @property
    def name(self) -> str:
        """Name of the Store."""
        return self._name

    @property
    def call_options(self) -> temporalio.client.StreamStoreCallOptions:
        """Options of the Local Activity that calls this Store for Workflow code."""
        return self._call_options or super().call_options

    @abstractmethod
    def _new_stream_runtime(
        self, runtime: temporalio.bridge.runtime.Runtime
    ) -> StreamRuntime: ...

    async def call(self, operation: str, request: bytes) -> bytes:
        """Answer one Stream operation with the Stream runtime of the bridge.

        Raises:
            temporalio.exceptions.StreamError: The call failed.
            RuntimeError: This build of temporalio has no Stream runtime.
        """
        if self._stream_runtime is None:
            runtime = self._runtime or temporalio.runtime.Runtime.default()
            self._stream_runtime = self._new_stream_runtime(runtime._core_runtime)
        return await self._stream_runtime.call(operation, request)


def _check_not_negative(**durations: timedelta | None) -> None:  # pyright: ignore[reportUnusedFunction]
    for label, value in durations.items():
        if value is not None and value < timedelta():
            raise ValueError(f"{label} must not be negative")
