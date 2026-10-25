"""The Stream backends that call the temporalio-streams runtime in the bridge."""

from __future__ import annotations

import logging
import weakref
from collections.abc import Sequence
from typing import Any

import temporalio.bridge.streams
import temporalio.client
import temporalio.converter
import temporalio.runtime
import temporalio.service
import temporalio.worker
from temporalio.common._streams._backend import BackendCalls, _ExtensionCalls
from temporalio.common._streams._errors import StreamConfigError
from temporalio.common._streams._plugin import StreamBackend
from temporalio.common._streams.proto import v1 as ext_pb

logger = logging.getLogger(__name__)


class ExtensionStreamBackend(StreamBackend):
    """Base class of the Redis and memory backends, which call the temporalio-streams runtime.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        name: str,
        memory: bool,
        config: ext_pb.RedisStreamConfig | ext_pb.MemoryStreamConfig,
        runtime: temporalio.runtime.Runtime | None,
        backend_name: str,
        default: bool,
    ) -> None:
        """Create the backend over the bridge's Stream runtime, on Redis or in memory.

        ``backend_name`` is the name that Stream handles store for this backend. ``default``
        makes it the backend of a create with no backend name.
        """
        config.backend = backend_name
        self._config = config.SerializeToString()
        # On Redis, the clients of one Core runtime share one runtime object and its connections.
        # The memory backend has one runtime object, so all its clients share its Streams.
        self._backends: weakref.WeakKeyDictionary[
            temporalio.runtime.Runtime, _ExtensionCalls
        ] = weakref.WeakKeyDictionary()
        self._memory: _ExtensionCalls | None = None
        if memory:
            self._memory = self._new_backend(
                runtime or temporalio.runtime.Runtime.default(), True
            )
        super().__init__(
            name,
            backend_name=backend_name,
            default=default,
        )

    def _new_backend(
        self, runtime: temporalio.runtime.Runtime, memory: bool = False
    ) -> _ExtensionCalls:
        if self._memory is not None and not memory:
            return self._memory
        backend = self._backends.get(runtime)
        if backend is None:
            backend = _ExtensionCalls(
                temporalio.bridge.streams.StreamRuntime(
                    runtime._core_runtime, memory, self._config
                )
            )
            self._backends[runtime] = backend
        return backend

    def _configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        config = super()._configure_client(config)
        backend = self._new_backend(_runtime_of(config["service_client"]))
        config["interceptors"] = [
            *config.get("interceptors", []),
            _StreamInterceptor(self, backend, config["data_converter"]),
        ]
        return config

    def _backend_for(self, client: temporalio.client.Client) -> BackendCalls:
        # The Stream runtime that this backend gave the client.
        interceptor = self._interceptor_of(
            client.config(active_config=True).get("interceptors", [])
        )
        if interceptor is None or interceptor.backend is None:
            raise StreamConfigError(
                f"The client has no {type(self).__name__}. Register it with "
                "stream_backends=."
            )
        return interceptor.backend

    def _interceptor_of(self, interceptors: Sequence[Any]) -> _StreamInterceptor | None:
        for interceptor in interceptors:
            if (
                isinstance(interceptor, _StreamInterceptor)
                and interceptor.plugin is self
            ):
                return interceptor
        return None

    async def shutdown(self) -> None:
        """Close every open writer, including the client's.

        Call it when the process stops writing to Streams.
        """
        failed = 0
        for backend in list(self._backends.values()):
            failed += await backend.close_writers()
        if failed:
            logger.warning("Stream shutdown left %d failed writers", failed)


def _runtime_of(
    service: temporalio.service.ServiceClient,
) -> temporalio.runtime.Runtime:
    # Find the runtime of a client's connection the same way the Worker does.
    service = getattr(service, "worker_service_client", service)
    runtime = getattr(service, "_runtime", None) or getattr(
        getattr(service, "config", None), "runtime", None
    )
    return runtime or temporalio.runtime.Runtime.default()


class _StreamInterceptor(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    # There is one for each client of the backend, with that client's Stream runtime. The
    # client's Workers get it from the client.
    def __init__(
        self,
        plugin: ExtensionStreamBackend,
        backend: _ExtensionCalls | None,
        converter: temporalio.converter.DataConverter | None,
    ) -> None:
        super().__init__()
        self.plugin = plugin
        self.backend = backend
        self.converter = converter or temporalio.converter.DataConverter.default
