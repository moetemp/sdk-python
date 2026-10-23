"""The native Stream plugin, whose Streams the Temporal server keeps.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.bridge.client
import temporalio.bridge.streams
import temporalio.service
from temporalio.contrib.streams._backend import _ExtensionStreamBackend
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import _ExtensionStreamPlugin, _runtime_of


class _ConnectionRuntime:
    # The native runtime of one client, on that client's connection. A lazy client connects at
    # its first call, so the runtime comes then too.
    def __init__(
        self, service: temporalio.service.ServiceClient, config: bytes
    ) -> None:
        self._service = service
        self._config = config
        self._streams: temporalio.bridge.streams.StreamRuntime | None = None

    async def call(self, method: str, input: bytes) -> bytes:
        if self._streams is None:
            bridge_client = await _bridge_client(self._service)
            # Two first calls may both wait for the connection. One runtime serves both, so
            # a writer that one opens is known to the other.
            if self._streams is None:
                self._streams = temporalio.bridge.streams.StreamRuntime.on_client(
                    _runtime_of(self._service)._core_runtime,
                    bridge_client,
                    self._config,
                )
        return await self._streams.call(method, input)

    def free_writer(self, writer_id: int) -> None:
        # A writer exists only after a call, which built the runtime.
        if self._streams is not None:
            self._streams.free_writer(writer_id)


async def _bridge_client(
    service: temporalio.service.ServiceClient,
) -> temporalio.bridge.client.Client:
    service = getattr(service, "worker_service_client", service)
    connect = getattr(service, "_connected_client", None)
    if connect is None:
        raise RuntimeError("The native Stream backend needs a client with a connection")
    return await connect()


class NativeStreamPlugin(_ExtensionStreamPlugin):
    """Carries Stream calls to the Temporal server, which keeps the Streams.

    Workflow writes run as Local Activities, as on Redis, and the client's Stream runtime
    writes them to the server's ``temporal.api.streamservice.v1.StreamService``. Activity and
    client calls go to the same service. Each client of the plugin gets its own runtime on its
    own connection, and the client's Workers share it. It registers the backend name
    ``native``. Put it on the client next to ``RedisStreamPlugin`` to keep old Streams on
    Redis and create new ones on the server.

    .. warning::
        This API is experimental.
    """

    _server_closes_with_run = True

    def _new_backend(
        self, service: temporalio.service.ServiceClient
    ) -> _ExtensionStreamBackend:
        # Each client gets its own runtime on its own connection, so a call goes on the
        # connection of the client that makes it.
        backend = _ExtensionStreamBackend(_ConnectionRuntime(service, self._config))
        self._backends.add(backend)
        return backend

    def __init__(
        self,
        *,
        retention: timedelta | None = None,
        name: str = "native",
        default: bool = False,
    ) -> None:
        """Create the plugin. It makes no call. Each client of the plugin calls the server on
        its own connection.

        Args:
            retention: The default Retention of a new Stream. Defaults to the server's.
            name: The backend name that Stream handles carry.
            default: Make this the client's default backend. A client with more than one
                Stream plugin marks exactly one, else it raises ``StreamConfigError``.
        """
        config = ext_pb.NativeStreamConfig()
        if retention is not None:
            config.retention.FromTimedelta(retention)
        super().__init__(
            "temporalio.NativeStreamPlugin", False, config, None, name, default
        )
