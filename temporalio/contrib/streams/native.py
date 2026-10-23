"""The native Stream plugin, whose Streams the Temporal server keeps.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import dataclasses
from collections import OrderedDict
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.bridge.client
import temporalio.bridge.streams
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.nexus.system
import temporalio.service
import temporalio.worker
import temporalio.workflow
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.contrib.streams import _errors, _workflow_reader
from temporalio.contrib.streams._backend import _ExtensionStreamBackend
from temporalio.contrib.streams._errors import StreamConfigError
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import (
    _decode_page,
    _ExtensionStreamPlugin,
    _runtime_of,
    _WorkflowOutbound,
)

_SERVICE = "temporal.api.streamservice.v1.StreamService"

_WAIT_COMPLETED = temporalio.workflow.NexusOperationCancellationType.WAIT_COMPLETED

# A Workflow read's Local Activity answers with its positions only: the request, and the answer
# with no Records. The codec step turns them back into the page before Workflow code sees it.
POSITIONS_TYPE = b"temporal.io/stream-read-positions"
_COUNT_KEY = "temporal.io/stream-read-count"
_PAGE_TYPE = stream_pb.ReadRecordsResponse.DESCRIPTOR.full_name.encode()
# The pages that this process read, so a live Workflow Task needs no second read.
_CACHE_SIZE = 256


def _page_payload(page: stream_pb.ReadRecordsResponse) -> Payload:
    return Payload(
        metadata={"encoding": b"binary/protobuf", "messageType": _PAGE_TYPE},
        data=page.SerializeToString(),
    )


class _DirectReadCodec(
    temporalio.converter.PayloadCodec, temporalio.converter.WithSerializationContext
):
    # Wraps the client's codec, and turns a read's positions back into its page on decode.
    def __init__(
        self,
        plugin: NativeStreamPlugin,
        backend: Callable[[], _ExtensionStreamBackend],
        inner: temporalio.converter.PayloadCodec | None,
    ) -> None:
        self._plugin = plugin
        # The runtime that a replay reads again on: the client's, or for a Replayer, one of
        # a client of the plugin.
        self._backend = backend
        self._inner = inner

    def with_context(
        self, context: temporalio.converter.SerializationContext
    ) -> _DirectReadCodec:
        inner = self._inner
        if isinstance(inner, temporalio.converter.WithSerializationContext):
            inner = inner.with_context(context)
        return _DirectReadCodec(self._plugin, self._backend, inner)

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        if self._inner is None:
            return list(payloads)
        return await self._inner.encode(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        decoded = (
            list(payloads)
            if self._inner is None
            else await self._inner.decode(payloads)
        )
        return [
            await self._page(payload)
            if payload.metadata.get("messageType") == POSITIONS_TYPE
            else payload
            for payload in decoded
        ]

    async def _page(self, positions: Payload) -> Payload:
        cached = self._plugin._pages.get(positions.data)
        if cached is not None:
            return _page_payload(cached)
        request_part, answer_part = Payloads.FromString(positions.data).payloads
        answer = stream_pb.ReadRecordsResponse.FromString(answer_part.data)
        count = int(positions.metadata[_COUNT_KEY])
        if not count:
            return _page_payload(answer)
        # The range is fixed by History, and a Stream only appends, so a read of the same range
        # gives the same Records. It runs on replay, or when the cache lost the page.
        request = stream_pb.ReadRecordsRequest.FromString(request_part.data)
        request.to_offset = answer.next_offset
        page = await self._backend().call_async(
            "ReadRecords", request, stream_pb.ReadRecordsResponse
        )
        if (len(page.records), page.next_offset, page.done) != (
            count,
            answer.next_offset,
            answer.done,
        ):
            raise temporalio.workflow.NondeterminismError(
                f"A replayed read of the Stream {request.stream.id} found {len(page.records)} "
                f"Records up to {page.next_offset}, and the read found {count} up to "
                f"{answer.next_offset}"
            )
        # The bodies are as their writer encoded them, so the client's codec decodes them.
        await _decode_page(
            dataclasses.replace(
                temporalio.converter.DataConverter.default, payload_codec=self._inner
            ),
            page,
        )
        return _page_payload(page)


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


class _NativeWorkflowOutbound(_WorkflowOutbound):
    # The server parks a read at the end of the Stream and completes it on an append or a
    # close, so a reader needs no timer.
    _temporal_stream_parks_reads = True
    _temporal_stream_read_wait = timedelta(seconds=60)

    async def _temporal_stream_read_page(
        self, request: stream_pb.ReadRecordsRequest
    ) -> stream_pb.ReadRecordsResponse:
        # A page is a direct read in a Local Activity, whose marker holds only positions. Only
        # the wait for new Records is a System Nexus operation, which the server parks.
        direct = stream_pb.ReadRecordsRequest()
        direct.CopyFrom(request)
        direct.ClearField("wait")
        direct.ClearField("create_wait")
        try:
            page = await super()._temporal_stream_read_page(direct)
        except _errors.StreamNotFoundError:
            if not request.HasField("create_wait"):
                raise
            await self._wake(request)
            return await super()._temporal_stream_read_page(direct)
        if page.records or page.done or not request.HasField("wait"):
            return page
        await self._wake(request)
        return await super()._temporal_stream_read_page(direct)

    async def _wake(self, request: stream_pb.ReadRecordsRequest) -> None:
        # Its answer holds no Records, so History holds only offsets.
        wake = stream_pb.ReadRecordsRequest()
        wake.CopyFrom(request)
        wake.wake_only = True
        handle = await self.next.start_system_nexus_operation(
            temporalio.worker.StartNexusOperationInput(
                endpoint=temporalio.nexus.system.TEMPORAL_SYSTEM_ENDPOINT,
                service=_SERVICE,
                operation="ReadRecords",
                input=wake,
                output_type=stream_pb.ReadRecordsResponse,
                schedule_to_close_timeout=None,
                schedule_to_start_timeout=None,
                start_to_close_timeout=None,
                cancellation_type=_WAIT_COMPLETED,
                headers=None,
                summary=None,
            )
        )
        try:
            await handle
        except temporalio.exceptions.NexusOperationError as err:
            raise _workflow_reader.error_of(err) from None


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

    _outbound_class = _NativeWorkflowOutbound
    _server_closes_with_run = True

    def _new_backend(
        self, service: temporalio.service.ServiceClient
    ) -> _ExtensionStreamBackend:
        # Each client gets its own runtime on its own connection, so a call goes on the
        # connection of the client that makes it.
        backend = _ExtensionStreamBackend(_ConnectionRuntime(service, self._config))
        self._backends.add(backend)
        return backend

    def _any_backend(self) -> _ExtensionStreamBackend:
        # A Replayer has no client, so a replay reads again on a client of the plugin.
        for backend in self._backends:
            return backend
        raise StreamConfigError(
            "A native replay reads the Stream again, so use the plugin of a client that "
            "connected"
        )

    def _read_result(
        self, request: stream_pb.ReadRecordsRequest, page: stream_pb.ReadRecordsResponse
    ) -> Any:
        # The marker holds the positions, and the page waits in the cache for the codec step.
        answer = stream_pb.ReadRecordsResponse(
            next_offset=page.next_offset, done=page.done
        )
        positions = Payload(
            metadata={
                "encoding": b"binary/protobuf",
                "messageType": POSITIONS_TYPE,
                _COUNT_KEY: str(len(page.records)).encode(),
            },
            data=Payloads(
                payloads=[
                    Payload(data=request.SerializeToString()),
                    Payload(data=answer.SerializeToString()),
                ]
            ).SerializeToString(),
        )
        self._pages[positions.data] = page
        self._pages.move_to_end(positions.data)
        while len(self._pages) > _CACHE_SIZE:
            self._pages.popitem(last=False)
        return temporalio.common.RawValue(positions)

    def configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        """Wrap the client's codec, so a Workflow read's positions turn back into its page."""
        config = super().configure_client(config)
        interceptor = self._interceptor_of(config.get("interceptors", []))
        backend = interceptor.backend if interceptor is not None else None
        converter = (
            config.get("data_converter") or temporalio.converter.DataConverter.default
        )
        config["data_converter"] = _wrap(
            self,
            (lambda: backend) if backend is not None else self._any_backend,
            converter,
        )
        return config

    def configure_replayer(
        self, config: temporalio.worker.ReplayerConfig
    ) -> temporalio.worker.ReplayerConfig:
        """Wrap the Replayer's codec, as the client's. A replay reads again on the connection
        of a client of the plugin, so use the plugin of a client that connected.
        """
        config = super().configure_replayer(config)
        converter = (
            config.get("data_converter") or temporalio.converter.DataConverter.default
        )
        config["data_converter"] = _wrap(self, self._any_backend, converter)
        return config

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
        self._pages: OrderedDict[bytes, stream_pb.ReadRecordsResponse] = OrderedDict()
        super().__init__(
            "temporalio.NativeStreamPlugin", False, config, None, name, default
        )


def _wrap(
    plugin: NativeStreamPlugin,
    backend: Callable[[], _ExtensionStreamBackend],
    converter: temporalio.converter.DataConverter,
) -> temporalio.converter.DataConverter:
    if isinstance(converter.payload_codec, _DirectReadCodec):
        return converter
    return dataclasses.replace(
        converter,
        payload_codec=_DirectReadCodec(plugin, backend, converter.payload_codec),
    )
