"""The native Stream backend, whose Streams the Temporal server keeps."""

from __future__ import annotations

import dataclasses
import hashlib
import weakref
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
from temporalio.common._streams import _errors, _workflow_reader
from temporalio.common._streams._backend import _ExtensionCalls
from temporalio.common._streams._extension import (
    ExtensionStreamBackend,
    _decode_page,
    _runtime_of,
    _WorkflowOutbound,
    page_payload,
)
from temporalio.common._streams.proto import v1 as ext_pb

_SERVICE = "temporal.api.streamservice.v1.StreamService"

# A wake's cancel ends it once the server has the cancel. The server drops the cancel of a run
# that closed, so the run's end waits for it.
_WAIT_REQUESTED = temporalio.workflow.NexusOperationCancellationType.WAIT_REQUESTED
# A close goes on when the run is cancelled, since the cancel may be what the close records.
_ABANDON = temporalio.workflow.NexusOperationCancellationType.ABANDON
# How long a wake parks on the server. A parked wake adds nothing to History, so an idle reader
# costs nothing. The server caps it at `stream.maxWakeWait`, and an older server at 60 s.
_WAKE_WAIT = timedelta(days=7)

# A Workflow read's Local Activity answers with its positions only: the request, and the answer
# with no Records. The codec step turns them back into the page before Workflow code sees it.
POSITIONS_TYPE = b"temporal.io/stream-read-positions"
_COUNT_KEY = "temporal.io/stream-read-count"
# The SHA-256 of the page's Records as the Store gave them, so a replay that reads other
# Records fails.
_DIGEST_KEY = "temporal.io/stream-read-digest"
# The run that read the page. A reset run replays the old run's pages, which this process may
# never have read.
_RUN_KEY = "temporal.io/stream-read-run"
# The pages that this process read and no Workflow Task took yet. A Workflow Task takes its
# pages, and the bound covers the runs that left the Worker before theirs.
_CACHE_SIZE = 256
_pages: OrderedDict[tuple[str, bytes], list[stream_pb.StreamRecord]] = OrderedDict()
RANGE_GONE_TYPE = "STREAM_RANGE_GONE"


def _digest(records: Sequence[stream_pb.StreamRecord]) -> bytes:
    page = stream_pb.ReadRecordsResponse(records=records)
    return hashlib.sha256(page.SerializeToString(deterministic=True)).digest()


def _range_gone(stream_id: str, detail: str) -> temporalio.exceptions.ApplicationError:
    # Retryable, so it fails the Workflow Task and not the run. The operator picks the way out.
    return temporalio.exceptions.ApplicationError(
        f"A recorded read of the Stream {stream_id} is no longer in the Store, so the run can't "
        f"replay: {detail}. Reset the run to a point before its reads of the Stream, or "
        "terminate it.",
        type=RANGE_GONE_TYPE,
    )


class _DirectReadCodec(
    temporalio.converter.PayloadCodec, temporalio.converter.WithSerializationContext
):
    # Wraps the client's codec, and turns a read's positions back into its page on decode.
    def __init__(
        self,
        backend: Callable[[], _ExtensionCalls],
        inner: temporalio.converter.PayloadCodec | None,
        records: temporalio.converter.PayloadCodec | None,
    ) -> None:
        # The runtime that a replay reads again on: the Worker's client's, or the Replayer's
        # stream_client's.
        self._backend = backend
        self._inner = inner
        # Writers encode Records with no serialization context, so a page decodes with the
        # client's codec as it is, live and on replay alike.
        self._records = records

    def with_context(
        self, context: temporalio.converter.SerializationContext
    ) -> _DirectReadCodec:
        inner = self._inner
        if isinstance(inner, temporalio.converter.WithSerializationContext):
            inner = inner.with_context(context)
        return _DirectReadCodec(self._backend, inner, self._records)

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
        request_part, answer_part = Payloads.FromString(positions.data).payloads
        # History has the answer, so the page takes its offsets and flags from it.
        page = stream_pb.ReadRecordsResponse.FromString(answer_part.data)
        count = int(positions.metadata[_COUNT_KEY])
        run_id = positions.metadata.get(_RUN_KEY, b"").decode()
        records: Sequence[stream_pb.StreamRecord] | None = _pages.pop(
            (run_id, positions.data), None
        )
        if records is None and count:
            records = await self._read_again(
                stream_pb.ReadRecordsRequest.FromString(request_part.data),
                page,
                count,
                positions.metadata[_DIGEST_KEY],
            )
        page.records.extend(records or [])
        await _decode_page(
            dataclasses.replace(
                temporalio.converter.DataConverter.default,
                payload_codec=self._records,
            ),
            page,
        )
        return page_payload(page)

    async def _read_again(
        self,
        request: stream_pb.ReadRecordsRequest,
        answer: stream_pb.ReadRecordsResponse,
        count: int,
        digest: bytes,
    ) -> Sequence[stream_pb.StreamRecord]:
        # The range is fixed by History, and a Stream only appends, so a read of the same range
        # gives the same Records. It runs on replay, or when the page left the cache.
        request.to_offset = answer.next_offset
        stream_id = request.stream.id
        try:
            page = await self._backend().call_async(
                "ReadRecords", request, stream_pb.ReadRecordsResponse
            )
        except (_errors.StreamExpiredError, _errors.StreamNotFoundError) as err:
            raise _range_gone(stream_id, err.message) from None
        except _errors.StreamError as err:
            # A Stream error is not retryable, and here it would fail the run. The run did
            # nothing wrong, so the Workflow Task fails and is retried.
            raise RuntimeError(
                f"Reading the Stream {stream_id} again for a replay failed: {err.message}"
            ) from err
        if _digest(page.records) == digest:
            return page.records
        if len(page.records) < count:
            raise _range_gone(
                stream_id,
                f"the read had {count} Records, and the Store has {len(page.records)}",
            )
        raise temporalio.workflow.NondeterminismError(
            f"A replayed read of the Stream {stream_id} found other Records than the read "
            f"before offset {answer.next_offset}"
        )


class _ConnectionRuntime:
    # The native runtime of one client, on that client's connection. A lazy client connects at
    # its first call, so the runtime comes then too.
    def __init__(
        self, service: temporalio.service.ServiceClient, config: bytes
    ) -> None:
        # The backend keeps the runtime by the client in a weak map, so a strong ref here
        # would keep every client alive.
        self._service = weakref.ref(service)
        self._config = config
        self._streams: temporalio.bridge.streams.StreamRuntime | None = None

    async def call(self, method: str, input: bytes) -> bytes:
        if self._streams is None and method == "CloseWriters":
            # A runtime that no call built has no writer, so a shutdown makes no connection.
            return ext_pb.CloseWritersResponse().SerializeToString()
        if self._streams is None:
            service = self._service()
            if service is None:
                raise RuntimeError("The client of the native Stream backend is gone")
            bridge_client = await _bridge_client(service)
            # Two first calls may both wait for the connection. One runtime serves both, so
            # a writer that one opens is known to the other.
            if self._streams is None:
                self._streams = temporalio.bridge.streams.StreamRuntime.on_client(
                    _runtime_of(service)._core_runtime,
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
    _temporal_stream_read_wait = _WAKE_WAIT
    _temporal_stream_create_wait = _WAKE_WAIT
    # The marker holds no Records, so a page needs no byte cap.
    _temporal_stream_page_bytes = 0

    async def _temporal_stream_read_page(
        self, request: stream_pb.ReadRecordsRequest
    ) -> _workflow_reader.Page:
        # A page is a direct read in a Local Activity, whose marker holds only positions. Only
        # the wait for new Records is a System Nexus operation, which the server parks.
        direct = stream_pb.ReadRecordsRequest()
        direct.CopyFrom(request)
        direct.ClearField("wait")
        direct.ClearField("create_wait")
        return await super()._temporal_stream_read_page(direct)

    async def _temporal_stream_start_wake(
        self, request: stream_pb.ReadRecordsRequest
    ) -> temporalio.workflow.NexusOperationHandle[stream_pb.ReadRecordsResponse]:
        # Its answer holds no Records, so History holds only offsets.
        wake = stream_pb.ReadRecordsRequest()
        wake.CopyFrom(request)
        wake.wake_only = True
        return await self.next.start_system_nexus_operation(
            temporalio.worker.StartNexusOperationInput(
                endpoint=temporalio.nexus.system.TEMPORAL_SYSTEM_ENDPOINT,
                service=_SERVICE,
                operation="ReadRecords",
                input=wake,
                output_type=stream_pb.ReadRecordsResponse,
                schedule_to_close_timeout=None,
                schedule_to_start_timeout=None,
                start_to_close_timeout=None,
                cancellation_type=_WAIT_REQUESTED,
                headers=None,
                summary=None,
            )
        )

    async def _temporal_stream_close(
        self, request: stream_pb.CloseStreamRequest
    ) -> None:
        # A System Nexus command, which the server carries out once the Workflow Task commits.
        # A task that fails after the close leaves the Stream open, and its retry closes it. The
        # server takes the closing run from the operation's links.
        close = stream_pb.CloseStreamRequest()
        close.CopyFrom(request)
        close.stream.ClearField("backend")
        handle = await self.next.start_system_nexus_operation(
            temporalio.worker.StartNexusOperationInput(
                endpoint=temporalio.nexus.system.TEMPORAL_SYSTEM_ENDPOINT,
                service=_SERVICE,
                operation="CloseStream",
                input=close,
                output_type=stream_pb.CloseStreamResponse,
                schedule_to_close_timeout=None,
                schedule_to_start_timeout=None,
                start_to_close_timeout=None,
                cancellation_type=_ABANDON,
                headers=None,
                summary=None,
            )
        )
        try:
            await handle
        except temporalio.exceptions.NexusOperationError as err:
            raise _workflow_reader.error_of(err) from None


class NativeStreamBackend(ExtensionStreamBackend):
    """Carries Stream calls to the Temporal server, which keeps the Streams.

    Every client, Worker and Replayer has it with no registration, under the name
    ``native``, and it is the default backend unless a registered backend has
    ``default=True``. Register one only to configure it, such as for its Retention, or to
    make it the default next to another backend.

    Workflow writes run as Local Activities, as on Redis, and the client's Stream runtime
    writes them to the server's ``temporal.api.streamservice.v1.StreamService``. Activity and
    client calls go to the same service. Each client gets its own runtime on its own
    connection, built at the first call, and the client's Workers share it. A page of a
    Workflow read is a direct read in a Local Activity whose marker holds only its positions,
    and a replay reads the page again, on the connection of the Replayer's ``stream_client``.

    .. warning::
        This API is experimental.
    """

    _kind = "native"
    _outbound_class = _NativeWorkflowOutbound
    _activity_streams = True
    # The server's `stream.maxBatchBytes` default, below the crate's 4 MiB.
    _max_batch_bytes = 2 * 1024 * 1024
    _nests_workflow_streams = True

    def __init__(
        self,
        *,
        retention: timedelta | None = None,
        name: str = "native",
        default: bool = False,
    ) -> None:
        """Create the backend. It makes no call. Each client of the backend calls the server
        on its own connection.

        Args:
            retention: The default Retention of a new Stream. Defaults to the server's.
            name: The backend name that Stream handles carry.
            default: Make this the backend of a create with no backend name. With more than
                one registered backend, exactly one has it.
        """
        config = ext_pb.NativeStreamConfig()
        if retention is not None:
            config.retention.FromTimedelta(retention)
        # The calls of each connection, which a client and its Workers share.
        self._connections: weakref.WeakKeyDictionary[
            temporalio.service.ServiceClient, _ExtensionCalls
        ] = weakref.WeakKeyDictionary()
        super().__init__(
            "temporalio.NativeStreamBackend", False, config, None, name, default
        )

    def _new_backend(
        self, service: temporalio.service.ServiceClient
    ) -> _ExtensionCalls:
        # Each connection gets its own runtime, so a call goes on the connection of the
        # client that makes it. The runtime comes at the first call.
        backend = self._connections.get(service)
        if backend is None:
            backend = _ExtensionCalls(_ConnectionRuntime(service, self._config))
            self._connections[service] = backend
            self._backends.add(backend)
        return backend

    def _backend_for(self, client: temporalio.client.Client) -> _ExtensionCalls:
        # The Stream runtime of the client's connection.
        return self._new_backend(client.service_client)

    async def _read_result(
        self,
        run_id: str,
        request: stream_pb.ReadRecordsRequest,
        page: stream_pb.ReadRecordsResponse,
        owner_ended: bool,  # pyright: ignore[reportUnusedParameter]
        converter: temporalio.converter.DataConverter,  # pyright: ignore[reportUnusedParameter]
    ) -> Any:
        # The marker holds the positions, and the Records wait as the Store gave them for the
        # codec step, which decodes them as on replay.
        answer = stream_pb.ReadRecordsResponse()
        answer.CopyFrom(page)
        answer.ClearField("records")
        positions = Payload(
            metadata={
                "encoding": b"binary/protobuf",
                "messageType": POSITIONS_TYPE,
                _COUNT_KEY: str(len(page.records)).encode(),
                _DIGEST_KEY: _digest(page.records),
                _RUN_KEY: run_id.encode(),
            },
            data=Payloads(
                payloads=[
                    Payload(data=request.SerializeToString()),
                    Payload(data=answer.SerializeToString()),
                ]
            ).SerializeToString(),
        )
        if page.records:
            key = (run_id, positions.data)
            _pages[key] = list(page.records)
            _pages.move_to_end(key)
            while len(_pages) > _CACHE_SIZE:
                _pages.popitem(last=False)
        return temporalio.common.RawValue(positions)

    async def _owner_ended(
        self,
        ref: stream_pb.StreamRef,  # pyright: ignore[reportUnusedParameter]
    ) -> bool:
        # The server closes a Workflow's Stream when its run chain ends.
        return False

    async def _close_in_workflow(self, request: stream_pb.CloseStreamRequest) -> int:
        # The close is a System Nexus command of the Workflow Task, which the server carries
        # out once the task commits, so the Worker polls no Activities for it.
        await _workflow_reader.reader_of(self.backend_name)._temporal_stream_close(
            request
        )
        return 0

    def _configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        # The calls come from the client's connection at the first call, so the client needs
        # no interceptor.
        return config


def wrap(
    backend: Callable[[], _ExtensionCalls],
    converter: temporalio.converter.DataConverter,
) -> temporalio.converter.DataConverter:
    """The converter with a codec step that turns a native read's positions back into its
    page, reading it again with ``backend`` when the page is not in the cache.
    """
    if isinstance(converter.payload_codec, _DirectReadCodec):
        return converter
    codec = converter.payload_codec
    return dataclasses.replace(
        converter, payload_codec=_DirectReadCodec(backend, codec, codec)
    )


_implicit: NativeStreamBackend | None = None


def implicit() -> NativeStreamBackend:
    """The native backend of the clients, Workers and Replayers that register none."""
    global _implicit
    if _implicit is None:
        _implicit = NativeStreamBackend()
    return _implicit
