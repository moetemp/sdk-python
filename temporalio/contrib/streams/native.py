"""The native Stream plugin, whose Streams the Temporal server keeps.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections import OrderedDict
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import temporalio.activity
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.nexus.system
import temporalio.runtime
import temporalio.worker
import temporalio.workflow
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.contrib.streams import _errors, _workflow_reader
from temporalio.contrib.streams._backend import _ExtensionStreamBackend
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import (
    _decode_page,
    _ExtensionStreamPlugin,
    _WorkflowOutbound,
    page_payload,
)

NATIVE_EXTENSION_NAME = "temporalio-streams-native"
_SERVICE = "temporal.api.streamservice.v1.StreamService"

# Nothing waits for the answer of a wake that the reader dropped, so its cancel ends it at once.
_TRY_CANCEL = temporalio.workflow.NexusOperationCancellationType.TRY_CANCEL
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
# The pages that this process read and no Workflow Task took yet. A run's pages go when the run
# leaves the Worker, and the bound covers runs that never took theirs.
_CACHE_SIZE = 256
RANGE_GONE_TYPE = "STREAM_RANGE_GONE"
_DESCRIBE_TIMEOUT = timedelta(seconds=10)


def _describe_activity_name(backend: str) -> str:
    return f"__temporal_stream_describe:{backend}"


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
        plugin: NativeStreamPlugin,
        inner: temporalio.converter.PayloadCodec | None,
        records: temporalio.converter.PayloadCodec | None,
    ) -> None:
        self._plugin = plugin
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
        return _DirectReadCodec(self._plugin, inner, self._records)

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
        records: Sequence[stream_pb.StreamRecord] | None = self._plugin._pages.pop(
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
            page = await self._plugin._backend.call_async(
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


class _NativeBackend(_ExtensionStreamBackend):
    def for_client(self, client: temporalio.client.Client) -> _NativeBackend:
        # A process with no Worker gives the extension its client's connection. A Worker
        # gives it the Worker's at its start.
        self._extension.attach(client)
        return self


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
                cancellation_type=_TRY_CANCEL,
                headers=None,
                summary=None,
            )
        )

    async def _temporal_stream_describe(
        self, request: stream_pb.DescribeStreamRequest
    ) -> stream_pb.DescribeStreamResponse:
        try:
            return await temporalio.workflow.execute_local_activity(
                _describe_activity_name(self._temporal_stream_backend_name),
                request,
                result_type=stream_pb.DescribeStreamResponse,
                start_to_close_timeout=_DESCRIBE_TIMEOUT,
            )
        except (
            temporalio.exceptions.ActivityError,
            temporalio.exceptions.ApplicationError,
        ) as err:
            raise _workflow_reader.error_of(err) from None


class NativeStreamPlugin(_ExtensionStreamPlugin):
    """Carries Stream calls to the Temporal server, which keeps the Streams.

    Workflow writes run as Local Activities, as on Redis, and the extension writes them to the
    server's ``temporal.api.streamservice.v1.StreamService`` on the Worker's connection.
    Activity and client calls go to the same service through the extension. A page of a
    Workflow read is a direct read in a Local Activity whose marker holds only its positions,
    and a replay reads the page again. It registers the backend name ``native``. Put it on the
    client next to ``RedisStreamPlugin`` to keep old Streams on Redis and create new ones on
    the server.

    .. warning::
        This API is experimental.
    """

    _backend_class = _NativeBackend
    _outbound_class = _NativeWorkflowOutbound
    _server_closes_with_run = True

    async def _read_result(
        self,
        run_id: str,
        request: stream_pb.ReadRecordsRequest,
        page: stream_pb.ReadRecordsResponse,
        owner_ended: bool,  # pyright: ignore[reportUnusedParameter]
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
            self._pages[key] = list(page.records)
            self._pages.move_to_end(key)
            while len(self._pages) > _CACHE_SIZE:
                self._pages.popitem(last=False)
        return temporalio.common.RawValue(positions)

    def _forget_run(self, run_id: str) -> None:
        for key in [key for key in self._pages if key[0] == run_id]:
            del self._pages[key]

    def _activities(self) -> list[Any]:
        return [*super()._activities(), _describe_activity(self)]

    def configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        """Wrap the client's codec, so a Workflow read's positions turn back into its page."""
        config = super().configure_client(config)
        converter = (
            config.get("data_converter") or temporalio.converter.DataConverter.default
        )
        config["data_converter"] = _wrap(self, converter)
        return config

    def configure_replayer(
        self, config: temporalio.worker.ReplayerConfig
    ) -> temporalio.worker.ReplayerConfig:
        """Wrap the Replayer's codec, as the client's. A replay reads again over the
        extension's connection, so use the plugin of a client or Worker that connected.
        """
        config = super().configure_replayer(config)
        converter = (
            config.get("data_converter") or temporalio.converter.DataConverter.default
        )
        config["data_converter"] = _wrap(self, converter)
        return config

    def __init__(
        self,
        *,
        retention: timedelta | None = None,
        runtime: temporalio.runtime.Runtime | None = None,
        name: str = "native",
        default: bool = False,
    ) -> None:
        """Create the plugin. It makes no call.

        Args:
            retention: The default Retention of a new Stream. Defaults to the server's.
            runtime: The runtime of the client. Defaults to the default runtime.
            name: The backend name that Stream handles carry.
            default: Make this the default backend when the client or Worker has more than
                one Stream plugin. With none marked, the first one is the default.
        """
        config = ext_pb.NativeStreamConfig()
        if retention is not None:
            config.retention.FromTimedelta(retention)
        self._pages: OrderedDict[tuple[str, bytes], list[stream_pb.StreamRecord]] = (
            OrderedDict()
        )
        super().__init__(
            "temporalio.NativeStreamPlugin",
            NATIVE_EXTENSION_NAME,
            config,
            runtime,
            name,
            default,
        )


def _describe_activity(plugin: NativeStreamPlugin) -> Any:
    # A Workflow reader of a standalone Stream learns its Retention, since a replay reads the
    # Stream again and Retention trims it.
    @temporalio.activity.defn(name=_describe_activity_name(plugin.backend_name))
    async def describe(
        request: stream_pb.DescribeStreamRequest,
    ) -> stream_pb.DescribeStreamResponse:
        request.namespace = temporalio.activity.info().namespace
        try:
            return await plugin._backend.call_async(
                "DescribeStream", request, stream_pb.DescribeStreamResponse
            )
        except _errors.StreamError as err:
            raise temporalio.exceptions.ApplicationError(
                str(err.message),
                err.reason,
                type=type(err).__name__,
                non_retryable=err.non_retryable,
            ) from None

    return describe


def _wrap(
    plugin: NativeStreamPlugin, converter: temporalio.converter.DataConverter
) -> temporalio.converter.DataConverter:
    if isinstance(converter.payload_codec, _DirectReadCodec):
        return converter
    codec = converter.payload_codec
    return dataclasses.replace(
        converter, payload_codec=_DirectReadCodec(plugin, codec, codec)
    )
