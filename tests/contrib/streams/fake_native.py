"""An in-memory stand-in for the server's Stream service, until a dev server serves it.

The same store answers the frontend's gRPC calls of Activities and clients, and the System Nexus
calls of Workflow code through a Worker interceptor. The Workflow answers are not in History, so
these tests don't replay.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import grpc
import grpc.aio

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
import temporalio.converter
import temporalio.nexus.system
import temporalio.worker
import temporalio.workflow
from temporalio.api.workflowservice.v1 import (
    GetSystemInfoRequest,
    GetSystemInfoResponse,
    WorkflowServiceServicer,
    add_WorkflowServiceServicer_to_server,
)
from temporalio.contrib.streams.proto import v1 as ext_pb

SERVICE = "temporal.api.streamservice.v1.StreamService"


class FakeError(Exception):
    def __init__(self, code: grpc.StatusCode, reason: str) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason


@dataclass
class _Stream:
    records: list[stream_pb.StreamRecord] = field(default_factory=list)
    closed: bool = False
    # The last batch number of each Producer, for the dedupe of a retried call.
    sequences: dict[str, int] = field(default_factory=dict)


class FakeStreamService:
    """Keeps Streams by id, with offsets 1, 2, 3 and so on."""

    def __init__(self) -> None:
        self.streams: dict[str, _Stream] = {}
        self.calls: list[tuple[str, Any]] = []
        self._lock = threading.Lock()
        self._next_offset = 1

    def handle(self, operation: str, request: Any) -> Any:
        with self._lock:
            self.calls.append((operation, request))
            return getattr(self, f"_{operation}")(request)

    def _stream(self, ref: stream_pb.StreamRef) -> _Stream:
        stream = self.streams.get(ref.id)
        if stream is None:
            raise FakeError(grpc.StatusCode.NOT_FOUND, "stream_not_found")
        return stream

    def _add(self, stream: _Stream, record: stream_pb.StreamRecord) -> int:
        record.offset = self._next_offset
        self._next_offset += 1
        stream.records.append(record)
        return record.offset

    def _CreateStream(
        self, request: stream_pb.CreateStreamRequest
    ) -> stream_pb.CreateStreamResponse:
        known = self.streams.get(request.stream_id)
        if known is None:
            self.streams[request.stream_id] = _Stream()
        status = (
            stream_pb.StreamStatus.STREAM_STATUS_CLOSED
            if known is not None and known.closed
            else stream_pb.StreamStatus.STREAM_STATUS_OPEN
        )
        return stream_pb.CreateStreamResponse(
            stream=stream_pb.StreamRef(id=request.stream_id),
            created=known is None,
            status=status,
        )

    def _AppendRecords(
        self, request: stream_pb.AppendRecordsRequest
    ) -> stream_pb.AppendRecordsResponse:
        stream = self._stream(request.stream)
        if stream.closed:
            raise FakeError(grpc.StatusCode.FAILED_PRECONDITION, "stream_closed")
        if request.sequence and request.producer.id:
            if stream.sequences.get(request.producer.id, 0) >= request.sequence:
                return stream_pb.AppendRecordsResponse()
            stream.sequences[request.producer.id] = request.sequence
        offsets = [
            self._add(
                stream,
                stream_pb.StreamRecord(
                    kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA,
                    topic=record.topic,
                    body=record.body,
                    producer=request.producer,
                ),
            )
            for record in request.records
        ]
        return stream_pb.AppendRecordsResponse(
            first_offset=offsets[0] if offsets else 0,
            last_offset=offsets[-1] if offsets else 0,
        )

    def _CloseStream(
        self, request: stream_pb.CloseStreamRequest
    ) -> stream_pb.CloseStreamResponse:
        stream = self._stream(request.stream)
        if not stream.closed:
            stream.closed = True
            record = stream_pb.StreamRecord(
                kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_CLOSE
            )
            if request.HasField("result"):
                record.close_result.CopyFrom(request.result)
            if request.HasField("failure"):
                record.close_failure.CopyFrom(request.failure)
            self._add(stream, record)
        return stream_pb.CloseStreamResponse()

    def _ReadRecords(
        self, request: stream_pb.ReadRecordsRequest
    ) -> stream_pb.ReadRecordsResponse:
        stream = self._stream(request.stream)
        start = request.from_offset
        if not start and request.start == stream_pb.ReadStart.READ_START_END:
            start = self._next_offset
        topics, prefixes = list(request.topics), list(request.topic_prefixes)
        records = [
            record
            for record in stream.records
            if record.offset >= start
            and (
                record.kind != stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA
                or (not topics and not prefixes)
                or record.topic in topics
                or any(record.topic.startswith(prefix) for prefix in prefixes)
            )
        ]
        if request.max_records:
            records = records[: request.max_records]
        last = max((record.offset for record in stream.records), default=0)
        next_offset = records[-1].offset + 1 if records else max(start, last + 1)
        done = stream.closed and any(
            record.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_CLOSE
            for record in records
        )
        return stream_pb.ReadRecordsResponse(
            records=records, next_offset=next_offset, done=done
        )

    def _DescribeStream(
        self, request: stream_pb.DescribeStreamRequest
    ) -> stream_pb.DescribeStreamResponse:
        stream = self._stream(request.stream)
        return stream_pb.DescribeStreamResponse(
            status=stream_pb.StreamStatus.STREAM_STATUS_CLOSED
            if stream.closed
            else stream_pb.StreamStatus.STREAM_STATUS_OPEN,
            last_offset=max((record.offset for record in stream.records), default=0),
        )

    async def serve(self, operation: str, request: Any) -> Any:
        """Answer a frontend call. A read with a wait polls until Records or the wait end."""
        wait = (
            request.wait.ToTimedelta().total_seconds()
            if operation == "ReadRecords" and request.HasField("wait")
            else 0
        )
        create_wait = (
            request.create_wait.ToTimedelta().total_seconds()
            if operation == "ReadRecords" and request.HasField("create_wait")
            else 0
        )
        until = time.monotonic() + max(wait, create_wait)
        while True:
            try:
                response = self.handle(operation, request)
            except FakeError as err:
                if err.reason != "stream_not_found" or time.monotonic() >= until:
                    raise
            else:
                if (
                    operation != "ReadRecords"
                    or response.records
                    or time.monotonic() >= until
                ):
                    return response
            await asyncio.sleep(0.02)


class _SystemInfo(WorkflowServiceServicer):
    async def GetSystemInfo(  # type: ignore[override]
        self, request: GetSystemInfoRequest, context: grpc.aio.ServicerContext[Any, Any]
    ) -> GetSystemInfoResponse:
        return GetSystemInfoResponse()


_REQUESTS: dict[str, Any] = {
    "CreateStream": stream_pb.CreateStreamRequest,
    "AppendRecords": stream_pb.AppendRecordsRequest,
    "CloseStream": stream_pb.CloseStreamRequest,
    "ReadRecords": stream_pb.ReadRecordsRequest,
    "DescribeStream": stream_pb.DescribeStreamRequest,
}

_RESPONSES: dict[str, Any] = {
    "CreateStream": stream_pb.CreateStreamResponse,
    "AppendRecords": stream_pb.AppendRecordsResponse,
    "CloseStream": stream_pb.CloseStreamResponse,
    "ReadRecords": stream_pb.ReadRecordsResponse,
    "DescribeStream": stream_pb.DescribeStreamResponse,
}


async def start_frontend(service: FakeStreamService) -> tuple[grpc.aio.Server, int]:
    """A gRPC server with the Stream service and the system info call that a client needs."""
    server = grpc.aio.server()
    add_WorkflowServiceServicer_to_server(_SystemInfo(), server)  # type: ignore[abstract]

    def method(operation: str) -> grpc.RpcMethodHandler:
        async def call(
            request: Any, context: grpc.aio.ServicerContext[Any, Any]
        ) -> Any:
            assert dict(context.invocation_metadata() or ()).get(  # type: ignore[arg-type]
                "temporal-namespace"
            ), "the call has no temporal-namespace metadata"
            try:
                return await service.serve(operation, request)
            except FakeError as err:
                status = temporalio.api.common.v1.GrpcStatus(
                    code=err.code.value[0], message=err.reason
                )
                status.details.add().Pack(ext_pb.StreamErrorDetails(reason=err.reason))
                await context.abort(
                    err.code,
                    err.reason,
                    trailing_metadata=(
                        ("grpc-status-details-bin", status.SerializeToString()),
                    ),
                )

        return grpc.unary_unary_rpc_method_handler(
            call,
            request_deserializer=_REQUESTS[operation].FromString,
            response_serializer=_serialize,
        )

    server.add_generic_rpc_handlers(
        (
            grpc.method_handlers_generic_handler(
                SERVICE, {operation: method(operation) for operation in _REQUESTS}
            ),
        )
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


def _serialize(message: Any) -> bytes:
    return message.SerializeToString()


class FakeSystemNexus(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    """Answers the Workflow's System Nexus Stream calls from the store, as the server would.

    Put it on the client before the Stream plugins. The interceptor that comes first wraps the
    outbound chain first, so it is the last outbound interceptor, next to the SDK's.
    """

    def __init__(self, service: FakeStreamService) -> None:
        super().__init__()
        self._service = service

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        service = self._service

        class Inbound(temporalio.worker.WorkflowInboundInterceptor):
            def init(
                self, outbound: temporalio.worker.WorkflowOutboundInterceptor
            ) -> None:
                super().init(_FakeOutbound(outbound, service))

        return Inbound


class _FakeOutbound(temporalio.worker.WorkflowOutboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.WorkflowOutboundInterceptor,
        service: FakeStreamService,
    ) -> None:
        super().__init__(next)
        self._service = service

    async def start_system_nexus_operation(
        self, input: temporalio.worker.StartNexusOperationInput[Any, Any]
    ) -> temporalio.workflow.NexusOperationHandle[Any]:
        if input.service != SERVICE:
            return await super().start_system_nexus_operation(input)
        converter = temporalio.nexus.system._get_payload_converter(
            temporalio.workflow.payload_converter(),
            temporalio.converter.DefaultFailureConverter(),
        )
        request = _REQUESTS[input.operation_name].FromString(
            converter.to_payload(input.input).data
        )
        request.namespace = temporalio.workflow.info().namespace
        response = self._service.handle(input.operation_name, request)
        payload = temporalio.api.common.v1.Payload(
            metadata={
                "encoding": b"binary/protobuf",
                "messageType": response.DESCRIPTOR.full_name.encode(),
            },
            data=response.SerializeToString(),
        )
        # Yield once, as a real operation waits for the server, so a caller must not need the
        # answer in the first step.
        await asyncio.sleep(0)
        return _Answered(converter.from_payload(payload, input.output_type or Any))


class _Answered(temporalio.workflow.NexusOperationHandle[Any]):
    def __init__(self, result: Any) -> None:
        self._result = result

    def cancel(self) -> bool:
        return False

    def __await__(self) -> Any:
        return self._value().__await__()

    async def _value(self) -> Any:
        return self._result

    @property
    def operation_token(self) -> str | None:
        return None


def records_of(
    service: FakeStreamService, stream_id: str, decode: Callable[[Any], Any]
) -> list[Any]:
    """The decoded data Records of a Stream, and ``closed`` with its result at the end."""
    items: list[Any] = []
    for record in service.streams[stream_id].records:
        if record.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA:
            items.append(decode(record.body))
        elif record.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_CLOSE:
            items.append(("closed", decode(record.close_result)))
    return items
