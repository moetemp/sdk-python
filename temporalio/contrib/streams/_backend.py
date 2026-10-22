"""Calls to the native Stream extension, for Activities and clients."""

from __future__ import annotations

from datetime import timedelta
from typing import Protocol, TypeVar

import google.protobuf.message

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
import temporalio.worker
from temporalio.contrib.streams import _errors
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.system import client as service_client

_M = TypeVar("_M", bound=google.protobuf.message.Message)

_REQUESTS: dict[str, type[google.protobuf.message.Message]] = {
    "CreateStream": stream_pb.CreateStreamRequest,
    "AppendRecords": stream_pb.AppendRecordsRequest,
    "CloseStream": stream_pb.CloseStreamRequest,
    "ReadRecords": stream_pb.ReadRecordsRequest,
    "DescribeStream": stream_pb.DescribeStreamRequest,
}

_RESPONSES: dict[str, type[google.protobuf.message.Message]] = {
    "CreateStream": stream_pb.CreateStreamResponse,
    "AppendRecords": stream_pb.AppendRecordsResponse,
    "CloseStream": stream_pb.CloseStreamResponse,
    "ReadRecords": stream_pb.ReadRecordsResponse,
    "DescribeStream": stream_pb.DescribeStreamResponse,
}


class StreamBackend(Protocol):
    """What a Stream plugin gives the handles. Not public. Each backend has its own class."""

    def service(
        self,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> service_client.StreamServiceClient:
        """The Stream service for ``namespace``."""
        ...

    async def open_writer(self, request: ext_pb.OpenWriterRequest) -> int:
        """Open the writer of an Activity or client Producer, and return its id."""
        ...

    async def write_records(
        self, writer_id: int, records: list[stream_pb.AppendRecord]
    ) -> int:
        """Write Records for a writer, and return the last Record's offset."""
        ...

    async def close_writer(self, writer_id: int) -> int:
        """Drop a writer. The Stream stays open."""
        ...

    async def flush_run_buffer(
        self, namespace: str, run_id: str, workflow_task: stream_pb.WorkflowTaskEpoch
    ) -> int:
        """Write a run's buffered calls before a Local Activity of the run starts."""
        ...


class _ExtensionStreamBackend:
    """Carries Stream calls to a worker extension with the temporalio-streams methods.

    The five Stream service operations go through the generated client, and the writer
    methods are the crate's own.
    """

    def __init__(self, extension: temporalio.worker.WorkerExtension) -> None:
        """Create a backend over an extension."""
        self._extension = extension

    def service(
        self,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> service_client.StreamServiceClient:
        """A generated Stream service client for ``namespace``.

        With a ``data_converter``, user values in the requests go through it and its codec.
        """
        return service_client.StreamServiceClient(
            _ExtensionTransport(self, namespace), data_converter=data_converter
        )

    def call(self, method: str, request: google.protobuf.message.Message) -> bytes:
        """Call a synchronous extension method."""
        try:
            return self._extension.call(method, request.SerializeToString())
        except temporalio.worker.ExtensionError as err:
            raise _errors.from_extension_error(err) from None

    async def call_async(
        self, method: str, request: google.protobuf.message.Message, response: type[_M]
    ) -> _M:
        """Call an async extension method."""
        try:
            data = await self._extension.call_async(method, request.SerializeToString())
        except temporalio.worker.ExtensionError as err:
            raise _errors.from_extension_error(err) from None
        return response.FromString(data)

    async def open_writer(self, request: ext_pb.OpenWriterRequest) -> int:
        """Open a buffered writer and return its id."""
        response = await self.call_async(
            "OpenWriter", request, ext_pb.OpenWriterResponse
        )
        return response.writer_id

    async def write_records(
        self, writer_id: int, records: list[stream_pb.AppendRecord]
    ) -> int:
        """Write Records at once for a writer. Returns the last Record's offset."""
        response = await self.call_async(
            "WriteRecords",
            ext_pb.WriteRecordsRequest(writer_id=writer_id, records=records),
            ext_pb.WriteRecordsResponse,
        )
        return response.last_offset

    async def flush_run_buffer(
        self, namespace: str, run_id: str, workflow_task: stream_pb.WorkflowTaskEpoch
    ) -> int:
        """Write a run's buffered Workflow calls before a Local Activity of the run starts.

        Returns how many Records it wrote.
        """
        response = await self.call_async(
            "FlushRunBuffer",
            ext_pb.FlushRunBufferRequest(
                namespace=namespace, run_id=run_id, workflow_task=workflow_task
            ),
            ext_pb.FlushRunBufferResponse,
        )
        return response.records

    async def close_writer(self, writer_id: int) -> int:
        """Flush and drop a writer. The Stream stays open."""
        response = await self.call_async(
            "CloseWriter",
            ext_pb.CloseWriterRequest(writer_id=writer_id),
            ext_pb.CloseWriterResponse,
        )
        return response.last_offset

    async def close_writers(self) -> int:
        """Drop every open writer of the extension, and return how many failed."""
        response = await self.call_async(
            "CloseWriters", ext_pb.CloseWritersRequest(), ext_pb.CloseWritersResponse
        )
        return response.failed

    async def drain_closes(self, timeout: timedelta) -> int:
        """Wait for the Workflow closes in flight, and return how many are left."""
        request = ext_pb.DrainClosesRequest()
        request.timeout.FromTimedelta(timeout)
        response = await self.call_async(
            "DrainCloses", request, ext_pb.DrainClosesResponse
        )
        return response.pending


class _ExtensionTransport:
    # The generated client takes a transport, so the five operations reach the extension as
    # they would reach a server later.
    def __init__(self, backend: _ExtensionStreamBackend, namespace: str) -> None:
        self._backend = backend
        self._namespace = namespace

    async def call(
        self,
        *,
        service: str,  # pyright: ignore[reportUnusedParameter]
        operation: str,
        input: temporalio.api.common.v1.Payload,
    ) -> temporalio.api.common.v1.Payload:
        request = _REQUESTS[operation].FromString(input.data)
        # The IDL leaves the namespace out, since the server knows it.
        setattr(request, "namespace", self._namespace)
        response = await self._backend.call_async(
            operation, request, _RESPONSES[operation]
        )
        return temporalio.api.common.v1.Payload(
            metadata={
                "encoding": b"binary/protobuf",
                "messageType": response.DESCRIPTOR.full_name.encode(),
            },
            data=response.SerializeToString(),
        )
