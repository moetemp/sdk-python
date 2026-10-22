"""Calls to the native Stream extension, for Activities and clients."""

from __future__ import annotations

import weakref
from datetime import timedelta
from typing import Any, TypeVar

import google.protobuf.message

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
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


class StreamBackend:
    """Carries Stream calls to a worker extension with the temporalio-streams methods.

    A Stream plugin gives one to the facade. The five Stream service operations go through the
    generated client, and the writer methods are the crate's own.

    .. warning::
        This API is experimental.
    """

    def __init__(self, extension: temporalio.worker.WorkerExtension) -> None:
        """Create a backend over an extension."""
        self._extension = extension
        # Writers whose open or encode may still run on the loop, for close_writers.
        self.writers: weakref.WeakSet[Any] = weakref.WeakSet()

    @property
    def extension(self) -> temporalio.worker.WorkerExtension:
        """The extension that the backend calls."""
        return self._extension

    def service(self, namespace: str) -> service_client.StreamServiceClient:
        """A generated Stream service client for ``namespace``."""
        return service_client.StreamServiceClient(_ExtensionTransport(self, namespace))

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

    def publish(
        self, writer_id: int, records: list[stream_pb.AppendRecord], force_flush: bool
    ) -> None:
        """Add encoded Records to a writer's buffer."""
        self.call(
            "Publish",
            ext_pb.PublishRequest(
                writer_id=writer_id, records=records, force_flush=force_flush
            ),
        )

    async def write_records(
        self, writer_id: int, records: list[stream_pb.AppendRecord]
    ) -> int:
        """Write Records at once for a writer, after its buffered ones. Returns the last
        Record's offset.
        """
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

    async def flush_writer(self, writer_id: int) -> int:
        """Wait until the writer's Records are in the Store."""
        response = await self.call_async(
            "FlushWriter",
            ext_pb.FlushWriterRequest(writer_id=writer_id),
            ext_pb.FlushWriterResponse,
        )
        return response.last_offset

    async def close_writer(self, writer_id: int) -> int:
        """Flush and drop a writer. The Stream stays open."""
        response = await self.call_async(
            "CloseWriter",
            ext_pb.CloseWriterRequest(writer_id=writer_id),
            ext_pb.CloseWriterResponse,
        )
        return response.last_offset

    async def close_writers(self) -> int:
        """Flush and drop every open writer of the extension, and return how many failed."""
        failed = 0
        for writer in list(self.writers):
            try:
                await writer.settle()
            except Exception:
                failed += 1
        response = await self.call_async(
            "CloseWriters", ext_pb.CloseWritersRequest(), ext_pb.CloseWritersResponse
        )
        return failed + response.failed

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
    def __init__(self, backend: StreamBackend, namespace: str) -> None:
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


def backend_of_client(client: temporalio.client.Client) -> StreamBackend:
    """The backend that a client's Stream plugin gives."""
    for plugin in client.config()["plugins"]:
        backend = getattr(plugin, "_temporal_stream_backend", None)
        if isinstance(backend, StreamBackend):
            return backend
    raise _errors.StreamTransportMissingError(
        "The client has no Stream plugin, such as RedisStreamPlugin"
    )


def client_backend(
    client: temporalio.client.Client, name: str | None
) -> tuple[str, StreamBackend]:
    """The name and the backend of a client's Stream plugin. None picks the first plugin.

    Raises:
        StreamTransportMissingError: The client has no Stream plugin.
        StreamUnsupportedError: The client has no Stream plugin of that name.
    """
    backends = [
        (getattr(plugin, "backend_name", ""), backend)
        for plugin in client.config()["plugins"]
        if isinstance(
            backend := getattr(plugin, "_temporal_stream_backend", None), StreamBackend
        )
    ]
    if not backends:
        raise _errors.StreamTransportMissingError(
            "The client has no Stream plugin, such as RedisStreamPlugin"
        )
    if not name:
        return backends[0]
    for backend_name, backend in backends:
        if backend_name == name:
            return backend_name, backend
    raise _errors.StreamUnsupportedError(
        f"The client has no Stream backend {name}. It has "
        f"{', '.join(backend_name for backend_name, _ in backends)}.",
        reason="stream_unsupported",
    )
