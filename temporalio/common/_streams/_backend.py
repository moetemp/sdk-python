"""Calls to the temporalio-streams runtime in the bridge, for Activities and clients."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol, TypeVar

import google.protobuf.message

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.bridge.streams
import temporalio.converter
import temporalio.service
from temporalio.common._streams import _errors
from temporalio.common._streams.proto import v1 as ext_pb
from temporalio.common._streams.system import client as service_client

if TYPE_CHECKING:
    # The writer bindings import this package's protos, so they load after it.
    from temporalio.common._streams.writer_system import client as writer_client

_M = TypeVar("_M", bound=google.protobuf.message.Message)

_REQUESTS: dict[str, type[google.protobuf.message.Message]] = {
    "CreateStream": stream_pb.CreateStreamRequest,
    "AppendRecords": stream_pb.AppendRecordsRequest,
    "CloseStream": stream_pb.CloseStreamRequest,
    "ReadRecords": stream_pb.ReadRecordsRequest,
    "DescribeStream": stream_pb.DescribeStreamRequest,
    "OpenWriter": ext_pb.OpenWriterRequest,
    "WriteRecords": ext_pb.WriteRecordsRequest,
    "CloseWriter": ext_pb.CloseWriterRequest,
    "FreeWriter": ext_pb.FreeWriterRequest,
}

_RESPONSES: dict[str, type[google.protobuf.message.Message]] = {
    "CreateStream": stream_pb.CreateStreamResponse,
    "AppendRecords": stream_pb.AppendRecordsResponse,
    "CloseStream": stream_pb.CloseStreamResponse,
    "ReadRecords": stream_pb.ReadRecordsResponse,
    "DescribeStream": stream_pb.DescribeStreamResponse,
    "OpenWriter": ext_pb.OpenWriterResponse,
    "WriteRecords": ext_pb.WriteRecordsResponse,
    "CloseWriter": ext_pb.CloseWriterResponse,
    "FreeWriter": ext_pb.FreeWriterResponse,
}


@dataclass(frozen=True)
class CallOptions:
    """The ``rpc_metadata`` and ``rpc_timeout`` of a client's Stream call."""

    metadata: Mapping[str, str | bytes]
    timeout: timedelta | None


# The options of the client call in progress. Each transport reads them, so they reach the
# generated clients' calls and the writer's with no new argument there.
call_options: contextvars.ContextVar[CallOptions | None] = contextvars.ContextVar(
    "__temporal_stream_call_options", default=None
)


def options_of(
    rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
) -> CallOptions | None:
    """The call options, or None for a call with neither."""
    if not rpc_metadata and rpc_timeout is None:
        return None
    return CallOptions(metadata=rpc_metadata, timeout=rpc_timeout)


@contextlib.contextmanager
def using(options: CallOptions | None) -> Iterator[None]:
    """Give the calls in the block these options."""
    if options is None:
        yield
        return
    token = call_options.set(options)
    try:
        yield
    finally:
        call_options.reset(token)


def longest_wait(longest: timedelta) -> timedelta:
    """The longest wait of one call. It is less than ``longest`` to answer before the call's
    rpc_timeout.
    """
    options = call_options.get()
    if options is None or options.timeout is None:
        return longest
    # A long poll answers before the call's deadline, as the server does for its own long
    # polls, so a read that waits goes on with its next poll and does not fail.
    timeout = options.timeout
    return min(longest, max(timeout - timedelta(seconds=1), timeout / 2))


class BackendCalls(Protocol):
    """The calls that a Stream backend gives the handles of one client."""

    def service(
        self,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> service_client.StreamServiceClient:
        """The Stream service for ``namespace``."""
        ...

    def writers(
        self, namespace: str, data_converter: temporalio.converter.DataConverter
    ) -> writer_client.StreamWriterClient:
        """The writers of Activity and client Producers for ``namespace``."""
        ...

    def free_writer(self, writer_id: int) -> None:
        """Release one handle of a writer with no wait, for a writer that Python drops."""
        ...


class StreamRuntime(Protocol):
    """The calls of the crate's runtime object, which the bridge builds."""

    async def call(self, method: str, input: bytes) -> bytes:
        """Call a method with encoded input, and give the encoded output."""
        ...

    def free_writer(self, writer_id: int) -> None:
        """Release one handle of a writer with no wait."""
        ...


class _ExtensionCalls:
    """Carries Stream calls to the crate's runtime object, which serves one client.

    The five Stream service operations go through the generated client, and the writer
    methods are the crate's own.
    """

    def __init__(self, runtime: StreamRuntime) -> None:
        """Create a backend over a runtime object."""
        self._runtime = runtime

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

    def writers(
        self, namespace: str, data_converter: temporalio.converter.DataConverter
    ) -> writer_client.StreamWriterClient:
        """The generated writer client over the runtime."""
        from temporalio.common._streams.writer_system import client

        return client.StreamWriterClient(
            _ExtensionTransport(self, namespace), data_converter=data_converter
        )

    def free_writer(self, writer_id: int) -> None:
        """Release one handle of a writer with no wait."""
        try:
            self._runtime.free_writer(writer_id)
        except temporalio.bridge.streams.StreamCallError:
            # A writer that the runtime already dropped, such as at shutdown, has nothing to
            # release.
            pass

    async def call_async(
        self, method: str, request: google.protobuf.message.Message, response: type[_M]
    ) -> _M:
        """Call a method of the runtime."""
        try:
            data = await self._runtime.call(method, request.SerializeToString())
        except temporalio.bridge.streams.StreamCallError as err:
            raise _errors.from_call_error(err) from None
        return response.FromString(data)

    async def close_writers(self) -> int:
        """Drop every open writer of the runtime, and return how many failed."""
        response = await self.call_async(
            "CloseWriters", ext_pb.CloseWritersRequest(), ext_pb.CloseWritersResponse
        )
        return response.failed


class _ExtensionTransport:
    # The generated client takes a transport, so the five operations reach the runtime as they
    # would reach a server later.
    def __init__(self, backend: _ExtensionCalls, namespace: str) -> None:
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
        if "namespace" in request.DESCRIPTOR.fields_by_name:
            setattr(request, "namespace", self._namespace)
        # The runtime makes no RPC, so it has no use for rpc_metadata. The rpc_timeout is the
        # call's deadline here too.
        options = call_options.get()
        timeout = options.timeout if options is not None else None
        call = self._backend.call_async(operation, request, _RESPONSES[operation])
        if timeout is None:
            response = await call
        else:
            try:
                response = await asyncio.wait_for(call, timeout.total_seconds())
            except asyncio.TimeoutError:
                raise temporalio.service.RPCError(
                    f"The Stream call {operation} passed its rpc_timeout of {timeout}",
                    temporalio.service.RPCStatusCode.DEADLINE_EXCEEDED,
                    b"",
                ) from None
        return temporalio.api.common.v1.Payload(
            metadata={
                "encoding": b"binary/protobuf",
                "messageType": response.DESCRIPTOR.full_name.encode(),
            },
            data=response.SerializeToString(),
        )
