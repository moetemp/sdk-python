"""Calls to the temporalio-streams runtime in the bridge, for Activities and clients."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
import weakref
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast

import google.protobuf.message
import nexusrpc
from typing_extensions import override

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.bridge.proto.streams as runtime_pb
import temporalio.bridge.streams
import temporalio.client
import temporalio.converter
import temporalio.nexus.system
import temporalio.service
from temporalio.converter._payload_converter import _get_transfer_type_converter
from temporalio.streams import _errors
from temporalio.streams.system import client as service_client
from temporalio.streams.system import models
from temporalio.streams.system.services import StreamService

if TYPE_CHECKING:
    # Only a writer needs the writer bindings, so _writers_class() imports them on first use.
    from temporalio.streams.writer_system import client as writer_client

_M = TypeVar("_M", bound=google.protobuf.message.Message)
_OutputT = TypeVar("_OutputT")
_T = TypeVar("_T")

_RESPONSES: dict[str, type[google.protobuf.message.Message]] = {
    "CreateStream": stream_pb.CreateStreamResponse,
    "AppendRecords": stream_pb.AppendRecordsResponse,
    "CloseStream": stream_pb.CloseStreamResponse,
    "ReadRecords": stream_pb.ReadRecordsResponse,
    "DescribeStream": stream_pb.DescribeStreamResponse,
}


@dataclass(frozen=True)
class CallOptions:
    """The ``rpc_metadata`` and ``rpc_timeout`` of a client's Stream call."""

    metadata: Mapping[str, str | bytes]
    timeout: timedelta | None


# The options of the client call in progress. The runtime's Nexus client reads them, so the
# generated clients and the writer don't need a new argument for them.
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
    """Apply these options to the calls in the block."""
    if options is None:
        yield
        return
    token = call_options.set(options)
    try:
        yield
    finally:
        call_options.reset(token)


def longest_wait(longest: timedelta) -> timedelta:
    """The longest wait of one call, at most ``longest`` and below the call's rpc_timeout."""
    options = call_options.get()
    if options is None or options.timeout is None:
        return longest
    # A long poll returns before the call's deadline, as the server's long polls do, so a
    # waiting read starts its next poll and doesn't fail.
    timeout = options.timeout
    return min(longest, max(timeout - timedelta(seconds=1), timeout / 2))


class BackendCalls(Protocol):
    """The calls that a Stream backend gives the Streams of one client."""

    def service(
        self,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> StreamServiceCalls:
        """The Stream service for ``namespace``."""
        ...

    def writers(
        self, namespace: str, data_converter: temporalio.converter.DataConverter
    ) -> writer_client.StreamWriterClientExtension:
        """The writers of Activity and client Producers for ``namespace``."""
        ...


class StreamWriterObject(Protocol):
    """One writer of the crate's runtime. Dropping the last reference releases it."""

    async def write(self, input: bytes) -> bytes:
        """Write an encoded ``WriteRecordsRequest``, and return the encoded response."""
        ...

    async def close(self) -> bytes:
        """Close the writer, and return the encoded ``CloseWriterResponse``."""
        ...


class StreamRuntime(Protocol):
    """The calls of the crate's runtime object, which the bridge builds."""

    async def call(self, method: str, input: bytes) -> bytes:
        """Call a method with encoded input, and return the encoded output."""
        ...

    async def open_writer(self, input: bytes) -> StreamWriterObject:
        """Open a writer from an encoded ``OpenWriterRequest``."""
        ...


async def _answer(call: Awaitable[_T]) -> _T:
    # A failed call of the runtime or a writer carries its Stream error.
    try:
        return await call
    except temporalio.bridge.streams.StreamCallError as err:
        stream_error = _errors.from_failure(err)
        if stream_error is None:
            raise
        raise stream_error from None


class _ExtensionCalls:
    """Makes Stream calls to the crate's runtime object, which serves one client.

    The generated client starts the Stream service operations, and its Nexus client is the
    runtime. A writer is an object of the runtime.
    """

    def __init__(self, runtime: StreamRuntime) -> None:
        """Create a backend over a runtime object."""
        self._runtime = runtime
        # The open writers, with the loop that opened each, so a shutdown can close them. A
        # writer that Python frees leaves it, and the drop releases the crate's writer.
        self._writers: weakref.WeakKeyDictionary[
            StreamWriterObject, asyncio.AbstractEventLoop
        ] = weakref.WeakKeyDictionary()

    def service(
        self,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> StreamServiceCalls:
        """The generated Stream service client for ``namespace``.

        With a ``data_converter``, the client encodes user values in the requests with it and
        its codec.
        """
        return StreamServiceCalls(_RuntimeNexusClient(self, namespace, data_converter))

    def writers(
        self, namespace: str, data_converter: temporalio.converter.DataConverter
    ) -> writer_client.StreamWriterClientExtension:
        """The writer client over the runtime."""
        return _writers_class()(_RuntimeNexusClient(self, namespace, data_converter))

    async def call_async(
        self, method: str, request: google.protobuf.message.Message, response: type[_M]
    ) -> _M:
        """Call a method of the runtime."""
        data = await _answer(self._runtime.call(method, request.SerializeToString()))
        return response.FromString(data)

    async def open_writer(
        self, request: google.protobuf.message.Message
    ) -> StreamWriterObject:
        """Open a writer of the runtime. It stays on the running loop."""
        writer = await _answer(self._runtime.open_writer(request.SerializeToString()))
        self._writers[writer] = asyncio.get_running_loop()
        return writer

    async def close_writers(self) -> int:
        """Close the open writers of the running loop, and return how many failed.

        A writer of another loop is released when Python frees it.
        """
        loop = asyncio.get_running_loop()
        failed = 0
        for writer, owner in list(self._writers.items()):
            if owner is not loop:
                continue
            try:
                await _answer(writer.close())
            except Exception:
                failed += 1
        return failed


class StreamServiceCalls(service_client.StreamServiceClientExtension):
    """The generated Stream service client over the runtime.

    The generated client builds each request of a Stream's calls from its own sources. The SDK
    builds some requests itself, such as a close with a writer's Producer, and sends them with
    the ``send_`` calls.
    """

    def __init__(self, nexus_client: _RuntimeNexusClient) -> None:
        """Create the client over the runtime's Nexus client."""
        self._nexus_client = nexus_client

    @override
    def create_nexus_client(
        self, service: type[Any] | str, endpoint: str
    ) -> temporalio.client.NexusClient[Any]:
        """Return the runtime's Nexus client, which serves every operation in process."""
        del service, endpoint
        # It has only the start that the generated clients call.
        runtime: Any = self._nexus_client
        return runtime

    async def send_append_records(
        self, request: models.AppendRecordsRequest[Any]
    ) -> models.AppendRecordsResponse:
        """Send an append."""
        return await self._send(StreamService.append_records, request)

    async def send_close_stream(
        self, request: models.CloseStreamRequest
    ) -> models.CloseStreamResponse:
        """Send a close."""
        return await self._send(StreamService.close_stream, request)

    async def send_read_records(
        self, request: models.ReadRecordsRequest, body_type: type[Any]
    ) -> models.ReadRecordsResponse[Any]:
        """Send a read of one page. Each Record body decodes with ``body_type``."""
        handle = await self._nexus_client.start_operation(
            "ReadRecords",
            request,
            id="",
            result_type=cast(Any, models.ReadRecordsResponse)[body_type],
        )
        return await handle.result()

    async def send_describe_stream(
        self, request: models.DescribeStreamRequest
    ) -> models.DescribeStreamResponse:
        """Send a describe."""
        return await self._send(StreamService.describe_stream, request)

    async def _send(
        self, operation: nexusrpc.Operation[Any, _OutputT], request: Any
    ) -> _OutputT:
        handle = await self._nexus_client.start_operation(operation, request, id="")
        return await handle.result()


@functools.cache
def _writers_class() -> Callable[
    [_RuntimeNexusClient], writer_client.StreamWriterClientExtension
]:
    from temporalio.streams.writer_system import client

    class _StreamWriters(client.StreamWriterClientExtension):
        def __init__(self, nexus_client: _RuntimeNexusClient) -> None:
            self._nexus_client = nexus_client

        @override
        def writer_calls(self) -> client.WriterCalls:
            return self._nexus_client

    return _StreamWriters


class _RuntimeNexusClient:
    # The generated client starts each operation on a Nexus client. This one starts it on the
    # runtime in place of an endpoint, and the runtime answers before the start returns. The
    # writer calls encode their models the same way.
    def __init__(
        self,
        backend: _ExtensionCalls,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None,
    ) -> None:
        self._backend = backend
        self._namespace = namespace
        self._data_converter = (
            data_converter or temporalio.converter.DataConverter.default
        )

    def _converters(self) -> contextlib.AbstractContextManager[None]:
        # A model's user values and failures convert with the client's converters, as in
        # Workflow code.
        return temporalio.nexus.system._user_converter_context(
            temporalio.nexus.system._SystemNexusUserConverters(
                payload_converter=self._data_converter.payload_converter,
                failure_converter=self._data_converter.failure_converter,
            )
        )

    async def start_operation(
        self,
        operation: nexusrpc.Operation[Any, Any] | str,
        arg: Any,
        *,
        id: str,
        result_type: type[Any] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> _Answered:
        # The runtime makes no RPC and keeps no operation, so it has no use for the id and the
        # metadata.
        del id, rpc_metadata
        if isinstance(operation, nexusrpc.Operation):
            result_type = result_type or operation.output_type
            operation = operation.name
        request = await self._encode(arg)
        response = await self._deadline(
            operation,
            self._backend.call_async(operation, request, _RESPONSES[operation]),
            rpc_timeout,
        )
        return _Answered(await self._decode(response, result_type))

    async def open_writer(
        self, request: Any, *, rpc_timeout: timedelta | None = None
    ) -> StreamWriterObject:
        """Open a writer of the runtime from a writer model."""
        encoded = await self._encode(request)
        return await self._deadline(
            "OpenWriter", self._backend.open_writer(encoded), rpc_timeout
        )

    async def write_records(
        self,
        writer: StreamWriterObject,
        request: Any,
        result_type: type[_OutputT],
        *,
        rpc_timeout: timedelta | None = None,
    ) -> _OutputT:
        """Write a writer model's Records, and return the answer as ``result_type``."""
        encoded = await self._encode(request)
        data = await self._deadline(
            "WriteRecords",
            _answer(writer.write(encoded.SerializeToString())),
            rpc_timeout,
        )
        return await self._decode(
            runtime_pb.WriteRecordsResponse.FromString(data), result_type
        )

    async def close_writer(
        self,
        writer: StreamWriterObject,
        result_type: type[_OutputT],
        *,
        rpc_timeout: timedelta | None = None,
    ) -> _OutputT:
        """Close a writer, and return the answer as ``result_type``."""
        data = await self._deadline("CloseWriter", _answer(writer.close()), rpc_timeout)
        return await self._decode(
            runtime_pb.CloseWriterResponse.FromString(data), result_type
        )

    async def _encode(self, arg: Any) -> google.protobuf.message.Message:
        with self._converters():
            request = cast(
                google.protobuf.message.Message,
                _transfer_type_converter(type(arg)).to_transfer_type(arg),
            )
        # The IDL leaves the namespace out, so the runtime's client sets it on each request.
        if "namespace" in request.DESCRIPTOR.fields_by_name:
            setattr(request, "namespace", self._namespace)
        await _encode_payloads(request, self._data_converter.payload_codec)
        return request

    async def _decode(
        self, response: google.protobuf.message.Message, result_type: type[Any] | None
    ) -> Any:
        await _decode_payloads(response, self._data_converter.payload_codec)
        if result_type is None:
            return None
        with self._converters():
            return _transfer_type_converter(result_type).from_transfer_type(
                response, result_type
            )

    async def _deadline(
        self, operation: str, call: Awaitable[_T], rpc_timeout: timedelta | None
    ) -> _T:
        # The rpc_timeout is the call's deadline here too. The Streams give theirs through
        # call_options.
        options = call_options.get()
        timeout = rpc_timeout or (options.timeout if options is not None else None)
        if timeout is None:
            return await call
        try:
            return await asyncio.wait_for(call, timeout.total_seconds())
        except asyncio.TimeoutError:
            raise temporalio.service.RPCError(
                f"The Stream call {operation} passed its rpc_timeout of {timeout}",
                temporalio.service.RPCStatusCode.DEADLINE_EXCEEDED,
                b"",
            ) from None


@dataclass(frozen=True)
class _Answered:
    # The runtime answers before the start returns, so the operation has its result.
    value: Any

    async def result(self) -> Any:
        return self.value


def _transfer_type_converter(
    model: type[Any],
) -> temporalio.converter.TransferTypeConverter[Any, Any]:
    converter = _get_transfer_type_converter(model)
    if converter is None:
        raise TypeError(f"{model.__name__} has no proto transfer type")
    return converter


# The codec runs on the user payloads inside a request or a response, failure details
# included, and never on the message itself, which the runtime reads.
def _payloads_in(
    message: google.protobuf.message.Message,
) -> list[temporalio.api.common.v1.Payload]:
    if isinstance(message, temporalio.api.common.v1.Payload):
        return [message]
    found: list[temporalio.api.common.v1.Payload] = []
    for _, value in message.ListFields():
        if isinstance(value, google.protobuf.message.Message):
            found.extend(_payloads_in(value))
        elif not isinstance(value, (str, bytes, int, float, bool)):
            items = cast(
                "Iterable[object]",
                value.values() if hasattr(value, "values") else value,
            )
            for item in items:
                if isinstance(item, google.protobuf.message.Message):
                    found.extend(_payloads_in(item))
    return found


async def _encode_payloads(
    message: google.protobuf.message.Message,
    codec: temporalio.converter.PayloadCodec | None,
) -> None:
    if codec is None:
        return
    # One codec call for each message, since a remote codec costs a round trip for each call.
    payloads = _payloads_in(message)
    if payloads:
        encoded = await codec.encode(payloads)
        for payload, new in zip(payloads, encoded, strict=True):
            payload.CopyFrom(new)


async def _decode_payloads(
    message: google.protobuf.message.Message,
    codec: temporalio.converter.PayloadCodec | None,
) -> None:
    if codec is None:
        return
    payloads = _payloads_in(message)
    if payloads:
        decoded = await codec.decode(payloads)
        for payload, new in zip(payloads, decoded, strict=True):
            payload.CopyFrom(new)
