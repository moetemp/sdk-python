"""Calls to the temporalio-streams runtime in the bridge, for Activities and clients."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import functools
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast

import google.protobuf.message
from typing_extensions import override

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.bridge.streams
import temporalio.converter
import temporalio.nexus.system
import temporalio.service
from temporalio.common._streams import _errors
from temporalio.common._streams.proto import v1 as ext_pb
from temporalio.common._streams.system import client as service_client
from temporalio.converter._payload_converter import _get_transfer_type_converter

if TYPE_CHECKING:
    # The writer bindings import this package's protos, so _writers_class() imports them on first
    # use.
    from temporalio.common._streams.writer_system import client as writer_client

_M = TypeVar("_M", bound=google.protobuf.message.Message)

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
    """The calls that a Stream backend gives the handles of one client."""

    def service(
        self,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> service_client.StreamServiceClientExtension:
        """The Stream service for ``namespace``."""
        ...

    def writers(
        self, namespace: str, data_converter: temporalio.converter.DataConverter
    ) -> writer_client.StreamWriterClientExtension:
        """The writers of Activity and client Producers for ``namespace``."""
        ...

    def free_writer(self, writer_id: int) -> None:
        """Release one handle of a writer without waiting, such as when Python frees it."""
        ...


class StreamRuntime(Protocol):
    """The calls of the crate's runtime object, which the bridge builds."""

    async def call(self, method: str, input: bytes) -> bytes:
        """Call a method with encoded input, and return the encoded output."""
        ...

    def free_writer(self, writer_id: int) -> None:
        """Release one handle of a writer without waiting."""
        ...


class _ExtensionCalls:
    """Makes Stream calls to the crate's runtime object, which serves one client.

    The generated clients start the Stream service and writer operations. Their Nexus client is
    the runtime.
    """

    def __init__(self, runtime: StreamRuntime) -> None:
        """Create a backend over a runtime object."""
        self._runtime = runtime

    def service(
        self,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> service_client.StreamServiceClientExtension:
        """The generated Stream service client for ``namespace``.

        With a ``data_converter``, the client encodes user values in the requests with it and
        its codec.
        """
        return _StreamService(_RuntimeNexusClient(self, namespace, data_converter))

    def writers(
        self, namespace: str, data_converter: temporalio.converter.DataConverter
    ) -> writer_client.StreamWriterClientExtension:
        """The generated writer client over the runtime."""
        return _writers_class()(_RuntimeNexusClient(self, namespace, data_converter))

    def free_writer(self, writer_id: int) -> None:
        """Release one handle of a writer without waiting."""
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
        """Close every open writer of the runtime, and return how many failed."""
        response = await self.call_async(
            "CloseWriters", ext_pb.CloseWritersRequest(), ext_pb.CloseWritersResponse
        )
        return response.failed


class _StreamService(service_client.StreamServiceClientExtension):
    def __init__(self, nexus_client: _RuntimeNexusClient) -> None:
        self._nexus_client = nexus_client

    @override
    def _create_nexus_client(
        self, *, service: str, endpoint: str | None
    ) -> _RuntimeNexusClient:
        del service, endpoint
        return self._nexus_client


@functools.cache
def _writers_class() -> Callable[
    [_RuntimeNexusClient], writer_client.StreamWriterClientExtension
]:
    from temporalio.common._streams.writer_system import client

    class _StreamWriters(client.StreamWriterClientExtension):
        def __init__(self, nexus_client: _RuntimeNexusClient) -> None:
            self._nexus_client = nexus_client

        @override
        def _create_nexus_client(
            self, *, service: str, endpoint: str | None
        ) -> _RuntimeNexusClient:
            del service, endpoint
            return self._nexus_client

    return _StreamWriters


class _RuntimeNexusClient:
    # The generated clients start each operation on a Nexus client. This one starts it on the
    # runtime in place of an endpoint, and the runtime answers before the start returns.
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
        operation: str,
        input: Any,
        *,
        output_type: type[Any] | None = None,
    ) -> Awaitable[Any]:
        with self._converters():
            request = cast(
                google.protobuf.message.Message,
                _transfer_type_converter(type(input)).to_transfer_type(input),
            )
        # The IDL leaves the namespace out, since the server knows it.
        if "namespace" in request.DESCRIPTOR.fields_by_name:
            setattr(request, "namespace", self._namespace)
        codec = self._data_converter.payload_codec
        await _encode_payloads(request, codec)
        response = await self._call(operation, request)
        await _decode_payloads(response, codec)
        result: Any = None
        if output_type is not None:
            with self._converters():
                result = _transfer_type_converter(output_type).from_transfer_type(
                    response, output_type
                )
        done: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        done.set_result(result)
        return done

    async def _call(
        self, operation: str, request: google.protobuf.message.Message
    ) -> google.protobuf.message.Message:
        # The runtime makes no RPC, so it has no use for rpc_metadata. The rpc_timeout is the
        # call's deadline here too.
        options = call_options.get()
        timeout = options.timeout if options is not None else None
        call = self._backend.call_async(operation, request, _RESPONSES[operation])
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
