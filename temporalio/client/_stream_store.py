from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import google.protobuf.message
import nexusrpc

import temporalio.bridge._visitor
import temporalio.bridge.runtime
import temporalio.common
import temporalio.converter
import temporalio.converter._payload_converter
import temporalio.nexus.system.stream_service
import temporalio.runtime

if TYPE_CHECKING:
    import temporalio.bridge.streams


@dataclasses.dataclass(frozen=True)
class StreamStoreCallOptions:
    """Options of the Local Activity that calls a Stream Store for Workflow code.

    Workflow code can't do I/O, so a worker calls its Stream Store from a Local
    Activity.

    .. warning::
        This API is experimental and subject to change.
    """

    start_to_close_timeout: timedelta | None = timedelta(seconds=60)
    """Most time for one attempt."""

    schedule_to_close_timeout: timedelta | None = None
    """Most time for all attempts."""

    retry_policy: temporalio.common.RetryPolicy = dataclasses.field(
        default_factory=lambda: temporalio.common.RetryPolicy(maximum_attempts=1)
    )
    """Retries of a failed call. The default is one attempt."""

    def __post_init__(self) -> None:
        """Check that a timeout is set."""
        if not self.start_to_close_timeout and not self.schedule_to_close_timeout:
            raise ValueError(
                "start_to_close_timeout or schedule_to_close_timeout must be set"
            )


class StreamStore(ABC):
    """A Store that answers the Stream operations of a client.

    A client with a Store sends each Stream operation to it instead of the server.

    .. warning::
        This API is experimental and subject to change.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the Store."""
        ...

    @property
    def call_options(self) -> StreamStoreCallOptions:
        """Options of the Local Activity that calls this Store for Workflow code."""
        return StreamStoreCallOptions()

    @abstractmethod
    async def call(self, operation: str, request: bytes) -> bytes:
        """Answer one Stream operation.

        Args:
            operation: The Nexus operation name, such as ``CreateStream``,
                ``AppendRecords``, ``ReadRecords``, ``CloseStream`` or
                ``DescribeStream``.
            request: The serialized request message of
                ``temporal.api.streamservice.v1``.

        Returns:
            The serialized response message of ``temporal.api.streamservice.v1``.
        """
        ...


class _BridgeStreamStore(StreamStore):  # pyright: ignore[reportUnusedClass]
    """A Store that runs on the Stream runtime of the bridge.

    It builds the runtime at its first call, so the Store can be built before the
    Runtime and its Tokio threads exist.
    """

    def __init__(
        self,
        *,
        name: str,
        runtime: temporalio.runtime.Runtime | None,
        call_options: StreamStoreCallOptions | None = None,
    ) -> None:
        self._name = name
        self._runtime = runtime
        self._call_options = call_options
        self._stream_runtime: temporalio.bridge.streams.StreamRuntime | None = None

    @property
    def name(self) -> str:
        """Name of the Store."""
        return self._name

    @property
    def call_options(self) -> StreamStoreCallOptions:
        """Options of the Local Activity that calls this Store for Workflow code."""
        return self._call_options or super().call_options

    @abstractmethod
    def _new_stream_runtime(
        self, runtime: temporalio.bridge.runtime.Runtime
    ) -> temporalio.bridge.streams.StreamRuntime: ...

    async def call(self, operation: str, request: bytes) -> bytes:
        """Answer one Stream operation with the Stream runtime of the bridge.

        Raises:
            temporalio.exceptions.StreamError: The call failed.
            RuntimeError: This build of temporalio has no Stream runtime, or not this Store.
        """
        if self._stream_runtime is None:
            runtime = self._runtime or temporalio.runtime.Runtime.default()
            self._stream_runtime = self._new_stream_runtime(runtime._core_runtime)
        return await self._stream_runtime.call(operation, request)


def _check_not_negative(**durations: timedelta | None) -> None:  # pyright: ignore[reportUnusedFunction]
    for label, value in durations.items():
        if value is not None and value < timedelta():
            raise ValueError(f"{label} must not be negative")


def _stream_service() -> nexusrpc.ServiceDefinition:
    service = nexusrpc.get_service_definition(
        temporalio.nexus.system.stream_service.StreamService
    )
    if service is None:
        raise RuntimeError("The generated StreamService has no service definition")
    return service


_STREAM_SERVICE = _stream_service()


def _proto_type(model_type: Any) -> type[google.protobuf.message.Message]:
    converter = temporalio.converter._payload_converter._get_transfer_type_converter(
        model_type
    )
    if converter is None or converter.transfer_type is None:
        raise TypeError(f"{model_type} has no proto transfer type")
    return converter.transfer_type


async def _call_stream_store(  # pyright: ignore[reportUnusedFunction]
    store: StreamStore,
    data_converter: temporalio.converter.DataConverter,
    operation: str,
    request: bytes,
) -> bytes:
    if not data_converter.payload_codec:
        return await store.call(operation, request)
    # The worker package imports the client, so a module level import would cycle.
    from temporalio.bridge.worker import _Visitor

    definition = _STREAM_SERVICE.operation_definitions[operation]
    visitor = temporalio.bridge._visitor.PayloadVisitor(skip_search_attributes=True)
    request_message = _proto_type(definition.input_type).FromString(request)
    await visitor.visit(
        _Visitor(data_converter._encode_payload_sequence), request_message
    )
    response = await store.call(operation, request_message.SerializeToString())
    response_message = _proto_type(definition.output_type).FromString(response)
    await visitor.visit(
        _Visitor(data_converter._decode_payload_sequence), response_message
    )
    return response_message.SerializeToString()


def _stream_response(  # pyright: ignore[reportUnusedFunction]
    payload_converter: temporalio.converter.PayloadConverter,
    operation: str,
    result_type: type | None,
    response: bytes,
) -> Any:
    definition = _STREAM_SERVICE.operation_definitions[operation]
    [payload] = payload_converter.to_payloads(
        [_proto_type(definition.output_type).FromString(response)]
    )
    [result] = payload_converter.from_payloads(
        [payload], [result_type] if result_type else None
    )
    return result
