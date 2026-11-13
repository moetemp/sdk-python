from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import google.protobuf.message
import nexusrpc

import temporalio.activity
import temporalio.bridge._visitor
import temporalio.common
import temporalio.converter
import temporalio.converter._payload_converter
import temporalio.nexus.system.stream_service


@dataclasses.dataclass(frozen=True)
class StreamBackendCallOptions:
    """Options of the Local Activity that calls a Stream backend for Workflow code.

    Workflow code can't do I/O, so a worker calls its Stream backend from a Local
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
    """Retries of a failed call. The default is one attempt, since an append that runs
    again writes its Records again."""

    def __post_init__(self) -> None:
        """Check that a timeout is set."""
        if not self.start_to_close_timeout and not self.schedule_to_close_timeout:
            raise ValueError(
                "start_to_close_timeout or schedule_to_close_timeout must be set"
            )


class StreamBackend(ABC):
    """A store that answers the Stream operations of a client.

    A client with a backend sends each Stream operation to it instead of the server.

    .. warning::
        This API is experimental and subject to change.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the backend."""
        ...

    @property
    def call_options(self) -> StreamBackendCallOptions:
        """Options of the Local Activity that calls this backend for Workflow code."""
        return StreamBackendCallOptions()

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


_STREAM_BACKEND_ACTIVITY = "__temporal_stream_backend_call"


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


async def _call_stream_backend(
    backend: StreamBackend,
    data_converter: temporalio.converter.DataConverter,
    operation: str,
    request: bytes,
) -> bytes:
    if not data_converter.payload_codec:
        return await backend.call(operation, request)
    # The worker package imports the client, so a module level import would cycle.
    from temporalio.bridge.worker import _Visitor

    definition = _STREAM_SERVICE.operation_definitions[operation]
    visitor = temporalio.bridge._visitor.PayloadVisitor(skip_search_attributes=True)
    request_message = _proto_type(definition.input_type).FromString(request)
    await visitor.visit(
        _Visitor(data_converter._encode_payload_sequence), request_message
    )
    response = await backend.call(operation, request_message.SerializeToString())
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


def _stream_backend_activity(  # pyright: ignore[reportUnusedFunction]
    backend: StreamBackend, data_converter: temporalio.converter.DataConverter
) -> Callable[[str, bytes], Awaitable[bytes]]:
    @temporalio.activity.defn(name=_STREAM_BACKEND_ACTIVITY)
    async def call(operation: str, request: bytes) -> bytes:
        return await _call_stream_backend(backend, data_converter, operation, request)

    return call
