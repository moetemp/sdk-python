"""A Worker that implements the generated notification service.

The server delivers a NexusHandler callback's progress and completion as Nexus
tasks with no endpoint. Each input is a system payload, binary protobuf that
the Worker's codec must not touch, which holds payloads the client encoded
with that codec. The server reads each answer as a protobuf, so the answer
must not go through the codec either.
"""

from __future__ import annotations

from collections.abc import MutableSequence, Sequence
from unittest import mock

import nexusrpc
from nexusrpc.handler import StartOperationContext, service_handler, sync_operation

import temporalio.api.nexus.v1
import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.api.notificationservice.v1 import (
    OnCompleteRequest,
    OnCompleteResponse,
    OnProgressRequest,
    OnProgressResponse,
)
from temporalio.bridge._visitor import PayloadVisitor
from temporalio.bridge._visitor_functions import VisitorFunctions
from temporalio.bridge.proto.nexus import NexusTaskCompletion
from temporalio.nexus.system.notification_service import NotificationService
from temporalio.worker._nexus import _NexusTaskCancellation, _NexusWorker

SERVICE = "temporal.api.notificationservice.v1.NotificationService"


class _TestCodec(temporalio.converter.PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(metadata={"test-codec": b"true"}, data=p.SerializeToString())
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        decoded = []
        for payload in payloads:
            if payload.metadata.get("test-codec") != b"true":
                raise RuntimeError("not encoded with the test codec")
            decoded.append(Payload.FromString(payload.data))
        return decoded


_CONVERTER = temporalio.converter.DataConverter(payload_codec=_TestCodec())


@service_handler(service=NotificationService)
class _Receiver:
    def __init__(self) -> None:
        self.progress: list[OnProgressRequest] = []
        self.completions: list[OnCompleteRequest] = []

    @sync_operation
    async def on_progress(
        self, _ctx: StartOperationContext, input: OnProgressRequest
    ) -> OnProgressResponse:
        self.progress.append(input)
        return OnProgressResponse(receiver_state=b"next", behind=True)

    @sync_operation
    async def on_complete(
        self, _ctx: StartOperationContext, input: OnCompleteRequest
    ) -> OnCompleteResponse:
        self.completions.append(input)
        return OnCompleteResponse()


def _worker(receiver: _Receiver) -> _NexusWorker:
    return _NexusWorker(
        bridge_worker=lambda: mock.MagicMock(),
        client=mock.MagicMock(namespace="default"),
        namespace="default",
        task_queue="tq",
        service_handlers=[receiver],
        data_converter=_CONVERTER,
        interceptors=[],
        metric_meter=mock.MagicMock(),
        executor=None,
    )


async def _client_payload(value: str) -> Payload:
    # What a client puts in the callback's source context or a result.
    [payload] = await _CONVERTER.encode([value])
    return payload


def _server_payload(request: OnProgressRequest | OnCompleteRequest) -> Payload:
    # The server's EncodeSystemPayload.
    return Payload(
        metadata={
            "encoding": b"binary/protobuf",
            "messageType": request.DESCRIPTOR.full_name.encode(),
            "__temporal_system_payload": b"true",
        },
        data=request.SerializeToString(),
    )


async def _deliver(
    worker: _NexusWorker, operation: str, payload: Payload
) -> temporalio.api.nexus.v1.StartOperationResponse:
    return await worker._start_operation(
        temporalio.api.nexus.v1.StartOperationRequest(
            service=SERVICE, operation=operation, payload=payload
        ),
        headers={},
        cancellation=_NexusTaskCancellation(),
        request_deadline=None,
        endpoint="",
        data_converter=worker._data_converter_for_nexus_task("", SERVICE, operation),
    )


class _Codec(VisitorFunctions):
    """Runs the codec over a task completion, as the Worker does before it sends it."""

    async def visit_payload(self, payload: Payload) -> None:
        [encoded] = await _CONVERTER.encode([payload])
        payload.CopyFrom(encoded)

    async def visit_payloads(self, payloads: MutableSequence[Payload]) -> None:
        for payload in payloads:
            await self.visit_payload(payload)


async def _as_sent(response: temporalio.api.nexus.v1.StartOperationResponse) -> Payload:
    completion = NexusTaskCompletion(completed=temporalio.api.nexus.v1.Response())
    completion.completed.start_operation.CopyFrom(response)
    await PayloadVisitor().visit(_Codec(), completion)
    return completion.completed.start_operation.sync_success.payload


def test_the_generated_definition_names_the_wire_operations() -> None:
    definition = nexusrpc.get_service_definition(NotificationService)
    assert definition is not None
    assert definition.name == SERVICE
    progress = definition.operation_definitions["OnProgress"]
    assert progress.input_type is OnProgressRequest
    assert progress.output_type is OnProgressResponse
    complete = definition.operation_definitions["OnComplete"]
    assert complete.input_type is OnCompleteRequest
    assert complete.output_type is OnCompleteResponse


async def test_a_receiver_decodes_progress_as_the_server_encodes_it() -> None:
    receiver = _Receiver()
    request = OnProgressRequest(
        source_context=await _client_payload("orders"),
        operation_token="token",
        receiver_state=b"state",
    )
    request.progress.position = "7"
    request.progress.counter = 3

    response = await _deliver(_worker(receiver), "OnProgress", _server_payload(request))

    assert response.HasField("sync_success"), response
    [got] = receiver.progress
    assert got.operation_token == "token"
    assert got.receiver_state == b"state"
    assert got.progress.position == "7"
    assert got.progress.counter == 3
    # The codec decoded the client's payload inside the system payload.
    assert "test-codec" not in got.source_context.metadata
    assert _CONVERTER.payload_converter.from_payload(got.source_context) == "orders"

    sent = await _as_sent(response)
    # The server reads the answer as a protobuf, so the codec leaves it alone.
    assert sent.metadata["encoding"] == b"binary/protobuf"
    assert "test-codec" not in sent.metadata
    assert OnProgressResponse.FromString(sent.data) == OnProgressResponse(
        receiver_state=b"next", behind=True
    )


async def test_a_receiver_decodes_a_completion_as_the_server_encodes_it() -> None:
    receiver = _Receiver()
    request = OnCompleteRequest(
        success=await _client_payload("done"),
        source_context=await _client_payload("orders"),
        receiver_state=b"state",
    )

    response = await _deliver(_worker(receiver), "OnComplete", _server_payload(request))

    assert response.HasField("sync_success"), response
    [got] = receiver.completions
    assert _CONVERTER.payload_converter.from_payload(got.success) == "done"
    assert _CONVERTER.payload_converter.from_payload(got.source_context) == "orders"
    assert got.receiver_state == b"state"
    sent = await _as_sent(response)
    assert sent.metadata["encoding"] == b"binary/protobuf"
    assert "test-codec" not in sent.metadata
