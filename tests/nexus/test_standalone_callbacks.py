"""NexusHandler completion callbacks on a standalone Nexus operation.

The server takes callbacks of the NexusHandler kind on a standalone start only
with ``nexusoperation.enabledCallbackKinds`` set, so the live tests skip on a
server that refuses them. The streams live lane turns the setting on and fails
on that skip.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from nexusrpc.handler import StartOperationContext, service_handler, sync_operation

import temporalio.api.workflowservice.v1
import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    NexusClient,
    NexusHandlerCallback,
    NexusOperationHandle,
)
from temporalio.nexus.system.notification_service import (
    NotificationService,
    OnCompleteRequest,
    OnCompleteRequestResultSuccess,
    OnCompleteResponse,
    OnProgressRequest,
    OnProgressResponse,
)
from temporalio.service import RPCError, RPCStatusCode, ServiceClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from tests.helpers.nexus import end_standalone_operation, make_nexus_endpoint_name

# The message a live test skips with when the server refuses the callback or
# never delivers it. The streams live lane fails on it.
NO_HANDLER_CALLBACKS = "no NexusHandler callbacks on a standalone operation"


class _TestCodec(temporalio.converter.PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(metadata={"test-codec": b"true"}, data=p.SerializeToString())
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(p.data) for p in payloads]


def _captured_client() -> tuple[Client, dict[str, Any]]:
    captured: dict[str, Any] = {}

    async def start(req: Any, **_kwargs: Any) -> Any:
        captured["start"] = req
        return temporalio.api.workflowservice.v1.StartNexusOperationExecutionResponse(
            run_id="run"
        )

    service_client = Mock(spec=ServiceClient)
    service_client.workflow_service = Mock()
    service_client.workflow_service.start_nexus_operation_execution = AsyncMock(
        side_effect=start
    )
    service_client.config = Mock(identity="test-identity")
    client = Client(
        service_client=service_client,
        namespace="test-namespace",
        data_converter=temporalio.converter.DataConverter(payload_codec=_TestCodec()),
    )
    return client, captured


async def test_a_start_carries_each_callback_with_its_encoded_context() -> None:
    client, captured = _captured_client()
    nexus_client: NexusClient[Any] = client.create_nexus_client(
        service="Orders", endpoint="orders"
    )

    await nexus_client.start_operation(
        "watch",
        "order-1",
        id="op-1",
        completion_callbacks=[
            NexusHandlerCallback(
                task_queue="app",
                progress_operation="OnProgress",
                source_context={"follower": "orders"},
            ),
            NexusHandlerCallback(task_queue="audit", service="Audit", operation="Done"),
        ],
    )

    request = captured["start"]
    first, second = request.completion_callbacks
    handler = first.nexus_handler
    assert handler.task_queue_name == "app"
    assert handler.service == "temporal.api.notificationservice.v1.NotificationService"
    assert handler.operation == "OnComplete"
    assert handler.progress_operation == "OnProgress"
    # The client's codec encodes the context, as it does the input.
    assert handler.source_context.metadata["test-codec"] == b"true"
    [context] = await client.data_converter.decode([handler.source_context])
    assert context == {"follower": "orders"}
    other = second.nexus_handler
    assert (other.task_queue_name, other.service, other.operation) == (
        "audit",
        "Audit",
        "Done",
    )
    assert other.progress_operation == ""
    assert not other.HasField("source_context")


async def test_a_start_without_callbacks_sends_none() -> None:
    client, captured = _captured_client()
    nexus_client: NexusClient[Any] = client.create_nexus_client(
        service="Orders", endpoint="orders"
    )

    await nexus_client.start_operation("watch", "order-1", id="op-1")

    assert not captured["start"].completion_callbacks


@service_handler(name="StandaloneCallbackEcho")
class _Echo:
    @sync_operation
    async def echo(self, _ctx: StartOperationContext, input: str) -> str:
        return input


@service_handler(service=NotificationService)
class _Receiver:
    def __init__(self) -> None:
        self.endpoints: list[str] = []
        self.completions: asyncio.Queue[OnCompleteRequest] = asyncio.Queue()

    @sync_operation
    async def on_progress(
        self, _ctx: StartOperationContext, input: OnProgressRequest
    ) -> OnProgressResponse:
        _ = input
        return OnProgressResponse()

    @sync_operation
    async def on_complete(
        self, _ctx: StartOperationContext, input: OnCompleteRequest
    ) -> OnCompleteResponse:
        import temporalio.nexus

        self.endpoints.append(temporalio.nexus.info().endpoint)
        await self.completions.put(input)
        return OnCompleteResponse()


async def test_live_a_worker_gets_the_completion_of_a_standalone_operation(
    client: Client, env: WorkflowEnvironment
) -> None:
    if env.supports_time_skipping:
        pytest.skip("Standalone Nexus Operation tests don't work with time-skipping")
    task_queue = str(uuid.uuid4())
    endpoint = make_nexus_endpoint_name(task_queue)
    receiver = _Receiver()
    async with Worker(
        client,
        task_queue=task_queue,
        nexus_service_handlers=[_Echo(), receiver],
    ):
        await env.create_nexus_endpoint(endpoint, task_queue)
        nexus_client: NexusClient[Any] = client.create_nexus_client(
            service="StandaloneCallbackEcho", endpoint=endpoint
        )
        try:
            handle: NexusOperationHandle[str] = await nexus_client.start_operation(
                "echo",
                "hello",
                id=str(uuid.uuid4()),
                schedule_to_close_timeout=timedelta(seconds=30),
                completion_callbacks=[
                    NexusHandlerCallback(
                        task_queue=task_queue, source_context="context"
                    )
                ],
            )
        except RPCError as err:
            if err.status == RPCStatusCode.INVALID_ARGUMENT and "allback" in str(err):
                pytest.skip(f"{NO_HANDLER_CALLBACKS}: {err}")
            raise
        try:
            assert await handle.result() == "hello"
            try:
                completion = await asyncio.wait_for(receiver.completions.get(), 30)
            except asyncio.TimeoutError:
                # A server without the callback kind may keep the callback and never
                # call it.
                pytest.skip(f"{NO_HANDLER_CALLBACKS}: no OnComplete within 30s")
        finally:
            await end_standalone_operation(
                client, handle, lambda: bool(receiver.endpoints)
            )

    # The server sends the task with no endpoint, which the Worker serves.
    assert receiver.endpoints == [""]
    assert completion.result == OnCompleteRequestResultSuccess("hello")
    assert completion.source_context == "context"
