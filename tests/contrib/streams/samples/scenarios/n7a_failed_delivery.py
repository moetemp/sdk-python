"""Scenario 7a on Bridge: a caller drains the stream, then learns how the delivery failed.

``Delivery`` publishes two stages, then ends in one of three ways:

- ``fail``: it raises ``ApplicationError(type="CourierLost")``. The chain end
  closes the stream with the run's failure.
- ``close-with-error``: it calls ``close_workflow_stream(error=...)`` and keeps
  running. The close fails the operation at once.
- ``terminate``: it is terminated. No Worker runs a final task, so the
  server's owner check closes the stream with a terminated failure, within
  ``streamnotifier.ownerCheckInterval``.

A ``Dispatcher`` Workflow reads every stage with ``StreamReader``, and then
``next()`` raises ``NexusOperationError`` with the failure as its cause.

Left out: nothing. The close failure is Bridge's fix for 7a (DD-107).

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.n7a_failed_delivery``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import nexusrpc
import nexusrpc.handler

from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams import StreamRef, topic, workflow_writer
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
    close_workflow_stream,
)
from temporalio.exceptions import ApplicationError, NexusOperationError
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NewStore,
    connect,
    endpoint,
    run,
    until,
)

STAGES = topic("stages", str)
HOW = ("fail", "close-with-error", "terminate")


@nexusrpc.service
class DeliveryService:
    follow: nexusrpc.Operation[str, str]


@workflow.defn
class Delivery:
    def __init__(self) -> None:
        self._how: str | None = None

    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        stages.publish("assigned")
        stages.publish("picked up")
        await workflow.wait_condition(lambda: self._how is not None)
        lost = ApplicationError(
            "the courier stopped answering", type="CourierLost", non_retryable=True
        )
        if self._how == "fail":
            raise lost
        close_workflow_stream(error=lost, topic=STAGES)
        await workflow.wait_condition(lambda: self._how == "finish")

    @workflow.signal
    def end(self, how: str) -> None:
        self._how = how


async def attach_only(_ctx: Any, delivery_id: str) -> StreamRef:
    return StreamRef.for_workflow(delivery_id, topic=STAGES.name)


@nexusrpc.handler.service_handler(service=DeliveryService)
class DeliveryHandler:
    @nexusrpc.handler.operation_handler
    def follow(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(attach_only)


@dataclass
class Dispatched:
    stages: list[str] = field(default_factory=list)
    #: The cause's class, and its type for an application failure.
    failure: list[str] = field(default_factory=list)


@workflow.defn
class Dispatcher:
    @workflow.run
    async def run(self, endpoint: str, delivery_id: str) -> Dispatched:
        client = workflow.create_nexus_client(
            service=DeliveryService, endpoint=endpoint
        )
        handle = await client.start_operation(DeliveryService.follow, delivery_id)
        reader = StreamReader(handle, item_type=str, endpoint=endpoint)
        dispatched = Dispatched()
        try:
            while (batch := await reader.next()) is not None:
                dispatched.stages.extend(batch)
        except NexusOperationError as err:
            cause = err.cause
            dispatched.failure = [
                type(cause).__name__,
                getattr(cause, "type", None) or "",
            ]
        return dispatched


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "delivery-n7a",
    new_store: NewStore,
    how: str = "fail",
) -> Dispatched:
    """End the delivery as ``how`` says and return what the dispatcher learned.

    ``client`` is a client of a server with the stream notifier. ``how`` is
    one of ``fail``, ``close-with-error`` and ``terminate``.
    """
    streams = await connect(client, new_store().notify_on_append())
    name = await endpoint(client, task_queue)
    async with Worker(
        streams,
        task_queue=task_queue,
        workflows=[Delivery, Dispatcher],
        nexus_service_handlers=[DeliveryHandler(), TemporalStreamsHandler()],
    ):
        delivery = await streams.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )
        dispatcher = await streams.start_workflow(
            Dispatcher.run,
            args=[name, workflow_id],
            id=f"{workflow_id}-dispatcher",
            task_queue=task_queue,
        )

        async def attached() -> bool:
            history = await dispatcher.fetch_history()
            return any(
                event.HasField("nexus_operation_started_event_attributes")
                for event in history.events
            )

        await until(attached)
        if how == "terminate":
            await delivery.terminate("the courier's phone went dark")
        else:
            await delivery.signal(Delivery.end, how)
        dispatched = await asyncio.wait_for(dispatcher.result(), 30)
        if (await delivery.describe()).status == WorkflowExecutionStatus.RUNNING:
            await delivery.signal(Delivery.end, "finish")
        print(f"[dispatcher] {dispatched.stages}, then {dispatched.failure}")
        return dispatched


if __name__ == "__main__":
    run(main)
