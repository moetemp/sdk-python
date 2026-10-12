"""Scenario 1b on Bridge: a parent Workflow relays its child's stream.

The parent ``Order`` starts the child ``Delivery``. It then starts a stream
operation on an endpoint in its own namespace, whose ``open_stream`` names
``Delivery``'s stream (``USE_EXISTING`` finds the running child). The parent
reads the stream with ``StreamReader`` and republishes each stage on its own
stream with ``workflow_writer``. The backend long-polls the parent's stream.
The operation completes with ``Delivery``'s close result.

Left out: System Nexus ``ReadStream`` and records in progress callbacks.
Progress is a notification, and the records come from reads that the
parent's History records.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.n1b_parent_relay``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import workflow
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.contrib.streams import (
    StreamRecord,
    StreamRef,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
    close_workflow_stream,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, values
from tests.contrib.streams.samples.scenarios._nexus import (
    NewStore,
    connect,
    endpoint,
    run,
)

STAGES = topic("stages", str)
RELAY = topic("relay", str)
DELIVERY_STAGES = ["assigned", "picked up", "on the way", "delivered"]


@nexusrpc.service
class DeliveryService:
    follow: nexusrpc.Operation[str, str]


@workflow.defn
class Delivery:
    """Publishes its stages, then closes its stream with a summary."""

    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        for stage in DELIVERY_STAGES:
            stages.publish(stage)
            await workflow.sleep(timedelta(milliseconds=100))
        close_workflow_stream(f"{len(DELIVERY_STAGES)} stages", topic=STAGES)


async def open_delivery(_ctx: Any, delivery_id: str) -> StreamRef:
    """Names ``Delivery``'s stream, and starts ``Delivery`` if nobody did."""
    await temporalio.nexus.client().start_workflow(
        Delivery.run,
        id=delivery_id,
        task_queue=temporalio.nexus.info().task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return StreamRef.for_workflow(delivery_id, topic=STAGES.name)


@nexusrpc.handler.service_handler(service=DeliveryService)
class DeliveryHandler:
    @nexusrpc.handler.operation_handler
    def follow(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(open_delivery)


async def relay(endpoint: str, delivery_id: str) -> str:
    """Read ``delivery_id``'s stream through ``endpoint`` and republish it.

    Workflow code. Returns the operation's close result.
    """
    client = workflow.create_nexus_client(service=DeliveryService, endpoint=endpoint)
    handle = await client.start_operation(DeliveryService.follow, delivery_id)
    reader = StreamReader(handle, item_type=str, endpoint=endpoint)
    out = workflow_writer(RELAY)
    while (batch := await reader.next()) is not None:
        for stage in batch:
            out.publish(f"delivery {stage}")
    return await handle


@workflow.defn
class Order:
    """Starts its delivery as a child and relays the child's stages."""

    @workflow.run
    async def run(self, endpoint: str, delivery_id: str) -> str:
        child = await workflow.start_child_workflow(Delivery.run, id=delivery_id)
        result = await relay(endpoint, delivery_id)
        await child
        return result


@dataclass
class Outcome:
    relayed: list[str]
    close_result: str


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "order-n1b",
    new_store: NewStore,
) -> Outcome:
    """Run one order and return what the backend read from the parent.

    ``client`` is a client of a server with the stream notifier.
    """
    streams = await connect(client, new_store().notify_on_append())
    name = await endpoint(client, task_queue)
    async with Worker(
        streams,
        task_queue=task_queue,
        workflows=[Order, Delivery],
        nexus_service_handlers=[DeliveryHandler(), TemporalStreamsHandler()],
    ):
        handle = await streams.start_workflow(
            Order.run,
            args=[name, f"{workflow_id}-delivery"],
            id=workflow_id,
            task_queue=task_queue,
        )
        seen: list[StreamRecord[str]] = []
        await follow(
            "backend", get_stream_handle(streams, workflow_id).read(topic=RELAY), seen
        )
        result = await asyncio.wait_for(handle.result(), 30)
        return Outcome(relayed=values(seen), close_result=result)


if __name__ == "__main__":
    run(main)
