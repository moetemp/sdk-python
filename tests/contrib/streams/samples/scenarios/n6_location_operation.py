"""Scenario 6 on Bridge: live location through a second stream operation.

``Delivery`` runs in namespace ``streams-b``. It publishes ``tracking
started`` on ``events``, and the courier's phone there appends positions on
``location``. A ``Tracker`` Workflow in ``default`` follows ``events``
through one stream operation. When it sees ``tracking started``, it starts a
second operation that follows ``location``. Each read is a Nexus operation
in the tracker's History, which the test counts.

Left out: an app backend outside any Workflow that polls ``location`` in B.
It needs Nexus direct calls, which don't exist yet. Progress folds, so the
tracker reads what is there, not one record per wake.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.n6_location_operation``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.contrib.streams import (
    RecordKind,
    StreamNotFoundError,
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
from tests.contrib.streams.samples.scenarios._nexus import (
    NAMESPACE_B,
    NewStore,
    connect,
    endpoint,
    run,
    two_namespaces,
)
from tests.contrib.streams.samples.scenarios.s6_location_topic import (
    POSITIONS,
    Position,
)

EVENTS = topic("events", str)
LOCATION = topic("location", Position)


@dataclass
class Follow:
    delivery_id: str
    topic: str


@nexusrpc.service
class DeliveryService:
    follow: nexusrpc.Operation[Follow, str]


@workflow.defn
class Delivery:
    def __init__(self) -> None:
        self._arrived = False

    @workflow.run
    async def run(self) -> None:
        events = workflow_writer(EVENTS)
        events.publish("picked up")
        events.publish("tracking started")
        await workflow.wait_condition(lambda: self._arrived)
        events.publish("arrived")
        close_workflow_stream("tracking ended", topic=LOCATION)
        close_workflow_stream("delivered", topic=EVENTS)

    @workflow.signal
    def arrived(self) -> None:
        self._arrived = True


async def open_topic(_ctx: Any, follow: Follow) -> StreamRef:
    # Only the first operation starts the delivery. A later one can come after
    # the delivery closed, and a start then would begin a new run chain.
    if follow.topic == EVENTS.name:
        await temporalio.nexus.client().start_workflow(
            Delivery.run,
            id=follow.delivery_id,
            task_queue=temporalio.nexus.info().task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
    return StreamRef.for_workflow(follow.delivery_id, topic=follow.topic)


@nexusrpc.handler.service_handler(service=DeliveryService)
class DeliveryHandler:
    @nexusrpc.handler.operation_handler
    def follow(self) -> nexusrpc.handler.OperationHandler[Follow, str]:
        return StreamOperationHandler(open_topic)


@dataclass
class Tracked:
    events: list[str] = field(default_factory=list)
    positions: list[Position] = field(default_factory=list)


@workflow.defn
class Tracker:
    """Follows a delivery's events, and its location once tracking starts."""

    @workflow.run
    async def run(self, endpoint: str, delivery_id: str) -> Tracked:
        client = workflow.create_nexus_client(
            service=DeliveryService, endpoint=endpoint
        )
        tracked = Tracked()

        async def follow_location() -> None:
            handle = await client.start_operation(
                DeliveryService.follow, Follow(delivery_id, LOCATION.name)
            )
            reader = StreamReader(handle, item_type=Position, endpoint=endpoint)
            while (batch := await reader.next()) is not None:
                tracked.positions.extend(batch)

        events = await client.start_operation(
            DeliveryService.follow, Follow(delivery_id, EVENTS.name)
        )
        reader = StreamReader(events, item_type=str, endpoint=endpoint)
        location: asyncio.Task[None] | None = None
        while (batch := await reader.next()) is not None:
            for event in batch:
                tracked.events.append(event)
                if event == "tracking started" and location is None:
                    location = asyncio.create_task(follow_location())
        if location is not None:
            await location
        return tracked


@dataclass
class Outcome:
    tracked: Tracked
    #: How many stream service reads the tracker's History holds.
    reads: int


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "tracker-n6",
    new_store: NewStore,
) -> Outcome:
    """Run one delivery in B and return what the tracker in ``default`` saw.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier and the namespace ``streams-b``.
    """
    b = await two_namespaces(client, new_store().notify_on_append())
    a = await connect(client, None)
    b_queue = f"{task_queue}-b"
    name = await endpoint(client, b_queue, namespace=NAMESPACE_B)
    delivery_id = f"{workflow_id}-delivery"
    async with (
        Worker(
            b,
            task_queue=b_queue,
            workflows=[Delivery],
            nexus_service_handlers=[DeliveryHandler(), TemporalStreamsHandler()],
        ),
        Worker(a, task_queue=task_queue, workflows=[Tracker]),
    ):
        handle = await a.start_workflow(
            Tracker.run, args=[name, delivery_id], id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(b, delivery_id)

        async def wait_for(event_value: str) -> None:
            while True:
                try:
                    records = stream.read(topic=EVENTS)
                    async for record in records:
                        if (
                            record.kind is RecordKind.DATA
                            and record.value == event_value
                        ):
                            await records.aclose()
                            return
                except StreamNotFoundError:
                    # The tracker's first operation has not started the delivery yet.
                    await asyncio.sleep(0.1)

        await asyncio.wait_for(wait_for("tracking started"), 30)
        phone = stream.producer(topic=LOCATION, producer_id="courier-phone", attempt=1)
        for position in POSITIONS:
            await phone.append(position)
        await b.get_workflow_handle(delivery_id).signal(Delivery.arrived)
        tracked = await asyncio.wait_for(handle.result(), 30)
        reads = sum(
            1
            for event in (await handle.fetch_history()).events
            if event.event_type == EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
            and event.nexus_operation_scheduled_event_attributes.operation == "read"
        )
        print(f"[tracker] {len(tracked.positions)} positions in {reads} reads")
        return Outcome(tracked=tracked, reads=reads)


if __name__ == "__main__":
    run(main)
