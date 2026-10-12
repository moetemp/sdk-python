"""Scenario 2 on Bridge: dashcam segments by reference, across namespaces.

``Delivery`` runs in namespace ``streams-b``, and the dashcam there appends
s2's ``Segment`` records to its stream. A ``CameraFeed`` Workflow in
``default`` reads them through the n1c path. Each URL reaches it and
resolves in the object store, which a dict stands in for.

Left out: live video and storage that Temporal manages. Whether a store
claim resolves across Nexus is still open, so the record carries a URL.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.n2_dashcam_relay``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import workflow
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.contrib.streams import StreamRef, get_stream_handle
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
    close_workflow_stream,
)
from temporalio.service import RPCError
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NAMESPACE_B,
    NewStore,
    connect,
    endpoint,
    run,
    two_namespaces,
    until,
)
from tests.contrib.streams.samples.scenarios.s2_dashcam_reference import (
    DASHCAM,
    Segment,
)


@nexusrpc.service
class DashcamService:
    watch: nexusrpc.Operation[str, str]


@workflow.defn
class Delivery:
    """Holds the dashcam stream open until the courier arrives."""

    def __init__(self) -> None:
        self._arrived = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._arrived)
        close_workflow_stream("arrived", topic=DASHCAM)

    @workflow.signal
    def arrived(self) -> None:
        self._arrived = True


async def open_dashcam(_ctx: Any, delivery_id: str) -> StreamRef:
    await temporalio.nexus.client().start_workflow(
        Delivery.run,
        id=delivery_id,
        task_queue=temporalio.nexus.info().task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return StreamRef.for_workflow(delivery_id, topic=DASHCAM.name)


@nexusrpc.handler.service_handler(service=DashcamService)
class DashcamHandler:
    @nexusrpc.handler.operation_handler
    def watch(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(open_dashcam)


@dataclass
class Feed:
    segments: list[Segment] = field(default_factory=list)
    close_result: str = ""


@workflow.defn
class CameraFeed:
    """Collects the segment references of a delivery in another namespace."""

    @workflow.run
    async def run(self, endpoint: str, delivery_id: str) -> Feed:
        client = workflow.create_nexus_client(service=DashcamService, endpoint=endpoint)
        handle = await client.start_operation(DashcamService.watch, delivery_id)
        reader = StreamReader(handle, item_type=Segment, endpoint=endpoint)
        feed = Feed()
        while (batch := await reader.next()) is not None:
            feed.segments.extend(batch)
        feed.close_result = await handle
        return feed


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "feed-n2",
    new_store: NewStore,
) -> list[bytes]:
    """Record three segments in B and return the bytes the feed's URLs resolve to.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier and the namespace ``streams-b``.
    """
    object_store: dict[str, bytes] = {}
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
            nexus_service_handlers=[DashcamHandler(), TemporalStreamsHandler()],
        ),
        Worker(a, task_queue=task_queue, workflows=[CameraFeed]),
    ):
        handle = await a.start_workflow(
            CameraFeed.run,
            args=[name, delivery_id],
            id=workflow_id,
            task_queue=task_queue,
        )
        delivery = b.get_workflow_handle(delivery_id)

        async def started() -> bool:
            try:
                await delivery.describe()
            except RPCError:
                return False
            return True

        await until(started)
        dashcam = get_stream_handle(b, delivery_id).producer(
            topic=DASHCAM, producer_id="dashcam", attempt=1
        )
        for index in range(3):
            url = f"https://objects.example/{delivery_id}/{index}.mp4"
            object_store[url] = f"segment {index}".encode()
            await dashcam.append(Segment(url=url, seconds=10))
        await delivery.signal(Delivery.arrived)
        feed = await asyncio.wait_for(handle.result(), 30)
        print(
            f"[feed] {len(feed.segments)} segments, closed with {feed.close_result!r}"
        )
        return [object_store[segment.url] for segment in feed.segments]


if __name__ == "__main__":
    run(main)
