"""Scenario 5c on Bridge: callers attach to a stream late, one even after its close.

``Delivery`` publishes three stages, waits, then publishes three more and
closes its stream. A first ``Follower`` Workflow starts the stream operation
at once, a second after three records, and a third after the close. The
operation only attaches, so it never starts a delivery. Each follower reads
the backlog from ``BEGINNING`` through the stream service, and all three get
every record and the close result. The third completes at once, since the
notifier answers a late attach for ``streamnotifier.closedRetention``.

Left out: a standalone Nexus operation as the dashboard, and a push of the
backlog. The dashboard is a small Workflow, and it reads the backlog.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7961
python -m tests.contrib.streams.samples.scenarios.n5c_late_attach``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import nexusrpc
import nexusrpc.handler

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    StreamRef,
    close_workflow_stream,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NewStore,
    connect,
    endpoint,
    run,
)

STAGES = topic("stages", str)
FIRST = ["assigned", "picked up", "on the way"]
LAST = ["at the door", "delivered", "rated"]


@nexusrpc.service
class DeliveryService:
    follow: nexusrpc.Operation[str, str]


@workflow.defn
class Delivery:
    def __init__(self) -> None:
        self._go_on = False

    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        for stage in FIRST:
            stages.publish(stage)
        await workflow.wait_condition(lambda: self._go_on)
        for stage in LAST:
            stages.publish(stage)
        await close_workflow_stream(STAGES, result=f"{len(FIRST) + len(LAST)} stages")

    @workflow.signal
    def go_on(self) -> None:
        self._go_on = True


async def attach_only(_ctx: Any, delivery_id: str) -> StreamRef:
    # A follower must not start a delivery, so the operation names the stream only.
    return StreamRef.for_workflow(delivery_id, topic=STAGES.name)


@nexusrpc.handler.service_handler(service=DeliveryService)
class DeliveryHandler:
    @nexusrpc.handler.operation_handler
    def follow(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(attach_only)


@dataclass
class Followed:
    stages: list[str] = field(default_factory=list)
    close_result: str = ""


@workflow.defn
class Follower:
    @workflow.run
    async def run(self, endpoint: str, delivery_id: str) -> Followed:
        client = workflow.create_nexus_client(
            service=DeliveryService, endpoint=endpoint
        )
        handle = await client.start_operation(DeliveryService.follow, delivery_id)
        reader = StreamReader(handle, item_type=str, endpoint=endpoint)
        followed = Followed()
        while (batch := await reader.next()) is not None:
            followed.stages.extend(batch)
        followed.close_result = await handle
        return followed


@dataclass
class Outcome:
    early: Followed
    late: Followed
    after_close: Followed
    #: How long the follower that attached after the close took, in seconds.
    after_close_seconds: float


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "delivery-n5c",
    new_store: NewStore,
) -> Outcome:
    """Run the delivery with three followers and return what each one got.

    ``client`` is a client of a server with the stream notifier.
    """
    streams = await connect(client, new_store().notify_on_append())
    name = await endpoint(client, task_queue)
    async with Worker(
        streams,
        task_queue=task_queue,
        workflows=[Delivery, Follower],
        nexus_service_handlers=[DeliveryHandler(), TemporalStreamsHandler()],
    ):
        delivery = await streams.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )

        async def follower(suffix: str) -> Any:
            return await streams.start_workflow(
                Follower.run,
                args=[name, workflow_id],
                id=f"{workflow_id}-{suffix}",
                task_queue=task_queue,
            )

        early = await follower("early")
        seen = 0
        records = get_stream_handle(streams, workflow_id).read(topic=STAGES)
        async for record in records:
            seen += record.kind is RecordKind.DATA
            if seen == len(FIRST):
                break
        await records.aclose()
        late = await follower("late")
        await delivery.signal(Delivery.go_on)
        await delivery.result()
        started = asyncio.get_running_loop().time()
        after_close = await follower("after-close")
        after_close_result = await asyncio.wait_for(after_close.result(), 30)
        took = asyncio.get_running_loop().time() - started
        return Outcome(
            early=await asyncio.wait_for(early.result(), 30),
            late=await asyncio.wait_for(late.result(), 30),
            after_close=after_close_result,
            after_close_seconds=took,
        )


if __name__ == "__main__":
    run(main)
