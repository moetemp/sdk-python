"""Scenario 1a with U5: the app's Worker gets each stage pushed, with no long-poll.

``OrderWorkflow`` publishes the order's stages and closes its stream when the
order is delivered. A stream operation names the order's stream. The app
follows it with ``follow_stream``: a standalone Nexus operation with a
callback to the app's own Worker. The server calls that Worker each time the
stream moves, and ``StreamFollowerService`` reads the new stages and hands
them to the app's follower, then the close. The app code has no read loop.

Left out: data in the callback, since each call carries progress and the
follower reads the records. One callback per record. An endpoint is still
needed, even in one namespace.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.u1a_worker_push``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import nexusrpc
import nexusrpc.handler

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef, topic, workflow_writer
from temporalio.contrib.streams.nexus import (
    FollowBatch,
    FollowOutcome,
    StreamFollowerService,
    StreamOperationHandler,
    close_workflow_stream,
    follow_stream,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NewStore,
    connect,
    endpoint,
    run,
    until,
)

ORDER = topic("order", str)
STAGES = ["placed", "cooking", "picked up", "delivered"]


@nexusrpc.service
class OrderService:
    order_stream: nexusrpc.Operation[str, str]


@workflow.defn
class OrderWorkflow:
    """Publishes a stage each time it is told to advance, then closes the stream."""

    def __init__(self) -> None:
        self._advanced = 0

    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(ORDER)
        for index, stage in enumerate(STAGES):
            await workflow.wait_condition(lambda: self._advanced > index)
            stages.publish(stage)
        close_workflow_stream(f"{len(STAGES)} stages", topic=ORDER)

    @workflow.signal
    def advance(self) -> None:
        self._advanced += 1


async def name_order(_ctx: Any, order_id: str) -> StreamRef:
    return StreamRef.for_workflow(order_id, topic=ORDER.name)


@nexusrpc.handler.service_handler(service=OrderService)
class OrderHandler:
    @nexusrpc.handler.operation_handler
    def order_stream(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(name_order)


@dataclass
class AppFollower:
    """What the app does with each pushed batch. It keeps one copy per cursor."""

    stages: dict[str, str] = field(default_factory=dict)
    batches: int = 0
    outcome: FollowOutcome | None = None
    closed: asyncio.Event = field(default_factory=asyncio.Event)

    async def on_records(self, batch: FollowBatch[str]) -> None:
        self.batches += 1
        for record in batch.records:
            if record.cursor.token not in self.stages:
                print(f"[app] {record.value}")
            self.stages.setdefault(record.cursor.token, record.value or "")

    async def on_close(self, outcome: FollowOutcome) -> None:
        print(f"[app] closed with {outcome.result!r}")
        self.outcome = outcome
        self.closed.set()


@dataclass
class Outcome:
    stages: list[str]
    close_result: Any
    batches: int


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "order-u1a",
    new_store: NewStore,
) -> Outcome:
    """Run one order and return what the app's follower got.

    ``client`` is a client of a server with the stream notifier and NexusHandler
    callbacks on standalone operations.
    """
    streams = await connect(client, new_store().notify_on_append())
    name = await endpoint(client, task_queue)
    follower = AppFollower()
    async with Worker(
        streams,
        task_queue=task_queue,
        workflows=[OrderWorkflow],
        nexus_service_handlers=[
            OrderHandler(),
            StreamFollowerService({"order": follower}, client=streams),
        ],
    ):
        order = await streams.start_workflow(
            OrderWorkflow.run, id=workflow_id, task_queue=task_queue
        )
        await follow_stream(
            streams,
            endpoint=name,
            service=OrderService,
            operation=OrderService.order_stream,
            arg=workflow_id,
            follower="order",
            task_queue=task_queue,
        )
        for count in range(1, len(STAGES) + 1):
            await order.signal(OrderWorkflow.advance)

            async def reached(count: int = count) -> bool:
                return len(follower.stages) >= count

            # Each stage reaches the app before the next one is made.
            await until(reached)
        await asyncio.wait_for(follower.closed.wait(), 30)
        await order.result()
    assert follower.outcome is not None
    return Outcome(
        stages=list(follower.stages.values()),
        close_result=follower.outcome.result,
        batches=follower.batches,
    )


if __name__ == "__main__":
    run(main)
