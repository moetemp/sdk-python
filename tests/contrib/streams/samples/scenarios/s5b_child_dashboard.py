"""Scenario 5b: a dashboard follows a child Workflow's stream from the start.

A parent order starts the child ``Delivery``, which publishes its stages.
The dashboard long-polls ``Delivery``'s stream from ``BEGINNING``. It knows
the child's Workflow id, so it needs nothing from the parent.

Left out: the parent relaying the child's stream. A Workflow can't read a
stream in this release, so that half is the Bridge demo n5b.

Run with ``python -m tests.contrib.streams.samples.scenarios.s5b_child_dashboard``.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    StreamNotFoundError,
    StreamRecord,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

STAGES = topic("stages", str)


@workflow.defn
class Delivery:
    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        for stage in ("assigned", "picked up", "on the way", "delivered"):
            stages.publish(stage)
            await workflow.sleep(timedelta(milliseconds=100))


@workflow.defn
class Order:
    @workflow.run
    async def run(self, delivery_id: str) -> None:
        await workflow.execute_child_workflow(Delivery.run, id=delivery_id)


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "order-5b"
) -> list[str]:
    """Run one order and return the child's stages the dashboard saw.

    ``client`` must carry the stream store.
    """
    delivery_id = f"{workflow_id}-delivery"
    async with Worker(client, task_queue=task_queue, workflows=[Order, Delivery]):
        handle = await client.start_workflow(
            Order.run, delivery_id, id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, delivery_id)
        seen: list[StreamRecord[str]] = []
        while True:
            try:
                await follow(
                    "dashboard", stream.read(topic=STAGES, after=BEGINNING), seen
                )
                break
            except StreamNotFoundError:
                # The parent has not started the child yet.
                await asyncio.sleep(0.1)
        await handle.result()
        return values(seen)


if __name__ == "__main__":
    run(main)
