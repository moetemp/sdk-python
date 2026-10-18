"""Scenario 1a: an order Workflow publishes stages, a courier adds stages, and the app shows each.

The ``Order`` Workflow publishes ``placed`` with ``stream_producer``. Once
the app shows it, the courier appends ``picked up`` and ``out for
delivery`` through a client producer. When the courier reports the
delivery, the Workflow publishes ``delivered`` and finishes its topic. The
app reads the stream with a client read, which holds a long poll on the
store while the stream is idle.

Left out: a push to the app. The app runs its own read loop.

Run with ``python -m tests.contrib.streams.samples.scenarios.s1a_order_tracking``.
"""

from __future__ import annotations

import asyncio

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRecord,
    stream_handle,
    stream_producer,
    topic,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

ORDER = topic("order", str)


@workflow.defn
class Order:
    def __init__(self) -> None:
        self._delivered = False

    @workflow.run
    async def run(self) -> None:
        stages = stream_producer(ORDER)
        stages.append("placed")
        await workflow.wait_condition(lambda: self._delivered)
        stages.append("delivered")
        await stages.finish()

    @workflow.signal
    def delivered(self) -> None:
        self._delivered = True


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "order-1a"
) -> list[str]:
    """Run one order and return the stages the app showed, in order.

    ``client`` must carry the stream store.
    """
    async with Worker(client, task_queue=task_queue, workflows=[Order]):
        handle = await client.start_workflow(
            Order.run, id=workflow_id, task_queue=task_queue
        )
        stream = stream_handle(client, workflow_id, topic=ORDER)
        placed = asyncio.Event()

        def show(record: StreamRecord[str]) -> None:
            if record.value == "placed":
                placed.set()

        seen: list[StreamRecord[str]] = []
        app = asyncio.create_task(follow("app", stream.read(), seen, on_record=show))
        # The courier picks the order up only once it is placed.
        await asyncio.wait_for(placed.wait(), 10)
        courier = stream.producer(producer_id="courier", attempt=1)
        await courier.append("picked up")
        await courier.append("out for delivery")
        await handle.signal(Order.delivered)
        await handle.result()
        await app
        return values(seen)


if __name__ == "__main__":
    run(main)
