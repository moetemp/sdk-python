"""Scenario 1a: an order Workflow and two outside producers, followed by a backend.

``OrderWorkflow`` publishes ``placed``, which becomes visible when the
activation that published it completes. The restaurant and the courier are clients that
append stages as outside producers, each with its own ``producer_id`` and
``attempt=1``. Their appends don't wake the Workflow. When the courier
delivers, the Workflow writes ``FINISH``, and the backend's read ends when
the Workflow closes.

Left out: the Worker Callback that pushes each record to the app. The
backend long-polls from a cursor instead.

Run with ``python -m tests.contrib.streams.samples.scenarios.s1a_order_tracking``.
"""

from __future__ import annotations

import asyncio

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRecord,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, label, run, who

ORDER = topic("order", str)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self._delivered = False

    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(ORDER)
        stages.publish("placed")
        await workflow.wait_condition(lambda: self._delivered)
        stages.finish()

    @workflow.signal
    def delivered(self) -> None:
        self._delivered = True


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "order-1a"
) -> list[tuple[str, str]]:
    """Run one order and return each record the backend saw, as (producer, value).

    ``client`` must carry the stream store.
    """
    async with Worker(client, task_queue=task_queue, workflows=[OrderWorkflow]):
        handle = await client.start_workflow(
            OrderWorkflow.run, id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, workflow_id)
        placed = asyncio.Event()

        def wake(record: StreamRecord[str]) -> None:
            if record.value == "placed":
                placed.set()

        seen: list[StreamRecord[str]] = []
        backend = asyncio.create_task(
            follow("backend", stream.read(topic=ORDER), seen, on_record=wake)
        )
        await placed.wait()
        restaurant = stream.producer(topic=ORDER, producer_id="restaurant", attempt=1)
        await restaurant.append("accepted")
        await restaurant.append("cooking", "ready")
        courier = stream.producer(topic=ORDER, producer_id="courier", attempt=1)
        await courier.append("picked up")
        await courier.append("delivered")
        await handle.signal(OrderWorkflow.delivered)
        await handle.result()
        await backend
        return [(who(r), label(r).split("@")[0]) for r in seen]


if __name__ == "__main__":
    run(main)
