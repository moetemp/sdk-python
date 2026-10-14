"""Scenario 5a: a support dashboard opens late and reads the delivery from the start.

The courier appends two stages before anyone reads. Then a support
dashboard opens and reads from ``BEGINNING``, so it gets every stage the
topic still keeps. A live view opens at the same time and reads from
``END``, so it gets only the stages appended after it started.

Left out: the relay half of 5b, where a parent Workflow reads the same
stream. Workflow code can't read a stream in this release.

Run with ``python -m tests.contrib.streams.samples.scenarios.s5a_late_dashboard``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import END, StreamRecord, get_stream_handle, topic
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

STAGES = topic("stages", str)


@dataclass
class Outcome:
    dashboard: list[str]
    live: list[str]


@workflow.defn
class Delivery:
    def __init__(self) -> None:
        self._arrived = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._arrived)

    @workflow.signal
    def arrived(self) -> None:
        self._arrived = True


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "delivery-5a"
) -> Outcome:
    """Run one delivery and return what the late dashboard and the live view saw.

    ``client`` must carry the stream store.
    """
    async with Worker(client, task_queue=task_queue, workflows=[Delivery]):
        handle = await client.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, workflow_id, topic=STAGES)
        courier = stream.producer(producer_id="courier", attempt=1)
        await courier.append("picked up", "at the hub")

        board: list[StreamRecord[str]] = []
        live: list[StreamRecord[str]] = []
        # A read with no cursor starts at BEGINNING.
        dashboard = asyncio.create_task(follow("dashboard", stream.read(), board))
        live_view = asyncio.create_task(follow("live", stream.read(after=END), live))
        # A read takes END when it starts, so both reads start before the
        # next stages.
        await asyncio.sleep(1)
        await courier.append("out for delivery", "delivered")
        await handle.signal(Delivery.arrived)
        await handle.result()
        await asyncio.gather(dashboard, live_view)
        return Outcome(dashboard=values(board), live=values(live))


if __name__ == "__main__":
    run(main)
