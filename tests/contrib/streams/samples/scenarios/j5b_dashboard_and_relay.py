"""Scenario 5b in one namespace: a dashboard and a parent's relay read the child's stream.

The parent ``Order`` relays its ``Delivery`` child's stream with a Workflow
reader, as in ``j1b_parent_relay``. The app reads the parent's stream. Once
the app sees the first relayed stage, a support dashboard opens late on the
child's stream and reads it from ``BEGINNING`` with a client read. Both
readers get every stage, and both sleep on the notifier between stages.

Left out: a child in another namespace, which needs Nexus.

It needs a server with the stream notifier and the Workflow wake target,
and the store's ``notify_on_append()``.

Run with ``python -m tests.contrib.streams.samples.scenarios.j5b_dashboard_and_relay``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from temporalio.client import Client
from temporalio.contrib.streams import BEGINNING, StreamRecord, get_stream_handle
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values
from tests.contrib.streams.samples.scenarios.j1b_parent_relay import (
    RELAY,
    STAGES,
    Delivery,
    Order,
)


@dataclass
class Outcome:
    dashboard: list[str]
    relayed: list[str]


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "order-j5b"
) -> Outcome:
    """Run one order and return what the dashboard and the app saw."""
    delivery_id = f"{workflow_id}-delivery"
    async with Worker(client, task_queue=task_queue, workflows=[Order, Delivery]):
        handle = await client.start_workflow(
            Order.run, delivery_id, id=workflow_id, task_queue=task_queue
        )
        board: list[StreamRecord[str]] = []
        # At most one dashboard, opened by the first relayed stage.
        dashboard: list[asyncio.Task[list[StreamRecord[str]]]] = []

        def open_dashboard(_record: StreamRecord[str]) -> None:
            if not dashboard:
                stream = get_stream_handle(client, delivery_id, topic=STAGES)
                dashboard.append(
                    asyncio.create_task(
                        follow("dashboard", stream.read(after=BEGINNING), board)
                    )
                )

        relayed: list[StreamRecord[str]] = []
        await follow(
            "app",
            get_stream_handle(client, workflow_id, topic=RELAY).read(),
            relayed,
            on_record=open_dashboard,
        )
        await asyncio.gather(*dashboard)
        await handle.result()
    return Outcome(dashboard=values(board), relayed=values(relayed))


if __name__ == "__main__":
    run(main, notify=True)
