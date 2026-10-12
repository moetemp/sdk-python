"""Scenario 5b on Bridge: a dashboard and a parent's relay read the child's stream at once.

The parent relays ``Delivery``'s stream as in n1b. At the same time, a
dashboard client long-polls ``Delivery``'s stream from ``BEGINNING``, as in
s5b. Both get every record.

Left out: a polling reader that the server knows of. The dashboard reads
the store, and the server sees only the relay's operation.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.n5b_dashboard_and_relay``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    StreamNotFoundError,
    StreamRecord,
    get_stream_handle,
)
from temporalio.contrib.streams.nexus import TemporalStreamsHandler
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, values
from tests.contrib.streams.samples.scenarios._nexus import (
    NewStore,
    connect,
    endpoint,
    run,
)
from tests.contrib.streams.samples.scenarios.n1b_parent_relay import (
    RELAY,
    STAGES,
    Delivery,
    DeliveryHandler,
    Order,
)


@dataclass
class Outcome:
    dashboard: list[str]
    relayed: list[str]
    close_result: str


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "order-n5b",
    new_store: NewStore,
) -> Outcome:
    """Run one order and return what the dashboard and the relay saw.

    ``client`` is a client of a server with the stream notifier.
    """
    streams = await connect(client, new_store().notify_on_append())
    name = await endpoint(client, task_queue)
    delivery_id = f"{workflow_id}-delivery"
    async with Worker(
        streams,
        task_queue=task_queue,
        workflows=[Order, Delivery],
        nexus_service_handlers=[DeliveryHandler(), TemporalStreamsHandler()],
    ):
        handle = await streams.start_workflow(
            Order.run, args=[name, delivery_id], id=workflow_id, task_queue=task_queue
        )

        async def dashboard() -> list[StreamRecord[str]]:
            seen: list[StreamRecord[str]] = []
            stream = get_stream_handle(streams, delivery_id)
            while True:
                try:
                    return await follow(
                        "dashboard", stream.read(topic=STAGES, after=BEGINNING), seen
                    )
                except StreamNotFoundError:
                    # The parent has not started the child yet.
                    await asyncio.sleep(0.1)

        relayed: list[StreamRecord[str]] = []
        board: list[StreamRecord[str]]
        board, _ = await asyncio.gather(
            dashboard(),
            follow(
                "backend",
                get_stream_handle(streams, workflow_id).read(topic=RELAY),
                relayed,
            ),
        )
        result = await asyncio.wait_for(handle.result(), 30)
        return Outcome(
            dashboard=values(board), relayed=values(relayed), close_result=result
        )


if __name__ == "__main__":
    run(main)
