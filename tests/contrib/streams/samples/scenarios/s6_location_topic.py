"""Scenario 6: a record on one topic opens a read of a second topic.

The courier appends delivery events on ``events`` and the courier's phone
appends positions on ``location``. The app reads ``events``. When it sees
``tracking started``, it opens a second client read on ``location`` from
``BEGINNING``, so it also gets the positions from before that event. Both
reads end when the delivery ends.

Left out: a map. A list of positions stands in for it.

Run with ``python -m tests.contrib.streams.samples.scenarios.s6_location_topic``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    StreamRecord,
    stream_handle,
    topic,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

EVENTS = topic("events", str)
LOCATION = topic("location", str)


@dataclass
class Outcome:
    events: list[str]
    locations: list[str]


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
    client: Client, task_queue: str, *, workflow_id: str = "delivery-6"
) -> Outcome:
    """Run one delivery and return the events and positions the app saw.

    ``client`` must carry the stream store.
    """
    async with Worker(client, task_queue=task_queue, workflows=[Delivery]):
        handle = await client.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )
        stream = stream_handle(client, workflow_id)
        events: list[StreamRecord[str]] = []
        points: list[StreamRecord[str]] = []
        # At most one read of the location, opened by the first event that asks.
        location: list[asyncio.Task[list[StreamRecord[str]]]] = []

        def on_event(record: StreamRecord[str]) -> None:
            if record.value == "tracking started" and not location:
                location.append(
                    asyncio.create_task(
                        follow(
                            "map", stream.read(topic=LOCATION, after=BEGINNING), points
                        )
                    )
                )

        app = asyncio.create_task(
            follow("app", stream.read(topic=EVENTS), events, on_record=on_event)
        )
        courier = stream.producer(topic=EVENTS, producer_id="courier", attempt=1)
        phone = stream.producer(topic=LOCATION, producer_id="phone", attempt=1)
        await courier.append("picked up")
        await phone.append("52.37,4.89")
        await courier.append("tracking started")
        await phone.append("52.36,4.90", "52.35,4.91")
        await courier.append("delivered")
        await handle.signal(Delivery.arrived)
        await handle.result()
        await app
        await asyncio.gather(*location)
        return Outcome(events=values(events), locations=values(points))


if __name__ == "__main__":
    run(main)
