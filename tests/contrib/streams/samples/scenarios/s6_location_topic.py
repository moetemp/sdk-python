"""Scenario 6: live location on a second topic, read after an in-band wake.

``Delivery`` publishes ``tracking started`` on ``events``. The courier
client appends positions on ``location``. The backend reads ``events``, and
when it sees ``tracking started`` it starts to read ``location`` too. Both
topics belong to the same Workflow, in one namespace.

Left out: the backend in another namespace. It would need Nexus direct
calls, which don't exist yet. A cheaper storage tier for ``location`` is
also out of scope.

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
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values


@dataclass
class Position:
    lat: float
    lon: float


EVENTS = topic("events", str)
LOCATION = topic("location", Position)
POSITIONS = [Position(52.52, 13.40), Position(52.53, 13.41), Position(52.54, 13.42)]


@workflow.defn
class Delivery:
    def __init__(self) -> None:
        self._arrived = False

    @workflow.run
    async def run(self) -> None:
        events = workflow_writer(EVENTS)
        events.publish("picked up")
        events.publish("tracking started")
        await workflow.wait_condition(lambda: self._arrived)
        events.publish("arrived")

    @workflow.signal
    def arrived(self) -> None:
        self._arrived = True


@dataclass
class Seen:
    events: list[str]
    positions: list[Position]
    #: How many events the backend had read when it opened ``location``.
    events_before_location: int


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "delivery-6"
) -> Seen:
    """Run one delivery and return what the backend saw on each topic.

    ``client`` must carry the stream store.
    """
    async with Worker(client, task_queue=task_queue, workflows=[Delivery]):
        handle = await client.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, workflow_id)
        events: list[StreamRecord[str]] = []
        positions: list[StreamRecord[Position]] = []
        location_reads: list[asyncio.Task] = []
        events_before_location = 0
        tracking = asyncio.Event()
        all_positions = asyncio.Event()

        def on_position(_: StreamRecord[Position]) -> None:
            if len(values(positions)) == len(POSITIONS):
                all_positions.set()

        def on_event(record: StreamRecord[str]) -> None:
            nonlocal events_before_location
            if record.value == "tracking started" and not location_reads:
                events_before_location = len(events)
                location_reads.append(
                    asyncio.create_task(
                        follow(
                            "location",
                            stream.read(topic=LOCATION, after=BEGINNING),
                            positions,
                            on_record=on_position,
                        )
                    )
                )
                tracking.set()

        events_read = asyncio.create_task(
            follow("events", stream.read(topic=EVENTS), events, on_record=on_event)
        )
        await tracking.wait()
        courier = stream.producer(
            topic=LOCATION, producer_id="courier-phone", attempt=1
        )
        for position in POSITIONS:
            await courier.append(position)
        await all_positions.wait()
        await handle.signal(Delivery.arrived)
        await asyncio.gather(events_read, *location_reads)
        await handle.result()
        return Seen(
            events=values(events),
            positions=values(positions),
            events_before_location=events_before_location,
        )


if __name__ == "__main__":
    run(main)
