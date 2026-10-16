"""Scenario 2: dashcam segments by reference.

The courier client appends a ``Segment`` record for each recorded segment.
The record carries the segment's URL, not the video. The backend fetches
each segment from the object store. A dict in this process stands in for
the object store.

Left out: live video and storage that Temporal manages. Both are out of
scope for this release.

Run with ``python -m tests.contrib.streams.samples.scenarios.s2_dashcam_reference``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import StreamRecord, get_stream_handle, topic
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values


@dataclass
class Segment:
    url: str
    seconds: int


DASHCAM = topic("dashcam", Segment)


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
    client: Client, task_queue: str, *, workflow_id: str = "delivery-2"
) -> list[bytes]:
    """Record three segments and return the bytes the backend fetched, in order.

    ``client`` must carry the stream store.
    """
    object_store: dict[str, bytes] = {}
    async with Worker(client, task_queue=task_queue, workflows=[Delivery]):
        handle = await client.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, workflow_id)
        fetched: list[bytes] = []

        def fetch(record: StreamRecord[Segment]) -> None:
            if record.value is not None:
                fetched.append(object_store[record.value.url])

        seen: list[StreamRecord[Segment]] = []
        backend = asyncio.create_task(
            follow("backend", stream.read(topic=DASHCAM), seen, on_record=fetch)
        )
        courier = stream.producer(topic=DASHCAM, producer_id="dashcam", attempt=1)
        for index in range(3):
            url = f"https://objects.example/{workflow_id}/{index}.mp4"
            # The bytes are stored before the reference, so a reader never
            # sees a URL that doesn't resolve yet.
            object_store[url] = f"segment {index}".encode()
            await courier.append(Segment(url=url, seconds=10))
        await handle.signal(Delivery.arrived)
        await handle.result()
        await backend
        assert len(values(seen)) == len(fetched)
        return fetched


if __name__ == "__main__":
    run(main)
