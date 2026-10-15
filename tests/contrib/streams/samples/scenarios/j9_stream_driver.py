"""A StreamDriver wakes a sleeping client, and the producer calls nothing in Temporal.

The store keeps a notification log: each append writes a log entry in the
same store step, in place of the producer's call to the server. A
``StreamDriver`` in the same process reads that log through Core and calls
``NotifyStreams`` for the streams that moved, and a client reader that sleeps
on the stream's notifier wakes. In production the driver runs in a process of
its own, one or more per store, each with its own consumer name.

The owner publishes three bursts with gaps longer than the reader's 2 second
idle wait, then closes its stream. The reader sleeps in each gap and wakes on
the next burst.

It needs a server with the stream notifier and ``NotifyStreams``, and the
store's ``notify_on_append(notification_log=True)``.

Run with ``python -m tests.contrib.streams.samples.scenarios.j9_stream_driver``.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamDriver,
    StreamRecord,
    close_workflow_stream,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

UPDATES = topic("updates", str)


@workflow.defn
class Bursts:
    @workflow.run
    async def run(self) -> None:
        writer = workflow_writer(UPDATES)
        for burst in range(3):
            for n in range(2):
                writer.publish(f"burst {burst} update {n}")
            await workflow.sleep(timedelta(seconds=4))
        await close_workflow_stream(UPDATES, result="three bursts")


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "bursts-j9"
) -> list[str]:
    """Run the owner, a driver and a client reader, and return what the reader read.

    ``client`` must carry a store with ``notify_on_append(notification_log=True)``.
    """
    driver = asyncio.create_task(
        StreamDriver(client, consumer=f"{task_queue}-driver").run()
    )
    seen: list[StreamRecord[str]] = []
    try:
        async with Worker(client, task_queue=task_queue, workflows=[Bursts]):
            await client.start_workflow(
                Bursts.run, id=workflow_id, task_queue=task_queue
            )
            records = get_stream_handle(client, workflow_id, topic=UPDATES).read()
            await follow("client", records, seen)
    finally:
        driver.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await driver
    return values(seen)


if __name__ == "__main__":
    run(main, log=True)
