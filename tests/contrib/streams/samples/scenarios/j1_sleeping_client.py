"""A client reader sleeps on the stream's notifier between bursts and wakes on each one.

The owner Workflow publishes three bursts, with a gap longer than the
reader's 2 second idle wait between them, then closes its stream. During
each gap the client's read arms the stream and waits on the notifier with a
long poll, so it holds no store connection. The first append of the next
burst notifies once, and the reader reads the burst.

It needs a server with the stream notifier, and the store's
``notify_on_append()``.

Run with ``python -m tests.contrib.streams.samples.scenarios.j1_sleeping_client``.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
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
            await writer.flush()
            await workflow.sleep(timedelta(seconds=4))
        await close_workflow_stream(UPDATES, result="three bursts")


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "bursts-j1"
) -> list[str]:
    """Run the owner and follow it from a client, and return what the client read."""
    seen: list[StreamRecord[str]] = []
    async with Worker(client, task_queue=task_queue, workflows=[Bursts]):
        await client.start_workflow(Bursts.run, id=workflow_id, task_queue=task_queue)
        records = get_stream_handle(client, workflow_id, topic=UPDATES).read()
        await follow("client", records, seen)
    return values(seen)


if __name__ == "__main__":
    run(main, notify=True)
