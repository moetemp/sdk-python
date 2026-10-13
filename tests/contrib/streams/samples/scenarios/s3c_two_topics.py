"""Scenario 3c: turns and deltas in two topics of one store.

The page keeps the deltas in Redis and the turns in a native stream, with a
reference record between them. Here both topics live in one store, so no
reference record is needed. The model Activity fails on its last attempt.
The backend sees ``SUPERSEDED`` before the retry's deltas, and the failed
attempt's partial deltas stay.

Left out: the native stream and a retraction the server writes when an
attempt times out. Both are out of scope for this release.

Run with ``python -m tests.contrib.streams.samples.scenarios.s3c_two_topics``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    StreamRecord,
    activity_handle,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, label, run

TURNS = topic("turns", str)
DELTAS = topic("deltas", str)


@activity.defn
async def reply() -> str:
    deltas = activity_handle().producer(topic=DELTAS)
    if activity.info().attempt == 1:
        await deltas.append("Hel")
    else:
        await deltas.append("Hel", "l")
    raise ApplicationError("the model call dropped")


@workflow.defn
class ChatWorkflow:
    @workflow.run
    async def run(self) -> None:
        turns = workflow_writer(TURNS)
        turns.publish("turn started")
        try:
            await workflow.execute_activity(
                reply,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(milliseconds=100), maximum_attempts=2
                ),
            )
            turns.publish("turn finished")
        except ActivityError:
            turns.publish("turn failed")


@dataclass
class Seen:
    turns: list[str]
    deltas: list[str]


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "chat-3c"
) -> Seen:
    """Run the turn and return a label for each record on each topic.

    ``client`` must carry the stream store.
    """
    async with Worker(
        client, task_queue=task_queue, workflows=[ChatWorkflow], activities=[reply]
    ):
        handle = await client.start_workflow(
            ChatWorkflow.run, id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, workflow_id)
        turns: list[StreamRecord[str]] = []
        deltas: list[StreamRecord[str]] = []
        await asyncio.gather(
            follow("turns", stream.read(topic=TURNS), turns),
            follow("deltas", stream.read(topic=DELTAS), deltas),
        )
        await handle.result()
        return Seen(turns=[label(r) for r in turns], deltas=[label(r) for r in deltas])


if __name__ == "__main__":
    run(main)
