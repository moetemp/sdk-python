"""A standalone Activity streams tokens to a stream of its own.

The client starts the Activity with ``start_activity``. No Workflow is
involved. ``activity_handle()`` in a standalone Activity is the Activity's
own stream, and every attempt of the run writes to it. A retry is reported
to readers as ``SUPERSEDED``. The Activity closes the stream as its last
step. If it ends with no close, the server closes the stream with the run's
outcome.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import activity
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    RecordKind,
    StreamRef,
    activity_handle,
    get_stream_handle,
    topic,
)
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

TOKENS = topic("tokens", str)


@activity.defn
async def generate(prompt: str) -> str:
    stream = activity_handle()
    out = stream.producer(topic=TOKENS)
    words = f"an answer to {prompt}".split()
    if activity.info().attempt == 1:
        # The first attempt fails halfway, as a model call might.
        await out.append(*words[:2])
        raise ApplicationError("the model call dropped")
    await out.append(*words)
    await stream.close(topic=TOKENS, result="done")
    return " ".join(words)


async def main(client: Client, task_queue: str, *, activity_id: str = "gen-1") -> str:
    """Run one prompt as a standalone Activity and return the text the client read.

    ``client`` must carry the stream store, for example
    ``Client.connect(..., plugins=[RedisStreams("redis://localhost:6379")])``.
    """
    async with Worker(client, task_queue=task_queue, activities=[generate]):
        handle = await client.start_activity(
            generate,
            "streams",
            id=activity_id,
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
        )
        ref = StreamRef.for_activity(handle.id, run_id=handle.run_id, topic=TOKENS)
        text: list[str] = []
        async for record in get_stream_handle(client, ref).read():
            if record.kind is RecordKind.SUPERSEDED:
                # The retry started over, so what the failed attempt wrote goes.
                text.clear()
            elif record.kind is RecordKind.DATA and record.value is not None:
                text.append(record.value)
        await handle.result()
        return " ".join(text)
