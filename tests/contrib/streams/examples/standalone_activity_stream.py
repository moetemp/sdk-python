"""Jetty v2, "A Standalone Activity streams its progress".

A Standalone Activity, one that no Workflow runs, owns a Stream on the native backend with
``activity.create_stream``. A client gets it from the Activity handle without a call. The server
closes the Stream when the Activity ends, with its result, so the Activity doesn't need to close
it.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from temporalio import activity, common
from temporalio.client import Client
from temporalio.worker import Worker


@common.defn(name="events")
class Events:
    tokens = common.topic(str)


@activity.defn
async def answer(question: str) -> str:
    stream = await activity.create_stream(Events)
    for word in question.split():
        await stream.topic(Events.tokens).publish(word)
    return question.upper()


async def main(client: Client, task_queue: str) -> list[str]:
    async with Worker(client, task_queue=task_queue, activities=[answer]):
        handle = await client.start_activity(
            answer,
            "find the answer",
            id=f"answer-{uuid.uuid4()}",
            task_queue=task_queue,
            start_to_close_timeout=timedelta(seconds=30),
        )
        seen = []
        # The handle has the run id, so the client builds the Stream with no call.
        async for item in handle.stream(Events).subscribe(
            create_wait=timedelta(seconds=30)
        ):
            if isinstance(item, common.Record):
                seen.append(item.data)
            elif isinstance(item, common.Closed):
                seen.append(f"activity said {item.result}")
        return seen
