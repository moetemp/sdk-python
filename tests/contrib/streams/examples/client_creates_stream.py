"""Brook API proposal, "A client creates a Stream and passes it to a Workflow".

The client creates the Stream with its settings, passes it to the Workflow as data, reads it,
and closes it, since the creator owns it.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.common import streams
from temporalio.contrib import stream as redis
from temporalio.worker import Worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)


@workflow.defn
class Chat:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> None:
        # The Workflow only writes. The client closes the Stream.
        await stream.topic(Events.turns).publish(
            Turn("model", "hi"), Turn("last", "bye")
        )


async def main(client: Client, task_queue: str) -> list[Turn]:
    async with Worker(client, task_queue=task_queue, workflows=[Chat]):
        turns = []
        # The block closes the Stream at its end, or with the error that ends it.
        async with redis.create(
            client,
            Events,
            id=f"chat-{uuid.uuid4()}",
            retention=timedelta(days=1),
        ) as chat_stream:
            await client.start_workflow(
                Chat.run,
                # A Stream is data, so the Workflow gets it as an argument.
                streams.WorkflowStream.from_handle(chat_stream),
                id=f"chat-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            async for item in chat_stream.subscribe():
                if not isinstance(item, streams.Record):
                    continue
                turns.append(item.data)
                if item.data.kind == "last":
                    break
        return turns
