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
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
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
    async def run(self, stream: stream_workflow.WorkflowStream[Events]) -> None:
        # The Workflow only writes. The client, or auto-close, closes the Stream.
        await stream.topic(Events.turns).publish(
            Turn("model", "hi"), Turn("last", "bye")
        )


async def main(client: Client, task_queue: str) -> list[Turn]:
    async with Worker(client, task_queue=task_queue, workflows=[Chat]):
        chat_stream = await streams.StreamClient(client).create(
            Events,
            id=f"chat-{uuid.uuid4()}",
            retention=timedelta(days=1),
            auto_close=timedelta(hours=1),
        )
        await client.start_workflow(
            Chat.run,
            # A Stream is data, so the Workflow gets it as an argument.
            stream_workflow.WorkflowStream.from_handle(chat_stream),
            id=f"chat-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        turns = []
        async for item in chat_stream.subscribe():
            if not isinstance(item, streams.Record):
                continue
            turns.append(item.data)
            if item.data.kind == "last":
                break
        await chat_stream.close()
        return turns
