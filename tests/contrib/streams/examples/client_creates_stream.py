"""Brook API proposal, "A client creates a Stream and passes it to a Workflow".

The client creates the Stream with its settings, passes it to the Workflow as data, reads it,
and closes it, since the creator owns it. A create is create-or-get by Stream id.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from temporalio import common, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@workflow.defn
class Chat:
    @workflow.run
    async def run(self, stream: common.WorkflowStream[Turn]) -> None:
        # The Workflow only writes. The client closes the Stream.
        await stream.publish(Turn("model", "hi"), Turn("last", "bye"))


async def main(client: Client, task_queue: str) -> list[Turn]:
    async with Worker(client, task_queue=task_queue, workflows=[Chat]):
        turns = []
        # The block closes the Stream at its end, or with the error that ends it.
        async with client.create_stream(
            Turns, id=f"chat-{uuid.uuid4()}", retention=timedelta(days=1)
        ) as stream:
            await client.start_workflow(
                Chat.run,
                # A Stream is data, so the Workflow gets it as an argument.
                common.WorkflowStream.from_handle(stream),
                id=f"chat-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            async for r in stream.subscribe():
                if not isinstance(r, common.Record):
                    continue
                turns.append(r.data)
                if r.data.kind == "last":
                    break
        return turns
