"""Streams API proposal, "A client creates a Stream and passes it to a Workflow".

The client creates the Stream with its settings, passes its ref to the Workflow as data, reads
it, and closes it at the end of ``async with``. The create fails if the id exists, and
``create_or_get_stream`` would get the Stream instead. The Workflow gets the ref as data, turns
it into a Stream to write, and passes the same ref to an Activity.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, streams, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@dataclass
class Turn:
    kind: str
    text: str | None = None


SECONDS_30 = timedelta(seconds=30)


@activity.defn
async def notify_user(ref: streams.StreamRef[Turn]) -> str:
    # A real Activity sends the Stream id to the user's device, which reads it by id.
    return ref.id


@workflow.defn
class Chat:
    @workflow.run
    async def run(self, ref: streams.StreamRef[Turn]) -> None:
        # The Workflow only writes. The client closes the Stream.
        turns = workflow.to_stream(ref)
        turns.publish(Turn("model", "hi"))
        await workflow.execute_activity(
            notify_user, ref, start_to_close_timeout=SECONDS_30
        )
        await turns.publish(Turn("last", "bye"))


def is_last_turn(r: streams.Record[Turn]) -> bool:
    return r.data.kind == "last"


async def main(client: Client, task_queue: str) -> list[Turn]:
    async with Worker(
        client, task_queue=task_queue, workflows=[Chat], activities=[notify_user]
    ):
        chat_id = f"chat-{uuid.uuid4()}"
        turns: list[Turn] = []
        render = turns.append
        # create_stream raises if the id exists, and create_or_get_stream gets it instead.
        stream = await client.create_stream[Turn](chat_id, retention=timedelta(days=1))
        async with stream:  # optional, the block end closes it
            await client.start_workflow(
                Chat.run,
                stream.ref,  # the ref is data, and stream.proto_ref goes in a proto message
                id=chat_id,
                task_queue=task_queue,
            )
            async for r in stream.subscribe():
                if r.kind != streams.ItemKind.RECORD:
                    continue
                render(r.data)
                if is_last_turn(r):
                    break
        return turns
