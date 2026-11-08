"""Streams API proposal, "A UI backend reads a Stream by its id".

A chat server creates the Stream and passes its ref to the agent Workflow, which publishes each
turn. The UI backend gets the Stream by its id, with no call, and renders each item as it
arrives. It may read before the creator has created the Stream, so the read waits for the
create. The read ends when the chat server closes the Stream.
"""

from __future__ import annotations

import asyncio
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


@activity.defn
async def call_model(prompt: str) -> str:
    return prompt.upper()


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, ref: streams.StreamRef[Turn], prompt: str) -> str:
        turns = workflow.get_stream(ref)
        await turns.append(Turn("model"))
        reply = await workflow.execute_activity(
            call_model, prompt, start_to_close_timeout=timedelta(minutes=5)
        )
        await turns.append(Turn("done", reply))
        return reply


async def chat_server(client: Client, chat_id: str, task_queue: str) -> None:
    # The block end closes the Stream after the agent's done Turn.
    async with await client.create_stream(chat_id, payload_type=Turn) as turns:
        await client.execute_workflow(
            Agent.run,
            args=[turns.ref, "hello there"],
            id=chat_id,
            task_queue=task_queue,
        )


class Screen:
    """A fake UI. It keeps what it shows by offset, so a withdrawal can remove items."""

    def __init__(self) -> None:
        self.shown: dict[streams.StreamOffset, Turn] = {}

    def render(self, turn: Turn, offset: streams.StreamOffset) -> None:
        self.shown[offset] = turn

    def unrender(self, offsets: tuple[streams.StreamOffset, ...]) -> None:
        for offset in offsets:
            del self.shown[offset]


# Where the view goes on. A first view has none, so it reads from the start.
saved: list[streams.StreamOffset] = []


def saved_offset() -> streams.StreamOffset | None:
    return saved[-1] if saved else None


async def main(client: Client, task_queue: str) -> Screen:
    async with Worker(
        client, task_queue=task_queue, workflows=[Agent], activities=[call_model]
    ):
        chat_id = f"chat-{uuid.uuid4()}"
        server = asyncio.create_task(chat_server(client, chat_id, task_queue))
        screen = Screen()
        stream = client.get_stream(chat_id, payload_type=Turn)  # by id, with no call
        async for r in stream.read(
            offset=saved_offset(),  # wherever the app keeps it, or streams.END
            create_wait=timedelta(seconds=10),  # the creator may not have created it
        ):
            match r.kind:  # the kind narrows the item's type
                case streams.ItemKind.SUPERSEDED:
                    screen.unrender(r.withdrawn_offsets)
                case _:
                    screen.render(r.data, r.offset)  # a Turn
        await server
        return screen
