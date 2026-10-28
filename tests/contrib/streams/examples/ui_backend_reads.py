"""Brook API proposal, "A UI backend reads a Workflow's Stream".

The agent Workflow hands out its ``turns`` Stream through a ``stream`` query. The UI backend
starts it, gets the Stream from the query, and renders each item as it arrives. A Stream is
data, so the client reads it as a ``ClientStream``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import activity, common, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@activity.defn
async def call_model(prompt: str) -> str:
    return prompt.upper()


@workflow.defn
class Agent:
    @workflow.query
    def stream(self) -> common.WorkflowStream[Turn]:
        # The handle makes no call, so a reader can get it before the create.
        return workflow.stream_handle(Turns)

    @workflow.run
    async def run(self, prompt: str) -> str:
        turns = await workflow.create_stream(Turns)
        await turns.publish(Turn("model"))
        reply = await workflow.execute_activity(
            call_model, prompt, start_to_close_timeout=timedelta(minutes=5)
        )
        await turns.publish(Turn("done", reply))
        # A run's end doesn't close the Stream, so the Workflow closes it before it returns.
        await turns.close(result=reply)
        return reply


class Screen:
    """A fake UI. It keeps what it shows by offset, so a withdrawal can remove items."""

    def __init__(self) -> None:
        self.shown: dict[int, Turn] = {}
        self.result: Any = None

    def render(self, record: common.Record[Turn]) -> None:
        self.shown[record.offset] = record.data

    def unrender(self, offsets: tuple[int, ...]) -> None:
        for offset in offsets:
            del self.shown[offset]


async def main(client: Client, task_queue: str) -> Screen:
    async with Worker(
        client, task_queue=task_queue, workflows=[Agent], activities=[call_model]
    ):
        handle = await client.start_workflow(
            Agent.run, "hello there", id=f"agent-{uuid.uuid4()}", task_queue=task_queue
        )
        # The Workflow hands out its Stream.
        stream = await handle.query("stream", result_type=common.ClientStream[Turn])
        screen = Screen()
        # The Workflow may not have created the Stream yet, so the read waits for it.
        async for r in stream.subscribe(create_wait=timedelta(seconds=10)):
            match r:
                case common.Closed():
                    screen.result = r.result if r.failure is None else r.failure
                case common.Superseded():
                    # The offsets of the Records that this read showed and the item withdraws.
                    screen.unrender(r.withdrawn_offsets)
                case _:
                    screen.render(r)
        return screen
