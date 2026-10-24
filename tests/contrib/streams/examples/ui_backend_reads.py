"""Brook API proposal, "A UI backend reads a Workflow's Stream".

The agent Workflow creates the ``events`` Stream. The UI backend starts it, gets the Stream
from the Workflow handle with no call to Temporal, and renders each item as it comes.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import streams
from temporalio.common.streams import workflow as stream_workflow
from temporalio.worker import Worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.ActivityStream[Events]


@activity.defn
async def call_model(input: CallModelInput) -> str:
    tokens = input.stream.topic(Events.tokens)
    for word in input.prompt.split():
        await tokens.publish(word)
    return input.prompt.upper()


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        stream = await stream_workflow.create_stream(Events)
        turns = stream.topic(Events.turns)
        await turns.publish(Turn("model"))
        reply = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(minutes=5),
        )
        await turns.publish(Turn("done", reply))
        return reply


class Screen:
    """Stands in for the UI. It keeps what it shows by offset, so a withdrawal can take items
    back.
    """

    def __init__(self) -> None:
        self.shown: dict[int, streams.Record[Any]] = {}
        self.result: Any = None

    def render(self, record: streams.Record[Any]) -> None:
        self.shown[record.offset] = record

    def unrender(self, offset: int) -> None:
        del self.shown[offset]


async def main(client: Client, task_queue: str) -> Screen:
    async with Worker(
        client, task_queue=task_queue, workflows=[Agent], activities=[call_model]
    ):
        handle = await client.start_workflow(
            Agent.run, "hello there", id=f"agent-{uuid.uuid4()}", task_queue=task_queue
        )
        stream = streams.StreamClient(client).workflow_stream(handle, Events)
        screen = Screen()
        # The Workflow may not have created the Stream yet, so the read waits for it.
        async for item in stream.subscribe(
            Events.turns, Events.tokens, create_wait=timedelta(seconds=10)
        ):
            if isinstance(item, streams.Closed):
                screen.result = item.result if item.failure is None else item.failure
            elif isinstance(item, streams.Superseded):
                # The offsets of the Records that this read showed and the item withdraws.
                for offset in item.withdrawn_offsets:
                    screen.unrender(offset)
            else:
                # item.data is a Turn or a str, by item.topic.
                screen.render(item)
        return screen
