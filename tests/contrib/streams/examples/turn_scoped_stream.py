"""Brook API proposal, "A Workflow scopes a Stream to one turn and closes it".

Each turn gets its own Stream, named with ``name=``, which ``async with`` closes at the end of
the turn. An index Stream lists the turns, and continue-as-new passes it to the next run.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.worker import Worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


@dataclass
class TurnStarted:
    n: int
    stream: streams.ClientStream[Events]


@streams.defn(name="index")
class TurnIndex:
    started = streams.topic(TurnStarted)


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.ActivityStream[Events]


@activity.defn
async def call_model_into(input: CallModelInput) -> None:
    await input.stream.topic(Events.tokens).publish(*input.prompt.split())


@dataclass
class ChatTurnsInput:
    prompts: list[str]
    # The first turn number of this run. A Stream name must be unique in the run chain.
    next: int = 0
    # From the run before continue-as-new.
    index: stream_workflow.WorkflowStream[TurnIndex] | None = None


@workflow.defn
class ChatTurns:
    @workflow.run
    async def run(self, input: ChatTurnsInput) -> None:
        index = input.index or await stream_workflow.create_stream(
            TurnIndex, retention=timedelta(days=1)
        )
        started = index.topic(TurnIndex.started)
        batch = input.prompts[:2]
        for i, prompt in enumerate(batch):
            async with await stream_workflow.create_stream(
                Events,
                name=f"turn-{input.next + i}",
                retention=timedelta(days=1),
                close_with_run=False,
            ) as turn:
                # A Stream is data, so the index Record carries the turn's Stream.
                await started.publish(
                    TurnStarted(input.next + i, streams.ClientStream.from_handle(turn))
                )
                await workflow.execute_activity(
                    call_model_into,
                    CallModelInput(prompt, streams.ActivityStream.from_handle(turn)),
                    start_to_close_timeout=timedelta(minutes=5),
                )
        if len(input.prompts) > len(batch):
            workflow.continue_as_new(
                ChatTurnsInput(input.prompts[2:], input.next + len(batch), index)
            )


async def main(client: Client, task_queue: str) -> dict[int, list[str]]:
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ChatTurns],
        activities=[call_model_into],
    ):
        handle = await client.start_workflow(
            ChatTurns.run,
            ChatTurnsInput(["a b", "c", "d e f"]),
            id=f"chat-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()
    stream_client = streams.StreamClient(client)
    index = stream_client.workflow_stream(handle, TurnIndex)
    turns = {}
    async for item in index.subscribe():
        if not isinstance(item, streams.Record):
            continue
        # A Stream from a payload has no client, so the reader gets it again by id.
        turn = stream_client.get_handle(Events, id=item.data.stream.id)
        turns[item.data.n] = [
            token.data
            async for token in turn.subscribe()
            if isinstance(token, streams.Record)
        ]
    return turns
