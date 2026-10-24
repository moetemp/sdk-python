"""Brook API proposal, "An agent Workflow publishes its turns".

The Workflow creates its Stream, publishes each turn with an awaited ``publish``, and passes the
Stream to the model Activity in its one input object. It closes the Stream with its result
before it returns, since a run end closes nothing.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import streams
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
class Reply:
    text: str
    tool_calls: list[str] = field(default_factory=list)


@dataclass
class CallModelInput:
    messages: list[str]
    stream: streams.ActivityStream[Events]


@activity.defn
async def call_model(input: CallModelInput) -> Reply:
    await input.stream.topic(Events.tokens).publish(*input.messages[-1].split())
    if len(input.messages) == 1:
        return Reply("", tool_calls=["search"])
    return Reply("the answer")


@activity.defn
async def run_tool(call: str) -> str:
    return f"{call} result"


@workflow.defn
class AgentTurns:
    @workflow.run
    async def run(self, messages: list[str]) -> str:
        stream = await workflow.create_stream(Events, retention=timedelta(days=1))
        turns = stream.topic(Events.turns)
        while True:
            # The publish is one Local Activity. History keeps its offset, not the Turn.
            await turns.publish(Turn("model"))
            reply = await workflow.execute_activity(
                call_model,
                CallModelInput(messages, streams.ActivityStream.from_handle(stream)),
                start_to_close_timeout=timedelta(minutes=5),
            )
            if not reply.tool_calls:
                await turns.publish(Turn("done", reply.text))
                # A run end closes nothing, so the Workflow closes its Stream.
                await stream.close(result=reply.text)
                return reply.text
            for call in reply.tool_calls:
                messages.append(
                    await workflow.execute_activity(
                        run_tool, call, start_to_close_timeout=timedelta(minutes=5)
                    )
                )


async def main(
    client: Client, task_queue: str
) -> list[streams.Record | streams.Closed]:
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[AgentTurns],
        activities=[call_model, run_tool],
    ):
        handle = await client.start_workflow(
            AgentTurns.run,
            ["find it"],
            id=f"agent-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()
        stream = handle.stream(Events)
        return [
            item
            async for item in stream.subscribe()
            if isinstance(item, (streams.Record, streams.Closed))
        ]
