"""Brook API proposal, "An agent Workflow publishes its turns".

The Workflow creates its two Streams, turns and token deltas, and hands out the turns Stream
through a ``stream`` query. It publishes each turn with an awaited ``publish``, and passes the
tokens Stream to the model Activity in its one input object. A create is create-or-get, so the
run after continue-as-new gets the same Streams. It closes both Streams before it returns, since
a run's end doesn't close a Stream.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, common, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@common.defn(name="tokens")
class Tokens(common.StreamDefinition[str]): ...


@dataclass
class Reply:
    text: str
    tool_calls: list[str] = field(default_factory=list)


@dataclass
class CallModelInput:
    messages: list[str]
    tokens: common.ActivityStream[str]


@activity.defn
async def call_model(input: CallModelInput) -> Reply:
    await input.tokens.publish(*input.messages[-1].split())
    if len(input.messages) == 1:
        return Reply("", tool_calls=["search"])
    return Reply("the answer")


@activity.defn
async def run_tool(call: str) -> str:
    return f"{call} result"


@workflow.defn
class Agent:
    @workflow.query
    def stream(self) -> common.WorkflowStream[Turn]:
        # The handle makes no call, so a reader can get it before the create.
        return workflow.stream_handle(Turns)

    @workflow.run
    async def run(self, messages: list[str]) -> str:
        # Create-or-get, so a retry or the run after continue-as-new gets the same Streams.
        turns = await workflow.create_stream(Turns, retention=timedelta(days=1))
        tokens = await workflow.create_stream(Tokens, retention=timedelta(days=1))
        while True:
            if workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(messages)
            # The publish is one Local Activity. History keeps its offset, not the Turn.
            await turns.publish(Turn("model"))
            reply = await workflow.execute_activity(
                call_model,
                CallModelInput(messages, common.ActivityStream.from_handle(tokens)),
                start_to_close_timeout=timedelta(minutes=5),
            )
            if not reply.tool_calls:
                await turns.publish(Turn("done", reply.text))
                # A run's end doesn't close the Streams, so the Workflow closes them.
                await turns.close(result=reply.text)
                await tokens.close()
                return reply.text
            for call in reply.tool_calls:
                messages.append(
                    await workflow.execute_activity(
                        run_tool, call, start_to_close_timeout=timedelta(minutes=5)
                    )
                )


async def main(client: Client, task_queue: str) -> list[common.Record | common.Closed]:
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Agent],
        activities=[call_model, run_tool],
    ):
        handle = await client.start_workflow(
            Agent.run,
            ["find it"],
            id=f"agent-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await handle.result()
        # The Workflow hands out its turns Stream, and the client reads it as a ClientStream.
        stream = await handle.query("stream", result_type=common.ClientStream[Turn])
        return [
            item
            async for item in stream.subscribe()
            if isinstance(item, (common.Record, common.Closed))
        ]
