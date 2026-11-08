"""Streams API proposal, "An agent Workflow publishes its turns".

The Workflow creates its two Streams, turns and token deltas, under ids of its choice, and hands
out the turns Stream's ref through a ``stream`` query. It appends each turn with an awaited
``append``, and passes the tokens ref to the model Activity in its one input object. A run
after continue-as-new gets the earlier run's refs and creates nothing. It closes both Streams
before it returns, since a run's end doesn't close a Stream.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import activity, streams, workflow
from temporalio.client import BufferedAppender, Client
from temporalio.worker import Worker

MINUTE = timedelta(minutes=1)

Message = str


@dataclass
class Turn:
    kind: str
    text: str | None = None


@dataclass
class Reply:
    text: str
    tool_calls: list[str] = field(default_factory=list)


@dataclass
class CallModelInput:
    messages: list[Message]
    tokens: streams.StreamRef[str]  # the ref that the Workflow passes


@activity.defn
async def call_model(input: CallModelInput) -> Reply:
    stream = activity.client().get_stream(input.tokens)  # writes as this attempt
    async with BufferedAppender(stream) as tokens:  # batches deltas
        for delta in input.messages[-1].split():
            tokens.append(delta)
    if len(input.messages) == 1:
        return Reply("", tool_calls=["search"])
    return Reply("the answer")


@activity.defn
async def run_tool(call: str) -> Message:
    return f"{call} result"


@dataclass
class AgentInput:
    session_id: str
    messages: list[Message]
    # The run before Continue-as-New sets the refs.
    turns: streams.StreamRef[Turn] | None = None
    tokens: streams.StreamRef[str] | None = None


@workflow.defn
class Agent:
    def __init__(self) -> None:
        self._turns: workflow.WorkflowStream[Turn] | None = None  # run creates it

    @workflow.query
    def stream(self) -> streams.StreamRef[Turn] | None:  # one way to hand it out
        return self._turns.ref if self._turns else None  # None before the create

    @workflow.run
    async def run(self, input: AgentInput) -> str:
        messages = input.messages
        # A Workflow creates only standalone Streams, under an id of its choice. A run after
        # Continue-as-New gets the earlier run's refs and creates nothing.
        if input.turns and input.tokens:
            self._turns = turns = workflow.get_stream(input.turns)
            tokens = workflow.get_stream(input.tokens)
        else:
            # Retention is a backend setting now, so both Streams use the backend's Retention.
            self._turns = turns = await workflow.create_stream(
                f"{input.session_id}-turns", payload_type=Turn
            )
            tokens = await workflow.create_stream(
                f"{input.session_id}-tokens", payload_type=str
            )
        while True:
            if workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(  # forward the refs, so the next run keeps the Streams
                    AgentInput(input.session_id, messages, turns.ref, tokens.ref)
                )
            await turns.append(Turn("model"))  # one Local Activity
            reply = await workflow.execute_activity(
                call_model,
                CallModelInput(messages, tokens.ref),
                start_to_close_timeout=MINUTE,
            )
            if not reply.tool_calls:
                await turns.append(Turn("done", reply.text))
                await turns.close()  # the done Turn carries the reply
                await tokens.close()
                return reply.text
            for call in reply.tool_calls:
                messages.append(
                    await workflow.execute_activity(
                        run_tool, call, start_to_close_timeout=MINUTE
                    )
                )


async def main(client: Client, task_queue: str) -> list[streams.Record[Turn]]:
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Agent],
        activities=[call_model, run_tool],
    ):
        session_id = f"session-{uuid.uuid4()}"
        handle = await client.start_workflow(
            Agent.run,
            AgentInput(session_id, ["find it"]),
            id=session_id,
            task_queue=task_queue,
        )
        await handle.result()
        # The Workflow hands out its turns ref, and the query's client reads the Stream.
        ref = await handle.query(Agent.stream)
        assert ref is not None
        # A deserialized ref decodes as JSON, so the id form takes the payload type.
        stream = client.get_stream(ref.id, payload_type=Turn, backend=ref.backend)
        return [r async for r in stream.read() if isinstance(r, streams.Record)]
