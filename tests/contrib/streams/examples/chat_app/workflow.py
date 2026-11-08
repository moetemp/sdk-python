"""The chat session Workflow, its agent loop and its sub-agent."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import streams, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError

from .shared import (
    MAX_ROUNDS,
    ROUNDS_PER_RUN,
    Message,
    ModelInput,
    ResearchInput,
    SessionInput,
    SessionStreams,
    SubAgent,
    Token,
    ToolCall,
    ToolInput,
    ToolResult,
    Turn,
    UserMessage,
    Waiting,
    tokens_id,
    turns_id,
)

with workflow.unsafe.imports_passed_through():
    from .activities import ModelActivities
    from .tools import TOOL_SPECS, ToolActivities, describe

# The pause before a retry keeps the withdrawn tokens on screen for a moment.
MODEL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1), backoff_coefficient=1.0, maximum_attempts=3
)


def notice(err: ActivityError | ChildWorkflowError) -> str:
    cause = err.cause
    return cause.message if isinstance(cause, ApplicationError) else str(cause)


class Agent:
    """The model rounds of an answer, which a chat session and a sub-agent share.

    Each round streams its tokens. The turns Stream gets a Turn for each round, each tool call
    and each tool result.
    """

    def __init__(
        self,
        turns: workflow.WorkflowStream[Turn],
        tokens: workflow.WorkflowStream[Token],
        tools: list[str],
        history: list[Message],
    ) -> None:
        self.turns = turns
        self.tokens = tokens
        self.tools = tools
        self.history = history
        self.rounds = 0
        self.number = 0
        self.answering = False
        # Messages that came during an answer, which join its next round.
        self.added: list[UserMessage] = []

    async def answer(self, number: int, fail_first_attempt: bool = False) -> str:
        self.number, self.answering = number, True
        for round in range(1, MAX_ROUNDS + 1):
            self.rounds += 1
            self.history += [Message("user", m.text) for m in self.added]
            self.added.clear()
            try:
                reply = await workflow.execute_activity_method(
                    ModelActivities.call_model,
                    ModelInput(
                        self.history,
                        number,
                        round,
                        self.tokens.ref,
                        self.tools,
                        fail_first_attempt and round == 1,
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                    # An attempt whose Worker stops misses its heartbeats, so its retry starts in
                    # seconds and withdraws its tokens.
                    heartbeat_timeout=timedelta(seconds=10),
                    retry_policy=MODEL_RETRY,
                )
            except ActivityError as err:
                return await self._stop(Turn("error", notice(err), number, round))
            self.history.append(
                Message("assistant", reply.text, reply.tool_calls, raw=reply.raw)
            )
            # A message that came during the round gets another round.
            final = not reply.tool_calls and not self.added
            if final:
                # A message that comes from here on starts a new turn.
                self.answering = False
            await self.turns.append(
                Turn("assistant", reply.text, number, round, final=final)
            )
            if final:
                return reply.text
            if reply.tool_calls:
                await self.turns.append(
                    *(
                        Turn(
                            "tool_call",
                            describe(c.name, c.args),
                            number,
                            round,
                            tool=c.name,
                            call_id=c.id,
                        )
                        for c in reply.tool_calls
                    )
                )
                # The round's tools run at the same time, and each one writes its own Turns.
                results = await asyncio.gather(
                    *(
                        self._call(c, number, round, i)
                        for i, c in enumerate(reply.tool_calls)
                    )
                )
                self.history.append(Message("tool", results=list(results)))
        text = f"The agent stopped after {MAX_ROUNDS} rounds"
        return await self._stop(Turn("error", text, number, MAX_ROUNDS))

    async def _stop(self, turn: Turn) -> str:
        self.answering = False
        # The next turn's model still sees a message that came during this one.
        self.history += [Message("user", m.text) for m in self.added]
        self.added.clear()
        await self.turns.append(turn)
        return ""

    async def _call(
        self, call: ToolCall, number: int, round: int, index: int
    ) -> ToolResult:
        try:
            if call.name == "research":
                return await workflow.execute_child_workflow(
                    Researcher.run,
                    ResearchInput(
                        str(call.args.get("topic", "")),
                        call,
                        number,
                        round,
                        self.turns.ref,
                    ),
                    id=f"{workflow.info().workflow_id}-agent-{number}-{round}-{index}",
                )
            return await workflow.execute_activity_method(
                ToolActivities.run_tool,
                ToolInput(call, number, round, self.turns.ref),
                start_to_close_timeout=timedelta(minutes=1),
            )
        except (ActivityError, ChildWorkflowError) as err:
            # The tool didn't write its result, so the Workflow writes the failure.
            result = ToolResult(call.id, call.name, f"error: {notice(err)}", error=True)
            await self.turns.append(
                Turn(
                    "tool_result",
                    result.text,
                    number,
                    round,
                    tool=call.name,
                    call_id=call.id,
                )
            )
            return result


@workflow.defn
class ChatSession:
    @workflow.init
    def __init__(self, input: SessionInput) -> None:
        self._waiting = list(input.waiting)
        self._number = input.number
        self._ended = False
        if input.streams is None:
            # A chat id can come back, such as in a restarted session, so the run gets the
            # chat's Streams if they exist.
            turns = workflow.create_stream_nowait(
                turns_id(input.session_id),
                payload_type=Turn,
                id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
            )
            # Retention is a backend setting now, so the tokens Stream uses the backend's.
            tokens = workflow.create_stream_nowait(
                tokens_id(input.session_id),
                payload_type=Token,
                id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
            )
        else:
            turns = workflow.get_stream(input.streams.turns)
            tokens = workflow.get_stream(input.streams.tokens)
        self._turns = turns
        self._agent = Agent(turns, tokens, list(TOOL_SPECS), list(input.history))
        # The init can't await, so it publishes with the _nowait call.
        turns.append_nowait(
            Turn("system", f"Run {input.run} of the session started", self._number)
        )

    @workflow.query
    def streams(self) -> SessionStreams:
        return SessionStreams(self._turns.ref, self._agent.tokens.ref)

    @workflow.update
    async def send(self, message: UserMessage) -> str:
        self._number += 1
        number = self._number
        # The awaited append gives the Record's offset, which the server hands to the browser as
        # its opaque string form.
        offset = await self._turns.append(Turn("user", message.text, number))
        self._waiting.append(Waiting(number, message))
        return str(offset)

    @send.validator
    def check_send(self, message: UserMessage) -> None:
        if not message.text.strip():
            raise ValueError("The message is empty")
        if self._ended:
            raise ValueError("The chat has ended")

    @workflow.signal
    def add_message(self, message: UserMessage) -> None:
        """A message that the user sends while the agent works."""
        if self._ended or not message.text.strip():
            return
        if self._agent.answering:
            # It joins the next round of the answer in progress.
            self._agent.added.append(message)
            number = self._agent.number
        else:
            self._number += 1
            number = self._number
            self._waiting.append(Waiting(number, message))
        # A signal handler can't await, so the user's Turn goes out with append_nowait.
        self._turns.append_nowait(Turn("user", message.text, number))

    @workflow.signal
    def end(self) -> None:
        self._ended = True

    @workflow.run
    async def run(self, input: SessionInput) -> None:
        agent = self._agent
        while True:
            # A send that started before the end still adds its message, so the end waits for it.
            await workflow.wait_condition(
                lambda: (
                    bool(self._waiting)
                    or (self._ended and workflow.all_handlers_finished())
                )
            )
            if not self._waiting:
                break
            waiting = self._waiting.pop(0)
            agent.history.append(Message("user", waiting.message.text))
            await agent.answer(waiting.number, waiting.message.fail_first_attempt)
            if self._ended or (
                agent.rounds < ROUNDS_PER_RUN
                and not workflow.info().is_continue_as_new_suggested()
            ):
                continue
            await workflow.wait_condition(workflow.all_handlers_finished)
            # The next run wouldn't know of an end that came during the wait.
            if not self._ended:
                # Forward the refs, so the next run keeps the Streams and creates nothing.
                workflow.continue_as_new(
                    SessionInput(
                        session_id=input.session_id,
                        history=agent.history,
                        streams=self.streams(),
                        run=input.run + 1,
                        number=self._number,
                        waiting=self._waiting,
                    )
                )
        await self._turns.append(Turn("system", "The chat ended", self._number))
        # A run's end doesn't close a Stream, so the session closes both.
        await self._turns.close()
        await agent.tokens.close()


@workflow.defn
class Researcher:
    """A sub-agent, which answers one research question in Streams of its own."""

    @workflow.run
    async def run(self, input: ResearchInput) -> ToolResult:
        info = workflow.info()
        # The ids include this run's id, which the parent can't know. So the sub-agent creates
        # its Streams itself, and hands their refs to the parent in a Turn.
        own_id = f"{info.workflow_id}-{info.run_id[:8]}"
        turns = await workflow.create_stream(turns_id(own_id), payload_type=Turn)
        tokens = await workflow.create_stream(tokens_id(own_id), payload_type=Token)
        call = input.call
        parent = workflow.get_stream(input.parent_turns)
        await parent.append(
            Turn(
                "agent",
                f"A sub-agent researches {input.topic}",
                input.number,
                input.round,
                tool=call.name,
                call_id=call.id,
                agent=SubAgent(own_id, turns.ref, tokens.ref),
            )
        )
        # The block end closes both Streams, so a reader of the sub-agent sees it end.
        async with turns, tokens:
            prompt = f"Research {input.topic}"
            await turns.append(Turn("user", prompt, 1))
            agent = Agent(turns, tokens, ["search_docs"], [Message("user", prompt)])
            summary = await agent.answer(1)
        result = ToolResult(
            call.id, call.name, summary or "The sub-agent found nothing."
        )
        await parent.append(
            Turn(
                "tool_result",
                result.text,
                input.number,
                input.round,
                tool=call.name,
                call_id=call.id,
            )
        )
        return result
