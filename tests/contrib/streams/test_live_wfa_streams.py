"""Streams that a Workflow Activity owns, against a server that serves them.

An agent Workflow starts a tool call with ``start_activity_with_stream``. The
tool writes its own stream, every attempt as a new producer attempt, and the
agent's Worker closes the stream when the agent sees the tool end. When that
close never comes, the server's notifier closes the stream after its graces.
The stock dev server has no stream notifier, so every case here skips
there. ``scripts/streams_live_lane.sh`` runs them on a server that has it, on
memory, standalone Redis and a Redis Cluster, each with a ``StreamDriver``.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import ResetWorkflowExecutionRequest
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    StreamCursorError,
    StreamRef,
    activity_handle,
    get_stream_handle,
    start_activity_with_stream,
    topic,
)
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    TimeoutError,
)
from temporalio.worker import Worker
from tests.contrib.streams import test_live_activity_owner as _saa
from tests.contrib.streams._support import new_workflow_id
from tests.contrib.streams.test_live_activity_owner import (
    OWNER_CHECK,
    Consume,
    _close_result,
    _read,
)
from tests.helpers import new_worker

# The same stores and driver as the standalone Activity cases.
driven = _saa.driven

TOKENS = topic("tokens", str)
TOOL_ID = "tool-1"
_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=2)


@workflow.defn
class Agent:
    """Runs one tool call that streams, and returns its result."""

    @workflow.run
    async def run(
        self, tool: str, timeout_ms: int, attempts: int, tool_queue: str = ""
    ) -> Any:
        handle, _ = start_activity_with_stream(
            tool,
            activity_id=TOOL_ID,
            task_queue=tool_queue or None,
            start_to_close_timeout=timedelta(milliseconds=timeout_ms),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), maximum_attempts=attempts
            ),
            topic=TOKENS,
        )
        return await handle


def _tool_ref(agent: Any) -> StreamRef:
    return StreamRef.for_workflow_activity(
        agent.id, run_id=agent.result_run_id, activity_id=TOOL_ID, topic=TOKENS
    )


async def _start(
    client: Client, task_queue: str, tool: str, *, timeout_ms: int = 10_000
) -> tuple[Any, Any]:
    """Start the agent and a Workflow reader of its tool's stream."""
    agent = await client.start_workflow(
        Agent.run,
        args=[tool, timeout_ms, 2],
        id=new_workflow_id(),
        task_queue=task_queue,
    )
    reader = await client.start_workflow(
        Consume.run, _tool_ref(agent), id=new_workflow_id(), task_queue=task_queue
    )
    return agent, reader


# Steps 1 to 3: a retry supersedes, and the close at resolution ends both
# readers with a null result.


@activity.defn
async def run_tool() -> str:
    out = activity_handle().producer(topic=TOKENS)
    if activity.info().attempt == 1:
        await out.append("a1", "a2", "a3")
        raise ApplicationError("retry me")
    await out.append("b1", "b2")
    return "ok"


async def test_live_both_readers_follow_a_tool_call_across_its_retry(driven: Client):
    async with new_worker(driven, Agent, Consume, activities=[run_tool]) as worker:
        agent, reader = await _start(driven, worker.task_queue, "run_tool")
        ref = _tool_ref(agent)
        seen, withdrawn = await asyncio.wait_for(_read(driven, ref), 60)
        assert await agent.result() == "ok"
        expected = ["a1", "a2", "a3", "SUPERSEDED", "b1", "b2"]
        assert seen == expected
        assert withdrawn == ["a1", "a2", "a3"]
        assert await _close_result(driven, ref) is None
        assert await asyncio.wait_for(reader.result(), 60) == expected


# Step 4: a tool that fails for good.


@activity.defn
async def fails_hard() -> None:
    await activity_handle().producer(topic=TOKENS).append("x1")
    raise ApplicationError("boom", type="Boom", non_retryable=True)


def _application_cause(error: BaseException) -> ApplicationError:
    while not isinstance(error, ApplicationError):
        assert error.__cause__ is not None, error
        error = error.__cause__
    return error


async def test_live_a_tool_that_fails_closes_its_stream_with_the_failure(
    driven: Client,
):
    async with new_worker(driven, Agent, Consume, activities=[fails_hard]) as worker:
        agent, reader = await _start(driven, worker.task_queue, "fails_hard")
        ref = _tool_ref(agent)
        with pytest.raises((ActivityError, ApplicationError)) as client_raised:
            await asyncio.wait_for(_read(driven, ref), 60)
        assert _application_cause(client_raised.value).type == "Boom"
        with pytest.raises(WorkflowFailureError):
            await agent.result()
        with pytest.raises(WorkflowFailureError) as raised:
            await asyncio.wait_for(reader.result(), 60)
        assert _application_cause(raised.value).type == "Boom"


# Step 5: a start-to-close timeout.


@activity.defn
async def hangs() -> None:
    await activity_handle().producer(topic=TOKENS).append("h1")
    await asyncio.sleep(30)


async def test_live_a_tool_that_times_out_closes_its_stream_with_the_timeout(
    driven: Client,
):
    async with new_worker(driven, Agent, Consume, activities=[hangs]) as worker:
        agent = await driven.start_workflow(
            Agent.run,
            args=["hangs", 2_000, 1],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        reader = await driven.start_workflow(
            Consume.run,
            _tool_ref(agent),
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowFailureError):
            await asyncio.wait_for(agent.result(), 60)
        with pytest.raises(WorkflowFailureError) as raised:
            await asyncio.wait_for(reader.result(), 60)
        error: BaseException | None = raised.value
        while error is not None and not isinstance(error, TimeoutError):
            error = error.__cause__
        assert isinstance(error, TimeoutError)


# Step 6: Continue-as-New gives the next run's Activity "1" a new stream.


@activity.defn
async def writes_its_run() -> str:
    info = activity.info()
    await activity_handle().producer(topic=TOKENS).append(info.workflow_run_id or "")
    return info.workflow_run_id or ""


@workflow.defn
class ChainedAgent:
    @workflow.run
    async def run(self, runs_left: int) -> list[str]:
        handle, ref = start_activity_with_stream(
            writes_its_run, start_to_close_timeout=timedelta(seconds=10), topic=TOKENS
        )
        await handle
        assert ref.activity_id == "1"
        if runs_left > 1:
            workflow.continue_as_new(runs_left - 1)
        return [workflow.info().run_id]


async def test_live_each_run_of_a_chain_gets_its_own_tool_stream(driven: Client):
    async with new_worker(driven, ChainedAgent, activities=[writes_its_run]) as worker:
        agent = await driven.start_workflow(
            ChainedAgent.run, 2, id=new_workflow_id(), task_queue=worker.task_queue
        )
        [last_run] = await asyncio.wait_for(agent.result(), 60)
        first_run = agent.first_execution_run_id or ""
        assert first_run != last_run
        refs = [
            StreamRef.for_workflow_activity(
                agent.id, run_id=run, activity_id="1", topic=TOKENS
            )
            for run in (first_run, last_run)
        ]
        first = [r async for r in get_stream_handle(driven, refs[0]).read()]
        last = [r async for r in get_stream_handle(driven, refs[1]).read()]
        assert [r.value for r in first] == [first_run]
        assert [r.value for r in last] == [last_run]
        with pytest.raises(StreamCursorError):
            async for _ in get_stream_handle(driven, refs[1]).read(
                after=first[-1].cursor
            ):
                pass


# Step 7: a Workflow reset reruns the tool in a new run, on a new stream.

_release: dict[str, asyncio.Event] = {}
_started: dict[str, asyncio.Event] = {}


@activity.defn
async def waits_in_the_first_run() -> str:
    info = activity.info()
    workflow_id = info.workflow_id or ""
    await activity_handle().producer(topic=TOKENS).append(info.workflow_run_id or "")
    if info.workflow_run_id == _first_runs.get(workflow_id):
        _started[workflow_id].set()
        await _release[workflow_id].wait()
    return "ok"


_first_runs: dict[str, str] = {}


async def test_live_a_reset_run_writes_a_new_tool_stream(driven: Client):
    workflow_id = new_workflow_id()
    _release[workflow_id] = asyncio.Event()
    _started[workflow_id] = asyncio.Event()
    async with new_worker(driven, Agent, activities=[waits_in_the_first_run]) as worker:
        agent = await driven.start_workflow(
            Agent.run,
            args=["waits_in_the_first_run", 60_000, 1],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        _first_runs[workflow_id] = agent.result_run_id or ""
        await asyncio.wait_for(_started[workflow_id].wait(), 30)
        history = await agent.fetch_history()
        [first_task] = [
            e.event_id
            for e in history.events
            if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
        ][:1]
        reset = await driven.workflow_service.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=driven.namespace,
                workflow_execution=WorkflowExecution(
                    workflow_id=workflow_id, run_id=agent.result_run_id or ""
                ),
                reason="lane",
                workflow_task_finish_event_id=first_task - 1,
                request_id=str(uuid.uuid4()),
            )
        )
        new_run = reset.run_id
        assert (
            await asyncio.wait_for(
                driven.get_workflow_handle(workflow_id, run_id=new_run).result(), 60
            )
            == "ok"
        )
        old = StreamRef.for_workflow_activity(
            workflow_id,
            run_id=_first_runs[workflow_id],
            activity_id=TOOL_ID,
            topic=TOKENS,
        )
        new = StreamRef.for_workflow_activity(
            workflow_id, run_id=new_run, activity_id=TOOL_ID, topic=TOKENS
        )
        # The old run ended with the reset, so its read ends at once.
        old_seen = [r.value async for r in get_stream_handle(driven, old).read()]
        assert old_seen == [_first_runs[workflow_id]]
        new_seen = [r.value async for r in get_stream_handle(driven, new).read()]
        assert new_seen == [new_run]
        _release[workflow_id].set()


# Step 8: the agent's Worker stops before it sees the tool end, so the
# notifier closes the stream after its graces.

_tool_started: dict[str, asyncio.Event] = {}
_tool_go: dict[str, asyncio.Event] = {}


@activity.defn
async def outlives_its_agent() -> str:
    workflow_id = activity.info().workflow_id or ""
    await activity_handle().producer(topic=TOKENS).append("o1")
    _tool_started[workflow_id].set()
    await _tool_go[workflow_id].wait()
    return "ok"


async def test_live_a_tool_whose_agent_never_closes_ends_by_the_owner_check(
    driven: Client,
):
    workflow_id = new_workflow_id()
    _tool_started[workflow_id] = asyncio.Event()
    _tool_go[workflow_id] = asyncio.Event()
    agent_queue = f"agent-{uuid.uuid4()}"
    async with (
        new_worker(driven, Consume) as readers,
        new_worker(driven, activities=[outlives_its_agent]) as tools,
    ):
        agent_worker = Worker(driven, task_queue=agent_queue, workflows=[Agent])
        async with agent_worker:
            agent = await driven.start_workflow(
                Agent.run,
                args=["outlives_its_agent", 60_000, 1, tools.task_queue],
                id=workflow_id,
                task_queue=agent_queue,
            )
            await asyncio.wait_for(_tool_started[workflow_id].wait(), 30)
        # The tool runs on its own task queue, so it completes while the agent
        # has no Worker to see it.
        _tool_go[workflow_id].set()
        reader = await driven.start_workflow(
            Consume.run,
            _tool_ref(agent),
            id=new_workflow_id(),
            task_queue=readers.task_queue,
        )
        with pytest.raises(WorkflowFailureError) as raised:
            await asyncio.wait_for(reader.result(), 2 * OWNER_CHECK)
        assert "without closing the stream" in str(_application_cause(raised.value))
        await agent.terminate(reason="lane")
