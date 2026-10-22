import asyncio
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.exceptions import ApplicationError
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


async def read(handle: WorkflowHandle[Any, Any], name: str = "events") -> list[object]:
    stream = streams.for_handle(
        handle, name=name, wait_for_create=timedelta(seconds=20)
    )
    stream.topic("turns", type=Turn)
    stream.topic("tokens", type=str)
    return [item async for item in stream.subscribe()]


def data(items: Sequence[object]) -> list[object]:
    return [item.data for item in items if isinstance(item, streams.Record)]


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        stream = await stream_workflow.create_stream(Events)
        turns = stream.topic(Events.turns)
        await turns.publish(Turn("user", prompt), Turn("model"))
        await stream.topic("tokens").publish(*prompt.split())
        return prompt.upper()


async def test_create_publish_and_close_with_run(stream_client: Client):
    async with new_worker(stream_client, Agent) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "HI THERE"
        items = await read(handle)
    assert data(items) == [Turn("user", "hi there"), Turn("model"), "hi", "there"]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "HI THERE"


@workflow.defn
class Turns:
    @workflow.run
    async def run(self, fail_at: int) -> None:
        for n in range(2):
            try:
                async with await stream_workflow.create_stream(
                    Events, name=f"turn-{n}", close_with_run=False
                ) as turn:
                    await turn.topic(Events.tokens).publish(f"token-{n}")
                    if n == fail_at:
                        raise ApplicationError("turn failed")
            except ApplicationError:
                pass
            await workflow.sleep(timedelta(milliseconds=10))


async def test_async_with_closes_the_stream(stream_client: Client):
    async with new_worker(stream_client, Turns) as worker:
        handle = await stream_client.start_workflow(
            Turns.run, 1, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        first = await read(handle, "turn-0")
        second = await read(handle, "turn-1")
    assert data(first) == ["token-0"]
    assert isinstance(first[-1], streams.Closed) and first[-1].failure is None
    assert data(second) == ["token-1"]
    assert isinstance(second[-1], streams.Closed)
    assert str(second[-1].failure) == "turn failed"


@workflow.defn
class Continuing:
    @workflow.run
    async def run(self, stream: stream_workflow.WorkflowStream[Events] | None) -> str:
        if stream is None:
            stream = await stream_workflow.create_stream(Events)
            await stream.topic(Events.tokens).publish("run 1")
            workflow.continue_as_new(stream)
        assert stream.definition is Events
        assert stream == stream_workflow.stream_handle(Events)
        await stream.topic(Events.tokens).publish("run 2")
        return "done"


async def test_continue_as_new_passes_the_stream(stream_client: Client):
    async with new_worker(stream_client, Continuing) as worker:
        handle = await stream_client.start_workflow(
            Continuing.run, None, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "done"
        items = await read(handle)
    assert data(items) == ["run 1", "run 2"]
    closed = [item for item in items if isinstance(item, streams.Closed)]
    assert len(closed) == 1 and closed[0].result == "done"


@workflow.defn
class Mistakes:
    def __init__(self) -> None:
        self._stream: stream_workflow.WorkflowStream[Events] | None = None

    @workflow.run
    async def run(self) -> list[str]:
        errors = []
        stream = await stream_workflow.create_stream(Events, close_with_run=False)
        self._stream = stream
        try:
            stream.topic("unknown")
        except streams.StreamError as err:
            errors.append(err.reason)
        try:
            await stream_workflow.create_stream(Events, name="b", backend="elsewhere")
        except streams.StreamUnsupportedError as err:
            errors.append(err.reason)
        await stream.close(result="early")
        try:
            await stream.topic(Events.tokens).publish("late")
        except streams.StreamClosedError as err:
            errors.append(err.reason)
        try:
            await stream_workflow.create_stream(Events)
        except streams.StreamClosedError as err:
            errors.append(err.reason)
        return errors

    @workflow.query
    def publish_in_query(self) -> str:
        assert self._stream is not None
        try:
            self._stream.topic(Events.tokens)._stream._append("tokens", ("x",))
        except streams.ReadOnlyContextError as err:
            return type(err).__name__
        return "wrote"


async def test_replay_reproducible_errors_raise_in_workflow_code(
    stream_client: Client,
):
    async with new_worker(stream_client, Mistakes) as worker:
        handle = await stream_client.start_workflow(
            Mistakes.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == [
            "unknown_topic",
            "stream_unsupported",
            "stream_closed",
            "stream_closed",
        ]
        assert await handle.query(Mistakes.publish_in_query) == "ReadOnlyContextError"
        items = await read(handle)
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "early"


@workflow.defn
class Failing:
    @workflow.run
    async def run(self) -> None:
        stream = await stream_workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("before")
        raise ApplicationError("run failed", non_retryable=True)


async def test_close_with_run_takes_the_failure(stream_client: Client):
    async with new_worker(stream_client, Failing) as worker:
        handle = await stream_client.start_workflow(
            Failing.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        items = await read(handle)
    assert data(items) == ["before"]
    assert isinstance(items[-1], streams.Closed)
    assert str(items[-1].failure) == "run failed"


@workflow.defn
class LeavesOpen:
    @workflow.run
    async def run(self) -> None:
        stream = await stream_workflow.create_stream(
            Events, auto_close=timedelta(seconds=1), close_with_run=False
        )
        await stream.topic(Events.tokens).publish("only")


async def test_auto_close_closes_a_quiet_stream(stream_client: Client):
    async with new_worker(stream_client, LeavesOpen) as worker:
        handle = await stream_client.start_workflow(
            LeavesOpen.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        await asyncio.sleep(1.5)
        stream = streams.for_handle(handle, name="events")
        items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert data(items) == ["only"]
    assert isinstance(items[-1], streams.Closed)
    assert items[-1].result is None and items[-1].failure is None
