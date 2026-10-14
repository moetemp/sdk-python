"""A Workflow closes its own stream from a Local Activity.

The writer's buffer lands first and its last flush writes ``FINISH``. Then
the close's Local Activity closes the store and the stream's notifier
through Core. On the stock test server there is no notifier, so these
cases check the store side. The Jetty lane checks the outcome on the
server.
"""

from __future__ import annotations

import asyncio

import pytest

from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporalio.contrib.streams import (
    RecordKind,
    close_workflow_stream,
    get_stream_handle,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
from tests.contrib.streams.test_workflow_writer import STEPS, redis_store
from tests.helpers import new_worker


@pytest.fixture(params=["memory", "redis"])
async def streams(request: pytest.FixtureRequest, client: Client) -> Client:
    store = MemoryStreams() if request.param == "memory" else redis_store()
    return await connect_with(client, store)


@workflow.defn
class PublishesThenCloses:
    """Publishes, closes the topic, then waits, so a read ends while it runs."""

    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self, how: str) -> None:
        if how != "no-writer":
            writer = workflow_writer(STEPS)
            writer.publish("one")
            writer.publish("two")
        if how == "error":
            await close_workflow_stream(
                STEPS, error=ApplicationError("gave up", type="GaveUp")
            )
        else:
            await close_workflow_stream(STEPS, result={"steps": 2})
        if how == "twice" or how == "no-writer":
            # A repeat, as a rerun after a failed Workflow Task makes, is done.
            await close_workflow_stream(STEPS, result={"steps": 2})
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


@pytest.mark.parametrize("how", ["result", "error", "twice", "no-writer"])
async def test_a_workflow_close_ends_reads_while_the_workflow_runs(
    streams: Client,
    how: str,
):
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesThenCloses) as worker:
        handle = await streams.start_workflow(
            PublishesThenCloses.run, how, id=workflow_id, task_queue=worker.task_queue
        )
        records: list = []
        try:
            async for record in get_stream_handle(streams, workflow_id).read(
                topic=STEPS
            ):
                records.append(record)
        except ApplicationError as error:
            # A close with an error ends the read with it, after the last record.
            assert how == "error" and error.type == "GaveUp"
        else:
            assert how != "error"
        values = [(r.kind, r.value) for r in records]
        if how == "no-writer":
            assert values == [(RecordKind.FINISH, None)]
        else:
            assert values == [
                (RecordKind.DATA, "one"),
                (RecordKind.DATA, "two"),
                (RecordKind.FINISH, None),
            ]
        # The read ended on the close, not on the run's end.
        assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
        await handle.signal(PublishesThenCloses.finish)
        await handle.result()
    await Replayer(
        workflows=[PublishesThenCloses], plugins=[MemoryStreams()]
    ).replay_workflow(await handle.fetch_history())


@workflow.defn
class ClosesWithBoth:
    @workflow.run
    async def run(self) -> None:
        await close_workflow_stream(STEPS, result=1, error=ApplicationError("no"))


async def test_a_workflow_close_takes_a_result_or_an_error_not_both(
    streams: Client,
):
    async with new_worker(
        streams, ClosesWithBoth, workflow_failure_exception_types=[ValueError]
    ) as worker:
        with pytest.raises(WorkflowFailureError) as raised:
            await asyncio.wait_for(
                streams.execute_workflow(
                    ClosesWithBoth.run,
                    id=new_workflow_id(),
                    task_queue=worker.task_queue,
                ),
                20,
            )
    assert "result or an error" in str(raised.value.cause)


@workflow.defn
class PublishesAfterClose:
    @workflow.run
    async def run(self) -> str:
        writer = workflow_writer(STEPS)
        writer.publish("one")
        await close_workflow_stream(STEPS)
        try:
            writer.publish("late")
        except ValueError as error:
            return str(error)
        return "published"


async def test_a_writer_refuses_a_publish_after_the_close(
    streams: Client,
):
    async with new_worker(streams, PublishesAfterClose) as worker:
        said = await streams.execute_workflow(
            PublishesAfterClose.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert "already finished" in said


# Workflow ids whose task already failed once, in this process.
_failed_once: set[str] = set()


@workflow.defn
class ClosesThenFailsItsTask:
    """Fails the task that ran the flushes and the close, once, so they all run again."""

    @workflow.run
    async def run(self) -> str:
        writer = workflow_writer(STEPS)
        writer.publish("one")
        writer.publish("two")
        await close_workflow_stream(STEPS, result="done")
        if workflow.info().workflow_id not in _failed_once:
            _failed_once.add(workflow.info().workflow_id)
            raise RuntimeError("fail this Workflow Task")
        return "closed"


async def test_a_rerun_after_the_close_finds_its_flushes_landed(
    streams: Client,
):
    workflow_id = new_workflow_id()
    async with new_worker(
        streams,
        ClosesThenFailsItsTask,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        result = await asyncio.wait_for(
            streams.execute_workflow(
                ClosesThenFailsItsTask.run,
                id=workflow_id,
                task_queue=worker.task_queue,
            ),
            30,
        )
    assert result == "closed"
    assert workflow_id in _failed_once
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    # The first run of the task wrote everything once. The rerun wrote nothing.
    assert [(r.kind, r.value) for r in records] == [
        (RecordKind.DATA, "one"),
        (RecordKind.DATA, "two"),
        (RecordKind.FINISH, None),
    ]


@workflow.defn
class PublishesAfterAnOutsideClose:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self.go)
        writer = workflow_writer(STEPS)
        writer.publish("late")
        try:
            await writer.flush()
        except BaseException as error:
            cause = getattr(error, "cause", None) or error
            return getattr(cause, "type", type(cause).__name__)
        return "landed"

    @workflow.signal
    def publish(self) -> None:
        self.go = True


async def test_a_flush_after_a_close_by_someone_else_fails(streams: Client):
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesAfterAnOutsideClose) as worker:
        handle = await streams.start_workflow(
            PublishesAfterAnOutsideClose.run,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        await get_stream_handle(streams, workflow_id).close(topic=STEPS)
        await handle.signal(PublishesAfterAnOutsideClose.publish)
        said = await asyncio.wait_for(handle.result(), 30)
    assert said == "StreamClosedError"
