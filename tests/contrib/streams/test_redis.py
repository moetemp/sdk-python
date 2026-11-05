import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import activity, streams, workflow
from temporalio.client import Client, Stream
from temporalio.streams.system import models
from temporalio.worker import (
    UnsandboxedWorkflowRunner,
)
from tests.contrib.streams._support import stream_status
from tests.helpers import new_worker


async def read_all(stream: Stream[Any]) -> list[Any]:
    return [item async for item in stream.subscribe(create_wait=timedelta(seconds=20))]


def records(items: list[Any]) -> list[streams.Record[Any]]:
    return [item for item in items if isinstance(item, streams.Record)]


attempts: dict[str, int] = {}


@workflow.defn
class PublishThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream[str](
            f"{workflow.info().workflow_id}-tokens"
        )
        await stream.publish("before")
        await workflow.sleep(timedelta(milliseconds=10))
        await stream.publish("after")
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")
        await stream.close()


async def test_failed_workflow_task_is_withdrawn(stream_client: Client):
    key = str(uuid.uuid4())
    # The test's counter must keep its value across the task's retry, so the Workflow has no
    # sandbox.
    async with new_worker(
        stream_client,
        PublishThenFailTask,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            PublishThenFailTask.run,
            key,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        stream_id = f"{handle.id}-tokens"
        items = await read_all(stream_client.get_stream[str](stream_id))
        assert (
            await stream_status(stream_client, stream_id)
            == models.StreamStatus.StreamStatusClosed
        )
    # The retried task writes "after" again, and withdraws the copy of the failed task.
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [record.data for record in written] == ["before", "after", "after"]
    withdrawn = [r for r in written if any(i.withdraws(r) for i in superseded)]
    assert withdrawn == [written[1]]


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.StreamRef[str]


@activity.defn
async def local_tokens(input: CallModelInput) -> None:
    await activity.client().to_stream(input.stream).publish(*input.prompt.split())


@workflow.defn
class LocalThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream[str](
            f"{workflow.info().workflow_id}-tokens"
        )
        first = attempts.setdefault(key, 0) == 0
        await workflow.execute_local_activity(
            local_tokens,
            CallModelInput("0 1" if first else "2 3", stream.ref),
            start_to_close_timeout=timedelta(seconds=30),
        )
        if first:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")
        await stream.close()


def _task_of(producer: streams.Producer | None) -> tuple[int, int]:
    assert producer is not None and producer.epoch is not None
    kind = producer.epoch.kind
    assert isinstance(kind, streams.ProducerEpochKindLocalActivity)
    task = kind.value.task
    assert task is not None
    return (task.attempt or 0, task.started_event_id or 0)


async def test_rerun_local_activity_withdraws_its_failed_task(stream_client: Client):
    async with new_worker(
        stream_client,
        LocalThenFailTask,
        activities=[local_tokens],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            LocalThenFailTask.run,
            str(uuid.uuid4()),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await read_all(stream_client.get_stream[str](f"{handle.id}-tokens"))
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    visible = [r for r in written if not any(i.withdraws(r) for i in superseded)]
    assert [r.data for r in visible] == ["2", "3"]
    # The rerun keeps the Producer id, and its epoch has the retry task, which orders it later.
    first, second = written[0].producer, visible[0].producer
    assert first is not None and second is not None and first.id == second.id
    assert _task_of(first) < _task_of(second)


@workflow.defn
class LocalOnlyInFailedTask:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream[str](
            f"{workflow.info().workflow_id}-tokens"
        )
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            await workflow.execute_local_activity(
                local_tokens,
                CallModelInput("0 1", stream.ref),
                start_to_close_timeout=timedelta(seconds=30),
            )
            raise RuntimeError("fail this Workflow Task once")
        await stream.publish("other")
        await stream.close()


async def test_later_write_withdraws_a_skipped_local_activity(stream_client: Client):
    # The retry task skips the Local Activity, so only its own write can withdraw "0 1". The
    # write also uses the skipped call's key.
    async with new_worker(
        stream_client,
        LocalOnlyInFailedTask,
        activities=[local_tokens],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            LocalOnlyInFailedTask.run,
            str(uuid.uuid4()),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        stream_id = f"{handle.id}-tokens"
        items = await read_all(stream_client.get_stream[str](stream_id))
        assert (
            await stream_status(stream_client, stream_id)
            == models.StreamStatus.StreamStatusClosed
        )
    written = records(items)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    visible = [r for r in written if not any(s.withdraws(r) for s in superseded)]
    assert [r.data for r in visible] == ["other"]
