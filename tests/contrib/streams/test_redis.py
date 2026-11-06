import uuid
from datetime import timedelta
from typing import Any

from temporalio import streams, workflow
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
