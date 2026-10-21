import uuid
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from tests.helpers import new_worker


@activity.defn
async def task_of_dispatch() -> list[int] | None:
    task = activity.info().workflow_task
    if task is None:
        return None
    assert task.started_time is not None
    return [task.started_event_id, task.attempt]


@workflow.defn
class DispatchWorkflow:
    @workflow.run
    async def run(self) -> list[list[int] | None]:
        local = await workflow.execute_local_activity(
            task_of_dispatch, start_to_close_timeout=timedelta(seconds=10)
        )
        await workflow.sleep(timedelta(milliseconds=10))
        later = await workflow.execute_local_activity(
            task_of_dispatch, start_to_close_timeout=timedelta(seconds=10)
        )
        remote = await workflow.execute_activity(
            task_of_dispatch, start_to_close_timeout=timedelta(seconds=10)
        )
        return [local, later, remote]


async def test_local_activity_gets_its_dispatching_workflow_task(client: Client):
    async with new_worker(
        client, DispatchWorkflow, activities=[task_of_dispatch]
    ) as worker:
        local, later, remote = await client.execute_workflow(
            DispatchWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert local is not None and later is not None
    assert local[0] > 0 and local[1] == 1
    # The second call comes from a later task, after the timer.
    assert later[0] > local[0]
    assert remote is None
