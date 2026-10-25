import uuid
from datetime import timedelta
from typing import Any

from temporalio import activity, common, workflow
from temporalio.client import Client
from temporalio.worker import UnsandboxedWorkflowRunner
from tests.helpers import assert_task_fail_eventually, new_worker


@common.defn(name="events")
class Events:
    tokens = common.topic(str)


def data(items: list[Any]) -> list[Any]:
    return [item.data for item in items if isinstance(item, common.Record)]


def closes(items: list[Any]) -> list[common.Closed]:
    return [item for item in items if isinstance(item, common.Closed)]


failed_once: set[str] = set()


@activity.defn
async def noop() -> None:
    pass


@workflow.defn
class ClosesThenFailsItsTask:
    def __init__(self) -> None:
        self._go = False

    @workflow.run
    async def run(self, key: str) -> str:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("a")
        # The create and the publish commit with this task.
        await workflow.sleep(timedelta(milliseconds=10))
        if key not in failed_once:
            failed_once.add(key)
            stream.close_nowait(result="closed")
            # A Local Activity in the same task runs before the task fails, and the close
            # must not.
            await workflow.execute_local_activity(
                noop, start_to_close_timeout=timedelta(seconds=10)
            )
            raise RuntimeError("fail the Workflow Task of the close")
        await workflow.wait_condition(lambda: self._go)
        await stream.close(result="closed")
        return "done"

    @workflow.signal
    def go(self) -> None:
        self._go = True


async def test_a_close_waits_for_its_workflow_task_to_commit(stream_client: Client):
    # A close runs only once its Workflow Task commits, so a task that fails after it leaves
    # the Stream open, and the retry closes it once.
    key = str(uuid.uuid4())
    # The test's set must survive the retry of the task, so no sandbox.
    async with new_worker(
        stream_client,
        ClosesThenFailsItsTask,
        activities=[noop],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            ClosesThenFailsItsTask.run,
            key,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await assert_task_fail_eventually(handle, message_contains="of the close")
        stream = handle.stream(Events)
        before = [item async for item in stream.subscribe(wait_new_records=False)]
        assert data(before) == ["a"] and not closes(before)
        await handle.signal(ClosesThenFailsItsTask.go)
        assert await handle.result() == "done"
    items = [item async for item in stream.subscribe()]
    assert data(items) == ["a"]
    [closed] = closes(items)
    assert closed.result == "closed"
