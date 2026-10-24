import uuid
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy, streams
from temporalio.exceptions import ApplicationError
from temporalio.worker import UnsandboxedWorkflowRunner
from tests.helpers import assert_task_fail_eventually, new_worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


def data(items: list[Any]) -> list[Any]:
    return [item.data for item in items if isinstance(item, streams.Record)]


def closes(items: list[Any]) -> list[streams.Closed]:
    return [item for item in items if isinstance(item, streams.Closed)]


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


@workflow.defn
class CronBlock:
    @workflow.run
    async def run(self) -> str:
        async with workflow.create_stream(Events) as stream:
            await stream.topic(Events.tokens).publish("turn")
        return "done"


async def test_a_cron_run_leaves_its_block_open(stream_client: Client):
    # The Stream spans the cron chain, so the end of each run's block leaves it open.
    async with new_worker(stream_client, CronBlock) as worker:
        handle = await stream_client.start_workflow(
            CronBlock.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            cron_schedule="@every 1s",
        )
        stream = handle.stream(Events)
        reader = stream.subscribe(create_wait=timedelta(seconds=20))
        first = await reader.__anext__()
        second = await reader.__anext__()
        await reader.aclose()
        # Read before the terminate, since a native server closes the Stream with it.
        items = [item async for item in stream.subscribe(wait_new_records=False)]
        await handle.terminate()
    assert [first.data, second.data] == ["turn", "turn"]  # type: ignore[union-attr]
    assert not closes(items)


@workflow.defn
class RetriesInBlock:
    @workflow.run
    async def run(self) -> str:
        attempt = workflow.info().attempt
        async with workflow.create_stream(Events) as stream:
            await stream.topic(Events.tokens).publish(f"run-{attempt}")
            if attempt == 1:
                raise ApplicationError("the first run fails")
        return "done"


async def test_a_retried_run_writes_on_and_its_block_closes(stream_client: Client):
    # The first run fails under a retry policy, so its block leaves the Stream open, and the
    # retry run writes on to it and closes it.
    async with new_worker(stream_client, RetriesInBlock) as worker:
        handle = await stream_client.start_workflow(
            RetriesInBlock.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            ),
        )
        assert await handle.result() == "done"
    items = [item async for item in handle.stream(Events).subscribe()]
    withdrawn = [item for item in items if isinstance(item, streams.Superseded)]
    visible = [
        item.data
        for item in items
        if isinstance(item, streams.Record)
        and not any(w.withdraws(item) for w in withdrawn)
    ]
    assert visible == ["run-2"]
    [closed] = closes(items)
    assert items[-1] is closed and closed.status == streams.CloseStatus.COMPLETED
