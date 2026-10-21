import asyncio
import uuid
from datetime import timedelta

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.plugin import SimplePlugin
from temporalio.worker import (
    Replayer,
    UnsandboxedWorkflowRunner,
    WorkflowActivationCompleteInput,
    WorkflowRunner,
)
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
from tests.helpers import assert_task_fail_eventually, new_worker


class RecordingPlugin(SimplePlugin):
    def __init__(self, fail: BaseException | None = None) -> None:
        super().__init__("test-activation-complete")
        self.inputs: list[WorkflowActivationCompleteInput] = []
        self.fail = fail

    async def on_workflow_activation_complete(
        self, input: WorkflowActivationCompleteInput
    ) -> None:
        self.inputs.append(input)
        if self.fail:
            raise self.fail

    def items(self) -> list[str]:
        return [item for input in self.inputs for item in input.items]


@workflow.defn
class SendingWorkflow:
    @workflow.run
    async def run(self) -> str:
        workflow.send_to_plugins("first")
        workflow.send_to_plugins("first-replayed", during_replay=True)
        await workflow.sleep(timedelta(milliseconds=10))
        workflow.send_to_plugins("second")
        return "done"


@pytest.mark.parametrize(
    "runner", [SandboxedWorkflowRunner(), UnsandboxedWorkflowRunner()]
)
async def test_plugin_gets_items_after_each_activation(
    client: Client, runner: WorkflowRunner
):
    plugin = RecordingPlugin()
    workflow_id = f"wf-{uuid.uuid4()}"
    async with new_worker(
        client, SendingWorkflow, plugins=[plugin], workflow_runner=runner
    ) as worker:
        handle = await client.start_workflow(
            SendingWorkflow.run, id=workflow_id, task_queue=worker.task_queue
        )
        assert await handle.result() == "done"
    assert plugin.items() == ["first", "first-replayed", "second"]
    assert all(input.run_id == handle.result_run_id for input in plugin.inputs)
    assert all(input.info.workflow_id == workflow_id for input in plugin.inputs)
    assert not any(input.is_replaying for input in plugin.inputs)
    assert plugin.inputs[0].data_converter is not None


async def test_plugin_gets_only_replay_items_on_cache_miss(client: Client):
    plugin = RecordingPlugin()
    async with new_worker(
        client, SendingWorkflow, plugins=[plugin], max_cached_workflows=0
    ) as worker:
        handle = await client.start_workflow(
            SendingWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "done"
    # The second Workflow Task replays the first, so only the value marked for replay comes
    # again.
    assert plugin.items() == ["first", "first-replayed", "first-replayed", "second"]
    assert any(input.is_replaying for input in plugin.inputs)


@pytest.mark.parametrize(
    "error", [RuntimeError("plugin refused"), asyncio.CancelledError("plugin refused")]
)
async def test_plugin_failure_fails_workflow_task(client: Client, error: BaseException):
    plugin = RecordingPlugin(fail=error)
    async with new_worker(client, SendingWorkflow, plugins=[plugin]) as worker:
        handle = await client.start_workflow(
            SendingWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await assert_task_fail_eventually(
            handle, message_contains="Failed plugin activation hook: plugin refused"
        )


async def test_replayer_does_not_call_plugin(client: Client):
    async with new_worker(client, SendingWorkflow) as worker:
        handle = await client.start_workflow(
            SendingWorkflow.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "done"
    plugin = RecordingPlugin()
    await Replayer(workflows=[SendingWorkflow], plugins=[plugin]).replay_workflow(
        await handle.fetch_history()
    )
    assert plugin.inputs == []
