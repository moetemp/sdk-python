import uuid

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.runtime import Runtime, TelemetryConfig
from temporalio.worker import WorkerExtension
from tests.helpers import assert_task_fail_eventually, new_worker

# Debug bridge builds link this extension for the SDK's tests.
TEST_EXTENSION = "temporalio-test"


def load_test_extension(config: bytes = b"") -> WorkerExtension:
    try:
        return WorkerExtension(TEST_EXTENSION, config)
    except ValueError:
        pytest.skip("Bridge build has no test extension")


@workflow.defn
class SimpleWorkflow:
    @workflow.run
    async def run(self) -> str:
        return "done"


def test_unknown_extension_name():
    with pytest.raises(ValueError, match="no worker extension named not-linked"):
        WorkerExtension("not-linked")


def test_extension_name():
    assert load_test_extension().name == TEST_EXTENSION


async def test_extension_lets_workflow_task_complete(client: Client):
    async with new_worker(
        client, SimpleWorkflow, extension=load_test_extension()
    ) as worker:
        result = await client.execute_workflow(
            SimpleWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert result == "done"


async def test_extension_failure_fails_workflow_task(client: Client):
    extension = load_test_extension(b"fail_before_complete")
    async with new_worker(client, SimpleWorkflow, extension=extension) as worker:
        handle = await client.start_workflow(
            SimpleWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await assert_task_fail_eventually(
            handle, message_contains="test extension refused the task"
        )


async def test_extension_must_use_client_runtime(client: Client):
    load_test_extension()
    other = Runtime(telemetry=TelemetryConfig())
    extension = WorkerExtension(TEST_EXTENSION, runtime=other)
    with pytest.raises(ValueError, match="same runtime as the client"):
        new_worker(client, SimpleWorkflow, extension=extension)
