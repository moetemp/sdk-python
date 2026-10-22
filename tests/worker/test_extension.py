import uuid

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.runtime import Runtime, TelemetryConfig
from temporalio.service import RPCStatusCode
from temporalio.worker import ExtensionError, WorkerExtension
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
    worker_extension = load_test_extension()
    async with new_worker(client, SimpleWorkflow, extension=worker_extension) as worker:
        result = await client.execute_workflow(
            SimpleWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert result == "done"
    before, after, accepted = map(int, extension_counts(worker_extension))
    assert before >= 1 and after == before and accepted == before


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


def extension_counts(extension: WorkerExtension) -> list[str]:
    return extension.call("Counts", b"").decode().split(",")


async def test_extension_calls():
    extension = load_test_extension()
    assert extension.call("Echo", b"abc") == b"abc"
    assert await extension.call_async("Echo", b"xyz") == b"xyz"
    assert extension_counts(extension) == ["0", "0", "0"]


async def test_extension_call_errors():
    extension = load_test_extension()
    with pytest.raises(ExtensionError) as sync_err:
        extension.call("Fail", b"details")
    assert sync_err.value.code == RPCStatusCode.NOT_FOUND
    assert sync_err.value.message == "test failure"
    assert sync_err.value.details == b"details"
    with pytest.raises(ExtensionError) as async_err:
        await extension.call_async("Fail", b"")
    assert async_err.value.code == RPCStatusCode.NOT_FOUND
    assert async_err.value.details == b""
    with pytest.raises(ExtensionError) as unknown_err:
        await extension.call_async("Missing", b"")
    assert unknown_err.value.code == RPCStatusCode.UNIMPLEMENTED
    assert "Missing" in unknown_err.value.message


async def test_combined_extension_stops_at_the_failing_part(client: Client):
    # Core takes one extension, so the parts run in order, and a failure skips the rest.
    first = load_test_extension()
    failing = load_test_extension(b"fail_before_complete")
    last = load_test_extension()
    combined = WorkerExtension.combine(first, failing, last)
    async with new_worker(client, SimpleWorkflow, extension=combined) as worker:
        handle = await client.start_workflow(
            SimpleWorkflow.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await assert_task_fail_eventually(
            handle, message_contains="temporalio-test: test extension refused the task"
        )
    assert int(extension_counts(first)[0]) >= 1
    assert int(extension_counts(last)[0]) == 0
