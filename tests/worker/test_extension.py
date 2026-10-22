import uuid

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.runtime import Runtime, TelemetryConfig
from temporalio.service import RPCStatusCode
from temporalio.worker import ExtensionError, WorkerExtension
from tests.helpers import new_worker

# Debug bridge builds link this extension for the SDK's tests.
TEST_EXTENSION = "temporalio-test"


def load_test_extension() -> WorkerExtension:
    try:
        return WorkerExtension(TEST_EXTENSION)
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


async def test_extension_must_use_client_runtime(client: Client):
    load_test_extension()
    other = Runtime(telemetry=TelemetryConfig())
    extension = WorkerExtension(TEST_EXTENSION, runtime=other)
    with pytest.raises(ValueError, match="same runtime as the client"):
        new_worker(client, SimpleWorkflow, extension=extension)


async def test_extension_calls():
    extension = load_test_extension()
    assert extension.call("Echo", b"abc") == b"abc"
    assert await extension.call_async("Echo", b"xyz") == b"xyz"


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
