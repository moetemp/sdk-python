from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

import temporalio.exceptions
from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import (
    Client,
    StreamBackend,
    StreamBackendCallOptions,
    WorkflowHandle,
)
from temporalio.common import RetryPolicy
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError
from temporalio.nexus.system.stream_service import (
    Producer,
    ProducerKindWorkflow,
    StreamRef,
    WorkflowProducer,
)
from temporalio.nexus.system.stream_service.workflow import create_stream, get_stream
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
    Replayer,
)
from tests.helpers import assert_task_fail_eventually, new_worker
from tests.helpers.stream_backend import MemoryStreamBackend
from tests.test_converter import SimpleCodec

# Workflow code writes these values, so the History shows only whether a Record body
# leaks into it.
_BODIES = ["first-record-body", "second-record-body"]


@workflow.defn
class AppendWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> StreamRef[str]:
        stream = await create_stream(id=stream_id, body_type=str)
        await stream.append(*_BODIES)
        return stream.ref


@workflow.defn
class UnsupportedCallsWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> list[str]:
        stream = await create_stream(id=stream_id, body_type=str)
        errors: list[str] = []
        for call in (stream.close, stream.read):
            try:
                await call()
            except ApplicationError as err:
                assert err.non_retryable
                errors.append(err.message)
        try:
            async with get_stream(stream.ref):
                pass
        except ApplicationError as err:
            errors.append(err.message)
        return errors


@workflow.defn
class AppendResultWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> str:
        stream = await create_stream(id=stream_id, body_type=str)
        try:
            await stream.append("a")
        except ApplicationError as err:
            return f"ApplicationError {err.type}: {err.message}"
        except temporalio.exceptions.TimeoutError as err:
            return f"TimeoutError {err.type and err.type.name}"
        return "appended"


@activity.defn
async def user_activity() -> None:
    pass


@workflow.defn
class AppendAndActivityWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> None:
        stream = await create_stream(id=stream_id, body_type=str)
        await stream.append("a")
        await workflow.execute_local_activity(
            user_activity, start_to_close_timeout=timedelta(seconds=10)
        )


class FailingStreamBackend(StreamBackend):
    @property
    def name(self) -> str:
        return "failing"

    async def call(self, operation: str, request: bytes) -> bytes:
        raise AssertionError(f"A replay called the backend with {operation}")


class FlakyAppendBackend(MemoryStreamBackend):
    def __init__(
        self,
        options: StreamBackendCallOptions,
        *,
        failures: int = 0,
        delay: timedelta = timedelta(),
    ) -> None:
        super().__init__()
        self._options = options
        self._failures = failures
        self._delay = delay
        self.appends = 0

    @property
    def call_options(self) -> StreamBackendCallOptions:
        return self._options

    async def call(self, operation: str, request: bytes) -> bytes:
        if operation == "AppendRecords":
            self.appends += 1
            await asyncio.sleep(self._delay.total_seconds())
            if self.appends <= self._failures:
                raise RuntimeError("append failed")
        return await super().call(operation, request)


class RecordingInterceptor(Interceptor):
    def __init__(self) -> None:
        self.activity_types: list[str] = []

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _RecordingActivityInbound(next, self.activity_types)


class _RecordingActivityInbound(ActivityInboundInterceptor):
    def __init__(
        self, next: ActivityInboundInterceptor, activity_types: list[str]
    ) -> None:
        super().__init__(next)
        self._activity_types = activity_types

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        self._activity_types.append(activity.info().activity_type)
        return await super().execute_activity(input)


def _with_backend(client: Client, backend: StreamBackend) -> Client:
    config = client.config()
    config["stream_backends"] = [backend]
    return Client(**config)


async def _run_append_workflow(
    client: Client,
) -> tuple[WorkflowHandle[AppendWorkflow, StreamRef[str]], StreamRef[str]]:
    async with new_worker(client, AppendWorkflow) as worker:
        handle = await client.start_workflow(
            AppendWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        return handle, await handle.result()


async def _append_result(client: Client, backend: StreamBackend) -> str:
    stream_client = _with_backend(client, backend)
    async with new_worker(stream_client, AppendResultWorkflow) as worker:
        return await stream_client.execute_workflow(
            AppendResultWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )


async def test_workflow_stream_uses_backend(client: Client):
    stream_client = _with_backend(client, MemoryStreamBackend())
    handle, ref = await _run_append_workflow(stream_client)

    stream = stream_client.get_stream(ref, body_type=str)
    records = [record async for record in stream.iter_read_items()]
    assert [record.body for record in records] == _BODIES
    run_id = handle.result_run_id
    assert run_id
    producer = Producer(
        id=f"wf:{run_id}",
        kind=ProducerKindWorkflow(WorkflowProducer(run_id=run_id)),
    )
    assert [record.producer for record in records] == [producer, producer]


async def test_workflow_stream_backend_uses_payload_codec(client: Client):
    backend = MemoryStreamBackend()
    config = client.config()
    config["data_converter"] = DataConverter(payload_codec=SimpleCodec())
    config["stream_backends"] = [backend]
    stream_client = Client(**config)
    _, ref = await _run_append_workflow(stream_client)

    stored = [record.body for record in backend.streams[ref.id].records]
    assert [body.metadata["simple-codec"] for body in stored] == [b"true", b"true"]
    stream = stream_client.get_stream(ref, body_type=str)
    assert [record.body async for record in stream.iter_read_items()] == _BODIES


async def test_workflow_stream_history_has_no_bodies(client: Client):
    stream_client = _with_backend(client, MemoryStreamBackend())
    handle, _ = await _run_append_workflow(stream_client)
    events = (await handle.fetch_history()).events
    markers = [
        event.marker_recorded_event_attributes.marker_name
        for event in events
        if event.event_type == EventType.EVENT_TYPE_MARKER_RECORDED
    ]
    assert markers == ["core_local_activity", "core_local_activity"]
    for event in events:
        for body in _BODIES:
            assert body.encode() not in event.SerializeToString()


async def test_workflow_stream_replay_does_not_call_backend(client: Client):
    stream_client = _with_backend(client, MemoryStreamBackend())
    handle, _ = await _run_append_workflow(stream_client)
    await Replayer(
        workflows=[AppendWorkflow], stream_backends=[FailingStreamBackend()]
    ).replay_workflow(await handle.fetch_history())


async def test_workflow_stream_without_backend_starts_nexus_operation(client: Client):
    async with new_worker(client, AppendWorkflow) as worker:
        handle = await client.start_workflow(
            AppendWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            execution_timeout=timedelta(seconds=30),
        )
        # The dev server doesn't serve the Stream service, so it rejects the Nexus
        # command that only the system Nexus path makes.
        await assert_task_fail_eventually(
            handle, message_contains="temporal.api.streamservice.v1.StreamService"
        )
        await handle.terminate()


async def test_workflow_stream_close_and_read_fail_with_backend(client: Client):
    stream_client = _with_backend(client, MemoryStreamBackend())
    async with new_worker(stream_client, UnsupportedCallsWorkflow) as worker:
        errors = await stream_client.execute_workflow(
            UnsupportedCallsWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert errors == [
        "CloseStream from Workflow code with a Stream backend isn't supported yet",
        "ReadRecords from Workflow code with a Stream backend isn't supported yet",
        "CloseStream from Workflow code with a Stream backend isn't supported yet",
    ]


async def test_workflow_stream_backend_error_reaches_workflow_code(client: Client):
    backend = FlakyAppendBackend(StreamBackendCallOptions(), failures=1)
    result = await _append_result(client, backend)
    assert result == "ApplicationError RuntimeError: append failed"
    assert backend.appends == 1


async def test_workflow_stream_backend_call_options_retry(client: Client):
    options = StreamBackendCallOptions(
        retry_policy=RetryPolicy(
            initial_interval=timedelta(milliseconds=10), maximum_attempts=2
        )
    )
    backend = FlakyAppendBackend(options, failures=1)
    assert await _append_result(client, backend) == "appended"
    assert backend.appends == 2


async def test_workflow_stream_backend_call_options_timeout(client: Client):
    options = StreamBackendCallOptions(
        start_to_close_timeout=timedelta(milliseconds=200)
    )
    backend = FlakyAppendBackend(options, delay=timedelta(seconds=5))
    assert await _append_result(client, backend) == "TimeoutError START_TO_CLOSE"


async def test_worker_keeps_its_stream_backends(client: Client):
    stream_client = _with_backend(client, MemoryStreamBackend())
    async with new_worker(stream_client, AppendWorkflow) as worker:
        with pytest.raises(ValueError, match="Stream backends"):
            worker.client = client
        worker.client = stream_client


async def test_activity_interceptors_skip_stream_backend_activity(client: Client):
    interceptor = RecordingInterceptor()
    stream_client = _with_backend(client, MemoryStreamBackend())
    async with new_worker(
        stream_client,
        AppendAndActivityWorkflow,
        activities=[user_activity],
        interceptors=[interceptor],
    ) as worker:
        await stream_client.execute_workflow(
            AppendAndActivityWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert interceptor.activity_types == ["user_activity"]
