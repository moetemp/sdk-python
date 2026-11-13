from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

import temporalio.api.streamservice.v1
import temporalio.common
import temporalio.exceptions
from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType, StreamErrorReason
from temporalio.client import (
    Client,
    StreamStore,
    StreamStoreCallOptions,
    WorkflowHandle,
)
from temporalio.common import RetryPolicy
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError, StreamError
from temporalio.nexus.system.stream_service import (
    AppendRecordsResponse,
    ClientProducer,
    Producer,
    ProducerKindClient,
    ProducerKindLocalActivity,
    StreamIdConflictPolicy,
    StreamRecord,
    StreamRef,
)
from temporalio.nexus.system.stream_service.workflow import create_stream, get_stream
from temporalio.testing import MemoryStreamStore
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
    Replayer,
)
from tests.helpers import assert_task_fail_eventually, new_worker
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
class AppendOffsetsWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> AppendRecordsResponse:
        stream = await create_stream(id=stream_id, body_type=str)
        return await stream.append(*_BODIES)


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
        except StreamError as err:
            return f"StreamError {StreamErrorReason.Name(err.reason)}: {err.message}"
        except ApplicationError as err:
            return f"ApplicationError {err.type}: {err.message}"
        except temporalio.exceptions.TimeoutError as err:
            return f"TimeoutError {err.type and err.type.name}"
        return "appended"


@workflow.defn
class AppendToStreamWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> str:
        stream = get_stream(StreamRef(id=stream_id), body_type=str)
        try:
            await stream.append("a")
        except StreamError as err:
            assert isinstance(err, ApplicationError)
            return StreamErrorReason.Name(err.reason)
        return "appended"


@workflow.defn
class CreateThenContinueAsNewWorkflow:
    @workflow.run
    async def run(self, stream_id: str, run: int) -> list[bool]:
        stream = await create_stream(id=stream_id, body_type=str)
        await stream.append(f"run-{run}")
        if run == 1:
            workflow.continue_as_new(args=[stream_id, 2])
        return [bool(stream.created)]


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


_fail_next_task = True


@workflow.defn(sandboxed=False)
class AppendThenFailTaskOnceWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> None:
        stream = await create_stream(
            id=stream_id,
            body_type=str,
            id_conflict_policy=StreamIdConflictPolicy.StreamIdConflictPolicyUseExisting,
        )
        await stream.append("a")
        global _fail_next_task
        if _fail_next_task:
            _fail_next_task = False
            raise RuntimeError("Failed the Workflow Task once")


@activity.defn
async def append_from_local_activity(stream_id: str) -> None:
    stream = activity.client().get_stream(StreamRef(id=stream_id), body_type=str)
    await stream.append("a")


@workflow.defn
class UserLocalActivityAppendWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> None:
        await workflow.execute_local_activity(
            append_from_local_activity,
            stream_id,
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn
class ExplicitProducerWorkflow:
    @workflow.run
    async def run(self, stream_id: str) -> None:
        stream = await create_stream(id=stream_id, body_type=str)
        await stream.append(
            "a", producer=Producer(id="mine", kind=ProducerKindClient(ClientProducer()))
        )


class FailingStreamStore(StreamStore):
    @property
    def name(self) -> str:
        return "failing"

    async def call(self, operation: str, request: bytes) -> bytes:
        raise AssertionError(f"A replay called the Store with {operation}")


class FlakyAppendStore(MemoryStreamStore):
    def __init__(
        self,
        options: StreamStoreCallOptions,
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
    def call_options(self) -> StreamStoreCallOptions:
        return self._options

    async def call(self, operation: str, request: bytes) -> bytes:
        if operation == "AppendRecords":
            self.appends += 1
            await asyncio.sleep(self._delay.total_seconds())
            if self.appends <= self._failures:
                raise RuntimeError("append failed")
        return await super().call(operation, request)


class LostAnswerStore(StreamStore):
    """Writes each call to a memory Store, then can lose the answer of the first append."""

    def __init__(
        self, options: StreamStoreCallOptions, *, lose_first_answer: bool = True
    ) -> None:
        self._store = MemoryStreamStore()
        self._options = options
        self._lose_first_answer = lose_first_answer
        self.appends: list[temporalio.api.streamservice.v1.AppendRecordsRequest] = []
        self.first_response: (
            temporalio.api.streamservice.v1.AppendRecordsResponse | None
        ) = None

    @property
    def name(self) -> str:
        return self._store.name

    @property
    def call_options(self) -> StreamStoreCallOptions:
        return self._options

    async def call(self, operation: str, request: bytes) -> bytes:
        response = await self._store.call(operation, request)
        if operation != "AppendRecords":
            return response
        self.appends.append(
            temporalio.api.streamservice.v1.AppendRecordsRequest.FromString(request)
        )
        if self.first_response is None:
            self.first_response = (
                temporalio.api.streamservice.v1.AppendRecordsResponse.FromString(
                    response
                )
            )
            if not self._lose_first_answer:
                return response
            raise StreamError(
                "Lost the answer of a write",
                reason=StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE,
            )
        return response


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


def _with_store(client: Client, store: StreamStore) -> Client:
    config = client.config()
    config["stream_stores"] = [store]
    return Client(**config)


async def _read_all(client: Client, stream_id: str) -> list[StreamRecord[str]]:
    stream = client.get_stream(StreamRef(id=stream_id), body_type=str)
    return [record async for record in stream.iter_read_items()]


def _producers(records: list[StreamRecord[str]]) -> list[Producer]:
    producers = [record.producer for record in records]
    assert all(producer is not None for producer in producers)
    return [producer for producer in producers if producer is not None]


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


async def _append_result(client: Client, store: StreamStore) -> str:
    stream_client = _with_store(client, store)
    async with new_worker(stream_client, AppendResultWorkflow) as worker:
        return await stream_client.execute_workflow(
            AppendResultWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )


async def test_workflow_stream_uses_store(client: Client):
    stream_client = _with_store(client, MemoryStreamStore())
    handle, ref = await _run_append_workflow(stream_client)

    stream = stream_client.get_stream(ref, body_type=str)
    records = [record async for record in stream.iter_read_items()]
    assert [record.body for record in records] == _BODIES
    run_id = handle.result_run_id
    assert run_id
    first, second = (record.producer for record in records)
    assert first is not None
    assert first == second
    # The create is the run's first Local Activity, so the append is its second.
    assert first.id == f"la:{run_id}:2"
    assert isinstance(first.kind, ProducerKindLocalActivity)
    assert first.kind.value.run_id == run_id
    assert first.kind.value.epoch.task.started_event_id > 0
    assert first.kind.value.epoch.attempt == 1


async def test_workflow_stream_store_uses_payload_codec(client: Client):
    store = MemoryStreamStore()
    config = client.config()
    config["data_converter"] = DataConverter(payload_codec=SimpleCodec())
    config["stream_stores"] = [store]
    stream_client = Client(**config)
    _, ref = await _run_append_workflow(stream_client)

    # A client without the codec reads the bodies as the Store keeps them.
    raw = _with_store(client, store).get_stream(
        StreamRef(id=ref.id), body_type=temporalio.common.RawValue
    )
    stored = [record.body.payload async for record in raw.iter_read_items()]
    assert [body.metadata["simple-codec"] for body in stored] == [b"true", b"true"]
    stream = stream_client.get_stream(ref, body_type=str)
    assert [record.body async for record in stream.iter_read_items()] == _BODIES


async def test_workflow_stream_history_has_no_bodies(client: Client):
    stream_client = _with_store(client, MemoryStreamStore())
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


async def test_workflow_stream_replay_does_not_call_store(client: Client):
    stream_client = _with_store(client, MemoryStreamStore())
    handle, _ = await _run_append_workflow(stream_client)
    await Replayer(
        workflows=[AppendWorkflow], stream_stores=[FailingStreamStore()]
    ).replay_workflow(await handle.fetch_history())


async def test_workflow_stream_without_store_starts_nexus_operation(client: Client):
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


async def test_workflow_stream_close_and_read_fail_with_store(client: Client):
    stream_client = _with_store(client, MemoryStreamStore())
    async with new_worker(stream_client, UnsupportedCallsWorkflow) as worker:
        errors = await stream_client.execute_workflow(
            UnsupportedCallsWorkflow.run,
            f"stream-{uuid.uuid4()}",
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert errors == [
        "CloseStream from Workflow code with a Stream Store isn't supported yet",
        "ReadRecords from Workflow code with a Stream Store isn't supported yet",
        "CloseStream from Workflow code with a Stream Store isn't supported yet",
    ]


async def test_workflow_stream_store_error_reaches_workflow_code(client: Client):
    store = FlakyAppendStore(StreamStoreCallOptions(), failures=1)
    result = await _append_result(client, store)
    assert result == "ApplicationError RuntimeError: append failed"
    assert store.appends == 1


async def test_workflow_append_to_closed_stream_raises_stream_error(client: Client):
    stream_client = _with_store(client, MemoryStreamStore())
    stream = await stream_client.create_stream(
        id=f"stream-{uuid.uuid4()}", body_type=str
    )
    await stream.close()
    async with new_worker(stream_client, AppendToStreamWorkflow) as worker:
        reason = await stream_client.execute_workflow(
            AppendToStreamWorkflow.run,
            stream.ref.id,
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert reason == "STREAM_ERROR_REASON_CLOSED"


async def test_strict_create_in_each_run_of_a_chain_succeeds(client: Client):
    stream_client = _with_store(client, MemoryStreamStore())
    stream_id = f"stream-{uuid.uuid4()}"
    async with new_worker(stream_client, CreateThenContinueAsNewWorkflow) as worker:
        handle = await stream_client.start_workflow(
            CreateThenContinueAsNewWorkflow.run,
            args=[stream_id, 1],
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == [True]
    first_run_id = handle.first_execution_run_id
    last_run = await stream_client.get_workflow_handle(handle.id).describe()
    assert first_run_id
    assert last_run.run_id != first_run_id
    records = await _read_all(stream_client, stream_id)
    assert [record.body for record in records] == ["run-1", "run-2"]
    first, second = _producers(records)
    assert isinstance(first.kind, ProducerKindLocalActivity)
    assert isinstance(second.kind, ProducerKindLocalActivity)
    assert first.kind.value.run_id == first_run_id
    assert second.kind.value.run_id == last_run.run_id


async def test_client_append_after_workflow_append_keeps_offset_order(
    client: Client,
):
    stream_client = _with_store(client, MemoryStreamStore())
    _, ref = await _run_append_workflow(stream_client)
    stream = stream_client.get_stream(ref, body_type=str)
    appended = await stream.append("from-client")
    records = await _read_all(stream_client, ref.id)
    assert [record.body for record in records] == [*_BODIES, "from-client"]
    offsets = [record.offset for record in records]
    assert offsets == sorted(offsets)
    assert len(set(offsets)) == len(offsets)
    assert offsets[-1] == appended.last_offset
    kinds = [type(producer.kind) for producer in _producers(records)]
    assert kinds == [
        ProducerKindLocalActivity,
        ProducerKindLocalActivity,
        ProducerKindClient,
    ]


async def test_workflow_stream_store_call_options_retry(client: Client):
    options = StreamStoreCallOptions(
        retry_policy=RetryPolicy(
            initial_interval=timedelta(milliseconds=10), maximum_attempts=2
        )
    )
    store = FlakyAppendStore(options, failures=1)
    assert await _append_result(client, store) == "appended"
    assert store.appends == 2


async def test_workflow_append_retry_in_one_task_writes_once(client: Client):
    store = LostAnswerStore(
        StreamStoreCallOptions(
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            )
        )
    )
    stream_client = _with_store(client, store)
    stream_id = f"stream-{uuid.uuid4()}"
    async with new_worker(stream_client, AppendOffsetsWorkflow) as worker:
        appended = await stream_client.execute_workflow(
            AppendOffsetsWorkflow.run,
            stream_id,
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    first, second = store.appends
    assert first.producer == second.producer
    assert first.producer.local_activity.epoch.attempt == 1
    assert first.sequence == second.sequence == 1
    records = await _read_all(stream_client, stream_id)
    assert [record.body for record in records] == _BODIES
    assert store.first_response is not None
    assert appended.first_offset == store.first_response.first_offset
    assert appended.last_offset == store.first_response.last_offset
    assert [appended.first_offset, appended.last_offset] == [
        record.offset for record in records
    ]


async def test_workflow_stream_store_call_options_timeout(client: Client):
    options = StreamStoreCallOptions(start_to_close_timeout=timedelta(milliseconds=200))
    store = FlakyAppendStore(options, delay=timedelta(seconds=5))
    assert await _append_result(client, store) == "TimeoutError START_TO_CLOSE"


async def test_worker_keeps_its_stream_stores(client: Client):
    stream_client = _with_store(client, MemoryStreamStore())
    async with new_worker(stream_client, AppendWorkflow) as worker:
        with pytest.raises(ValueError, match="Stream Stores"):
            worker.client = client
        worker.client = stream_client


async def test_activity_interceptors_skip_stream_store_activity(client: Client):
    interceptor = RecordingInterceptor()
    stream_client = _with_store(client, MemoryStreamStore())
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


async def test_workflow_task_retry_appends_with_higher_epoch(client: Client):
    global _fail_next_task
    _fail_next_task = True
    stream_client = _with_store(client, MemoryStreamStore())
    stream_id = f"stream-{uuid.uuid4()}"
    async with new_worker(stream_client, AppendThenFailTaskOnceWorkflow) as worker:
        await stream_client.execute_workflow(
            AppendThenFailTaskOnceWorkflow.run,
            stream_id,
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    first, second = _producers(await _read_all(stream_client, stream_id))
    assert first.id == second.id

    def epoch(producer: Producer) -> tuple[int, int, int]:
        assert isinstance(producer.kind, ProducerKindLocalActivity)
        local = producer.kind.value.epoch
        return (local.task.started_event_id, local.task.attempt, local.attempt)

    assert epoch(second) > epoch(first)


async def test_user_local_activity_append_has_local_activity_producer(client: Client):
    stream_client = _with_store(client, MemoryStreamStore())
    stream = await stream_client.create_stream(
        id=f"stream-{uuid.uuid4()}", body_type=str
    )
    async with new_worker(
        stream_client,
        UserLocalActivityAppendWorkflow,
        activities=[append_from_local_activity],
    ) as worker:
        handle = await stream_client.start_workflow(
            UserLocalActivityAppendWorkflow.run,
            stream.ref.id,
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
    [producer] = _producers(await _read_all(stream_client, stream.ref.id))
    run_id = handle.result_run_id
    assert producer.id == f"la:{run_id}:1"
    assert isinstance(producer.kind, ProducerKindLocalActivity)
    assert producer.kind.value.run_id == run_id
    assert producer.kind.value.epoch.task.started_event_id > 0
    assert producer.kind.value.epoch.attempt == 1


async def test_workflow_explicit_producer_reaches_store(client: Client):
    store = LostAnswerStore(StreamStoreCallOptions(), lose_first_answer=False)
    stream_client = _with_store(client, store)
    stream_id = f"stream-{uuid.uuid4()}"
    async with new_worker(stream_client, ExplicitProducerWorkflow) as worker:
        await stream_client.execute_workflow(
            ExplicitProducerWorkflow.run,
            stream_id,
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    [producer] = _producers(await _read_all(stream_client, stream_id))
    assert producer.id == "mine"
    assert isinstance(producer.kind, ProducerKindClient)
    [request] = store.appends
    assert request.producer.id == "mine"
    assert request.producer.WhichOneof("kind") == "client"
    assert request.sequence == 0
