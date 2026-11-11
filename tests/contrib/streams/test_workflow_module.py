import asyncio
import dataclasses
import logging
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, streams, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType, StreamErrorReason
from temporalio.client import (
    Client,
    WorkflowFailureError,
    WorkflowHandle,
)
from temporalio.common import RetryPolicy
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import (
    DataConverter,
    DefaultPayloadConverter,
    PayloadCodec,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.streams import _writes
from temporalio.streams._backend import _ExtensionCalls
from temporalio.streams.system import models
from temporalio.streams.system.workflow._resources.stream import (
    get_stream as generated_stream,
)
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner
from tests.contrib.streams._support import created, stream_status
from tests.helpers import assert_eventually, assert_task_fail_eventually, new_worker

OPEN = models.StreamStatus.StreamStatusOpen
CLOSED = models.StreamStatus.StreamStatusClosed
USE_EXISTING = streams.StreamIDConflictPolicy.USE_EXISTING


@dataclass
class Turn:
    kind: str
    text: str = ""


def own_id(name: str = "tokens") -> str:
    # A Workflow picks the id of each Stream that it creates. These tests use its Workflow ID.
    return f"{workflow.info().workflow_id}-{name}"


def stream_id(handle: WorkflowHandle[Any, Any], name: str = "tokens") -> str:
    return f"{handle.id}-{name}"


async def read(
    handle: WorkflowHandle[Any, Any],
    name: str = "tokens",
    payload_type: type[Any] = str,
) -> list[object]:
    stream = handle._client.get_stream(
        stream_id(handle, name), payload_type=payload_type
    )
    return [item async for item in stream.read(wait_new_records=False)]


async def status(handle: WorkflowHandle[Any, Any], name: str = "tokens") -> object:
    return await stream_status(handle._client, stream_id(handle, name))


def data(items: Sequence[object]) -> list[object]:
    return [item.data for item in items if isinstance(item, streams.Record)]


def reason(err: streams.StreamError) -> str:
    return StreamErrorReason.Name(StreamErrorReason.ValueType(err.reason))


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        # Two channels are two Streams.
        turns = await workflow.create_stream(own_id("turns"), payload_type=Turn)
        tokens = await workflow.create_stream(own_id("tokens"), payload_type=str)
        await turns.append(Turn("user", prompt), Turn("model"))
        await tokens.append(*prompt.split())
        # A run's end doesn't close these Streams, so the run closes them before it returns.
        await turns.close()
        await tokens.close()
        return prompt.upper()


async def test_create_append_and_close(stream_client: Client):
    async with new_worker(stream_client, Agent) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "HI THERE"
        turns = await read(handle, "turns", Turn)
        tokens = await read(handle)
        assert await status(handle, "turns") == await status(handle) == CLOSED
    assert data(turns) == [Turn("user", "hi there"), Turn("model")]
    assert data(tokens) == ["hi", "there"]


@workflow.defn
class CreatesTwice:
    @workflow.run
    async def run(self, client_id: str) -> list[str]:
        first = await workflow.create_stream(own_id(), payload_type=str)
        # The second create is the same create as the first, so it gets the same Stream.
        again = await workflow.create_stream(own_id(), payload_type=str)
        results = [str(again.ref == first.ref)]
        await first.close()
        try:
            await workflow.create_stream(client_id, payload_type=str)
        except streams.StreamError as err:
            results.append(reason(err))
        # USE_EXISTING gets the Stream that exists.
        theirs = await workflow.create_stream(
            client_id, payload_type=str, id_conflict_policy=USE_EXISTING
        )
        await theirs.append("one")
        return results


async def test_create_fails_on_an_id_that_another_caller_made(stream_client: Client):
    theirs = await stream_client.create_stream(
        f"client-{uuid.uuid4()}", payload_type=str
    )
    async with new_worker(stream_client, CreatesTwice) as worker:
        result = await stream_client.execute_workflow(
            CreatesTwice.run,
            theirs.ref.id,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert result == ["True", "STREAM_ERROR_REASON_ALREADY_EXISTS"]
    items = [item async for item in theirs.read(wait_new_records=False)]
    assert data(items) == ["one"]


@workflow.defn
class CreatesBetweenDraws:
    @workflow.run
    async def run(self) -> bool:
        before = workflow.random().getstate()
        stream = await workflow.create_stream(own_id(), payload_type=str)
        workflow.create_stream_nowait(
            own_id(), payload_type=str, id_conflict_policy=USE_EXISTING
        )
        drawn = workflow.random().getstate() != before
        await stream.close()
        return drawn


async def test_create_leaves_the_workflow_random_values_alone(stream_client: Client):
    async with new_worker(stream_client, CreatesBetweenDraws) as worker:
        drawn = await stream_client.execute_workflow(
            CreatesBetweenDraws.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert not drawn


@workflow.defn
class GeneratedCalls:
    @workflow.run
    async def run(self) -> list[str]:
        tokens = await workflow.create_stream(own_id(), payload_type=str)
        generated = generated_stream[str](
            id=tokens.ref.id,
            backend=tokens.ref.backend or None,
        )
        offset = await generated.append("a", "b")
        refused = []
        try:
            await generated.read()
        except streams.StreamError as err:
            refused.append(reason(err))
        await tokens.close()
        return [str(offset > b""), *refused]


async def test_generated_workflow_calls_run_as_local_activities(stream_client: Client):
    async with new_worker(stream_client, GeneratedCalls) as worker:
        handle = await stream_client.start_workflow(
            GeneratedCalls.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == ["True", "STREAM_ERROR_REASON_UNSUPPORTED"]
        tokens = await read(handle)
        events = await _event_types(handle)
    assert data(tokens) == ["a", "b"]
    # Each write is a marker, as with the Streams, and History has no Nexus event.
    assert events.count(EventType.EVENT_TYPE_MARKER_RECORDED) >= 2
    assert EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED not in events


@workflow.defn
class Blocks:
    @workflow.run
    async def run(self) -> None:
        async with await workflow.create_stream(
            own_id("first"), payload_type=str
        ) as first:
            await first.append("token-0")
        try:
            async with await workflow.create_stream(
                own_id("second"), payload_type=str
            ) as second:
                await second.append("token-1")
                raise ApplicationError("turn failed")
        except ApplicationError:
            pass


async def test_async_with_closes_the_stream(stream_client: Client):
    async with new_worker(stream_client, Blocks) as worker:
        handle = await stream_client.start_workflow(
            Blocks.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        assert data(await read(handle, "first")) == ["token-0"]
        assert data(await read(handle, "second")) == ["token-1"]
        assert await status(handle, "first") == await status(handle, "second") == CLOSED


@workflow.defn
class Continuing:
    @workflow.run
    async def run(self, ref: streams.StreamRef[str] | None) -> str:
        if ref is None:
            created = await workflow.create_stream(own_id(), payload_type=str)
            await created.append("run 1")
            # The next run gets the Stream only through the ref that this run forwards.
            workflow.continue_as_new(created.ref)
        stream = workflow.get_stream(ref)
        await stream.append("run 2")
        await stream.close()
        return "done"


async def test_continue_as_new_passes_the_stream(stream_client: Client):
    async with new_worker(stream_client, Continuing) as worker:
        handle = await stream_client.start_workflow(
            Continuing.run, None, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "done"
        assert data(await read(handle)) == ["run 1", "run 2"]
        assert await status(handle) == CLOSED


@workflow.defn
class CreatesInEachRun:
    @workflow.run
    async def run(self, first: bool) -> str:
        # The next run sends the create of the first run, so it gets the same Stream.
        stream = await workflow.create_stream(own_id(), payload_type=str)
        if first:
            await stream.append("run 1")
            workflow.continue_as_new(False)
        await stream.append("run 2")
        await stream.close()
        return "done"


async def test_create_in_the_next_run_gets_the_same_stream(stream_client: Client):
    async with new_worker(stream_client, CreatesInEachRun) as worker:
        handle = await stream_client.start_workflow(
            CreatesInEachRun.run,
            True,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "done"
        assert data(await read(handle)) == ["run 1", "run 2"]


@workflow.defn
class Mistakes:
    def __init__(self) -> None:
        self._stream: workflow.WorkflowStream[str] | None = None

    @workflow.run
    async def run(self) -> list[str]:
        errors = []
        stream = await workflow.create_stream(own_id(), payload_type=str)
        self._stream = stream
        await stream.append("last")
        await stream.close()
        try:
            await stream.append("late")
        except streams.StreamError as err:
            errors.append(reason(err))
        # A closed Stream fails a create with CLOSED, which comes before ALREADY_EXISTS.
        for policy in (streams.StreamIDConflictPolicy.FAIL, USE_EXISTING):
            try:
                await workflow.create_stream(
                    own_id(), payload_type=str, id_conflict_policy=policy
                )
            except streams.StreamError as err:
                errors.append(reason(err))
        return errors

    @workflow.query
    def append_in_query(self) -> str:
        assert self._stream is not None
        try:
            self._stream.append("x")
        except workflow.ReadOnlyContextError as err:
            return type(err).__name__
        return "wrote"


async def test_replay_reproducible_errors_raise_in_workflow_code(
    stream_client: Client,
):
    async with new_worker(stream_client, Mistakes) as worker:
        handle = await stream_client.start_workflow(
            Mistakes.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == ["STREAM_ERROR_REASON_CLOSED"] * 3
        assert await handle.query(Mistakes.append_in_query) == "ReadOnlyContextError"
        assert data(await read(handle)) == ["last"]


@workflow.defn
class ClosesAReceivedStream:
    @workflow.run
    async def run(self, ref: streams.StreamRef[str]) -> str:
        stream = workflow.get_stream(ref)
        await stream.append("from workflow")
        await stream.close()
        return "closed"


async def test_workflow_closes_a_standalone_stream_that_it_received(
    stream_client: Client,
):
    # A standalone Stream has no owner, so a Workflow closes it as its run's Producer.
    stream = await stream_client.create_stream(
        f"received-{uuid.uuid4()}", payload_type=str
    )
    async with new_worker(stream_client, ClosesAReceivedStream) as worker:
        assert (
            await stream_client.execute_workflow(
                ClosesAReceivedStream.run,
                stream.ref,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "closed"
        )
    items = [item async for item in stream.read()]
    assert data(items) == ["from workflow"]
    assert await stream_status(stream_client, stream.ref.id) == CLOSED


@workflow.defn
class Failing:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("before")
        raise ApplicationError("run failed", non_retryable=True)


async def test_run_end_closes_nothing(stream_client: Client):
    async with new_worker(stream_client, Failing) as worker:
        handle = await stream_client.start_workflow(
            Failing.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        assert data(await read(handle)) == ["before"]
        # The Stream stays open, so a later reader still waits for a close.
        assert await status(handle) == OPEN


@workflow.defn
class RetriesItsCreate:
    @workflow.run
    async def run(self) -> str:
        attempt = workflow.info().attempt
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append(f"run-{attempt}")
        if attempt == 1:
            raise ApplicationError("the first run fails")
        await stream.close()
        return f"created on attempt {attempt}"


async def test_retry_run_creates_the_stream_of_its_first_run(stream_client: Client):
    # The retry run sends the create of the first run, so the create gets that Stream.
    async with new_worker(stream_client, RetriesItsCreate) as worker:
        handle = await stream_client.start_workflow(
            RetriesItsCreate.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100), maximum_attempts=2
            ),
        )
        assert await handle.result() == "created on attempt 2"
        assert data(await read(handle)) == ["run-1", "run-2"]
        assert await status(handle) == CLOSED


@workflow.defn
class FailsInItsBlockOnce:
    @workflow.run
    async def run(self) -> str:
        attempt = workflow.info().attempt
        async with await workflow.create_stream(own_id(), payload_type=str) as stream:
            await stream.append(f"run-{attempt}")
            if attempt == 1:
                raise ApplicationError("the first run fails")
        return f"done on attempt {attempt}"


async def test_block_end_leaves_the_stream_open_for_a_retry_run(stream_client: Client):
    async with new_worker(stream_client, FailsInItsBlockOnce) as worker:
        handle = await stream_client.start_workflow(
            FailsInItsBlockOnce.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100), maximum_attempts=3
            ),
        )
        assert await handle.result() == "done on attempt 2"
        items = await read(handle)
        # Only the block end of the run that succeeds closes the Stream.
        assert await status(handle) == CLOSED
    written = [item for item in items if isinstance(item, streams.Record)]
    # Both runs' Records stay visible, since a run's writes are never withdrawn.
    assert [r.data for r in written] == ["run-1", "run-2"]


@workflow.defn
class EndsItsBlock:
    @workflow.run
    async def run(self, mode: str) -> str:
        try:
            async with await workflow.create_stream(
                own_id(), payload_type=str
            ) as stream:
                await stream.append(mode)
                if mode == "non_retryable":
                    raise ApplicationError("block failed", non_retryable=True)
                if mode == "listed_type":
                    raise ApplicationError("block failed", type="Listed")
                if mode == "caught_task_failure":
                    raise ValueError("block failed")
                raise ApplicationError("block failed")
        except (ApplicationError, ValueError) as err:
            if mode.startswith("caught"):
                return "caught"
            if mode == "then_non_retryable":
                raise ApplicationError("run failed", non_retryable=True) from err
            raise


async def test_block_end_closes_when_no_retry_run_follows(stream_client: Client):
    # Each run ends with no retry run, so each block end closes its Stream, at once or at the
    # run's end.
    retries = RetryPolicy(
        initial_interval=timedelta(milliseconds=100), maximum_attempts=3
    )
    cases = {
        "non_retryable": retries,
        "last_attempt": RetryPolicy(maximum_attempts=1),
        "listed_type": dataclasses.replace(
            retries, non_retryable_error_types=["Listed"]
        ),
        "caught": retries,
        "caught_task_failure": None,
        "then_non_retryable": retries,
    }
    async with new_worker(stream_client, EndsItsBlock) as worker:
        handles = {
            mode: await stream_client.start_workflow(
                EndsItsBlock.run,
                mode,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
                retry_policy=policy,
            )
            for mode, policy in cases.items()
        }
        for mode, handle in handles.items():
            if mode.startswith("caught"):
                assert await handle.result() == "caught"
            else:
                with pytest.raises(WorkflowFailureError):
                    await handle.result()
            assert data(await read(handle)) == [mode]
            assert await status(handle) == CLOSED, mode
        # A close at the run's end is a command after the run's code, even on replay.
        replayer = Replayer(
            workflows=[EndsItsBlock],
            stream_config=stream_client.config()["stream_config"],
        )
        for mode in ("caught", "then_non_retryable"):
            await replayer.replay_workflow(await handles[mode].fetch_history())


# The Activity of a test's run sets this when it starts, so the test cancels the run then.
waiting: dict[str, asyncio.Event] = {}


@activity.defn
async def waits_for_cancel() -> None:
    waiting[activity.info().workflow_id or ""].set()
    while True:
        activity.heartbeat()
        await asyncio.sleep(0.1)


@workflow.defn
class CancelledInItsBlock:
    @workflow.run
    async def run(self, local: bool) -> None:
        async with await workflow.create_stream(own_id(), payload_type=str) as stream:
            await stream.append("started")
            if local:
                await workflow.execute_local_activity(
                    waits_for_cancel, start_to_close_timeout=timedelta(minutes=1)
                )
            else:
                await workflow.execute_activity(
                    waits_for_cancel,
                    start_to_close_timeout=timedelta(minutes=1),
                    heartbeat_timeout=timedelta(seconds=1),
                )


async def test_cancel_during_an_activity_closes_the_stream(stream_client: Client):
    # The cancel reaches the block as an ActivityError, and a cancelled run gets no retry run.
    async with new_worker(
        stream_client, CancelledInItsBlock, activities=[waits_for_cancel]
    ) as worker:
        for local, policy in ((False, RetryPolicy(maximum_attempts=3)), (True, None)):
            workflow_id = f"wf-{uuid.uuid4()}"
            waiting[workflow_id] = asyncio.Event()
            handle = await stream_client.start_workflow(
                CancelledInItsBlock.run,
                local,
                id=workflow_id,
                task_queue=worker.task_queue,
                retry_policy=policy,
            )
            await asyncio.wait_for(waiting[workflow_id].wait(), 20)
            await handle.cancel()
            with pytest.raises(WorkflowFailureError):
                await handle.result()
            assert data(await read(handle)) == ["started"]
            assert await status(handle) == CLOSED, f"local={local}"


@workflow.defn
class CronBlock:
    @workflow.run
    async def run(self) -> str:
        # Each cron run sends the create of the first run, so it gets the same Stream.
        async with await workflow.create_stream(own_id(), payload_type=str) as stream:
            await stream.append("turn")
        return "done"


async def test_block_end_leaves_a_cron_stream_open(stream_client: Client):
    # The Stream spans the cron chain, so a close would fail the next run's create.
    async with new_worker(stream_client, CronBlock) as worker:
        handle = await stream_client.start_workflow(
            CronBlock.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            cron_schedule="@every 1s",
        )
        reader = (
            await created(stream_client.get_stream(stream_id(handle), payload_type=str))
        ).read()
        first = await asyncio.wait_for(anext(reader), 20)
        second = await asyncio.wait_for(anext(reader), 20)
        await reader.aclose()
        await handle.terminate()
        assert await status(handle) == OPEN
    assert isinstance(first, streams.Record) and isinstance(second, streams.Record)


failed_close_tasks: set[str] = set()


@workflow.defn
class SchedulesACloseThenFailsItsTask:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("token")
        stream.close_nowait()
        workflow_id = workflow.info().workflow_id
        if workflow_id not in failed_close_tasks:
            failed_close_tasks.add(workflow_id)
            raise RuntimeError("fail the Workflow Task that schedules the close")


async def test_failed_task_writes_no_close(stream_client: Client):
    # The test's set must keep its value across the task's retry, so the Workflow has no
    # sandbox. The retried task creates again with the same request id, so the create
    # succeeds.
    async with new_worker(
        stream_client,
        SchedulesACloseThenFailsItsTask,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            SchedulesACloseThenFailsItsTask.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await read(handle)
        assert await status(handle) == CLOSED
    written = [item for item in items if isinstance(item, streams.Record)]
    # The retried task writes the token again, and both copies stay visible.
    assert [r.data for r in written] == ["token", "token"]
    history = await handle.fetch_history()
    # The failed task's close was never sent, so History has one close Activity.
    scheduled = [
        event
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    ]
    assert [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in scheduled
    ] == ["__temporal_streams_close"]


@workflow.defn
class UsesBackend:
    @workflow.run
    async def run(self, backend: str | None) -> None:
        if backend is None:
            await workflow.create_stream(own_id(), payload_type=str)
        else:
            ref = streams.StreamRef[str](id=own_id(), backend=backend)
            await workflow.get_stream(ref).append("token")


async def test_worker_config_errors_fail_the_task(
    client: Client, stream_client: Client
):
    # A Worker with no backend, or a ref of another backend, fails the Workflow Task, so a
    # Worker with the right config can continue the run.
    for task_client, backend, message in (
        (client, None, "No Stream backend"),
        (stream_client, "elsewhere", "backend elsewhere"),
    ):
        async with new_worker(task_client, UsesBackend) as worker:
            handle = await task_client.start_workflow(
                UsesBackend.run,
                backend,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            await assert_task_fail_eventually(handle, message_contains=message)
            await handle.terminate()


@workflow.defn
class SyncHandlers:
    @workflow.init
    def __init__(self, prompt: str) -> None:
        # The create runs first, so the Stream exists for the signal's writes.
        self._stream = workflow.create_stream_nowait(own_id(), payload_type=str)
        self._prompt = prompt
        self._done = False

    @workflow.run
    async def run(self, prompt: str) -> None:
        assert prompt == self._prompt
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def token(self, text: str) -> None:
        self._stream.append_nowait(text, self._prompt)

    @workflow.update
    def finish(self) -> str:
        self._stream.close_nowait()
        try:
            self._stream.append_nowait("late")
        except streams.StreamError as err:
            return reason(err)
        finally:
            self._done = True
        return "wrote"


async def test_sync_handlers_append_and_close_with_no_await(stream_client: Client):
    async with new_worker(stream_client, SyncHandlers) as worker:
        handle = await stream_client.start_workflow(
            SyncHandlers.run, "p", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.signal(SyncHandlers.token, "a")
        assert (
            await handle.execute_update(SyncHandlers.finish)
            == "STREAM_ERROR_REASON_CLOSED"
        )
        await handle.result()
        assert data(await read(handle)) == ["a", "p"]
        assert await status(handle) == CLOSED


@workflow.defn
class CreatesInInit:
    @workflow.init
    def __init__(self, prompt: str) -> None:
        self._stream = workflow.create_stream_nowait(
            own_id(), payload_type=str, id_conflict_policy=USE_EXISTING
        )
        self._stream.append_nowait(prompt)

    @workflow.run
    async def run(self, prompt: str) -> str:
        await self._stream.append(prompt.upper())
        await self._stream.close()
        return "done"


async def test_init_creates_and_appends_with_no_await(stream_client: Client):
    async with new_worker(stream_client, CreatesInInit) as worker:
        handle = await stream_client.start_workflow(
            CreatesInInit.run,
            "hi",
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "done"
        assert data(await read(handle)) == ["hi", "HI"]
        assert await status(handle) == CLOSED


@workflow.defn
class CreatesTwoInInit:
    @workflow.init
    def __init__(self, taken_id: str) -> None:
        # The first create fails while the Local Activity of the second one is in flight.
        workflow.create_stream_nowait(taken_id, payload_type=str)
        workflow.create_stream_nowait(own_id(), payload_type=str)

    @workflow.run
    async def run(self, _taken_id: str) -> str:
        return "done"


async def test_nowait_error_raises_after_the_other_writes(stream_client: Client):
    # A command after the run's fail command fails the Workflow Task, and each retry fails
    # the same way, so the run would never end.
    taken = await stream_client.create_stream(f"taken-{uuid.uuid4()}", payload_type=str)
    async with new_worker(stream_client, CreatesTwoInInit) as worker:
        handle = await stream_client.start_workflow(
            CreatesTwoInInit.run,
            taken.ref.id,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowFailureError) as failed:
            await asyncio.wait_for(handle.result(), 20)
        # The error waits for the other write, so a replay gives the same commands.
        replayer = Replayer(
            workflows=[CreatesTwoInInit],
            stream_config=stream_client.config()["stream_config"],
        )
        await replayer.replay_workflow(await handle.fetch_history())
    assert isinstance(failed.value.cause, ApplicationError)
    assert failed.value.cause.type == "StreamError"
    (details,) = failed.value.cause.details
    assert details.reason == StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS


@workflow.defn
class WritesAfterAFailedCreate:
    @workflow.run
    async def run(self, taken_id: str, wait: bool) -> None:
        stream = workflow.create_stream_nowait(taken_id, payload_type=str)
        if wait:
            await stream.append("from the run")
        else:
            stream.append_nowait("from the run")


@pytest.mark.parametrize("wait", [False, True])
async def test_writes_behind_a_failed_create_send_nothing(
    stream_client: Client, wait: bool
):
    # The Stream of a failed create can be another owner's, so the writes queued behind the
    # create must not reach it, and the run fails.
    taken = await stream_client.create_stream(f"taken-{uuid.uuid4()}", payload_type=str)
    async with new_worker(stream_client, WritesAfterAFailedCreate) as worker:
        handle = await stream_client.start_workflow(
            WritesAfterAFailedCreate.run,
            args=[taken.ref.id, wait],
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowFailureError) as failed:
            await asyncio.wait_for(handle.result(), 20)
    assert isinstance(failed.value.cause, ApplicationError)
    assert failed.value.cause.type == "StreamError"
    (details,) = failed.value.cause.details
    assert details.reason == StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS
    assert [item async for item in taken.read(wait_new_records=False)] == []


@workflow.defn
class AppendsWithNoAwait:
    @workflow.run
    async def run(self) -> list[str]:
        tokens = await workflow.create_stream(own_id(), payload_type=str)
        first = tokens.append("a")
        second = tokens.append("b")
        last = await tokens.append("c")
        # Nobody awaits this write, and the run's end still waits for it.
        tokens.append("d")
        return [str(first.result()), str(second.result()), str(last)]


async def test_append_starts_the_write_and_returns_its_future(stream_client: Client):
    async with new_worker(stream_client, AppendsWithNoAwait) as worker:
        handle = await stream_client.start_workflow(
            AppendsWithNoAwait.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        offsets = await handle.result()
        items = await read(handle)
    written = [item for item in items if isinstance(item, streams.Record)]
    assert [r.data for r in written] == ["a", "b", "c", "d"]
    assert offsets == [str(r.offset) for r in written[:3]]


@workflow.defn
class LeavesAFailedAppend:
    @workflow.run
    async def run(self, stream_id: str, retrieve: bool) -> str:
        # Nobody created the Stream, so the backend refuses the write.
        missing = workflow.get_stream(streams.StreamRef[str](id=stream_id))
        future = missing.append("lost")
        errors: list[BaseException | None] = []
        if retrieve:
            future.add_done_callback(lambda done: errors.append(done.exception()))
            await workflow.wait_condition(lambda: bool(errors))
        return "done"


async def test_append_error_that_no_caller_gets_raises_at_the_run_end(
    stream_client: Client, caplog: pytest.LogCaptureFixture
):
    # The error raises as if the run didn't catch it, as with append_nowait.
    lost_id = f"missing-{uuid.uuid4()}"
    with caplog.at_level(logging.WARNING, logger="temporalio.workflow"):
        async with new_worker(stream_client, LeavesAFailedAppend) as worker:
            lost = await stream_client.start_workflow(
                LeavesAFailedAppend.run,
                args=[lost_id, False],
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            with pytest.raises(WorkflowFailureError) as failed:
                await lost.result()
            assert isinstance(failed.value.cause, ApplicationError)
            assert failed.value.cause.type == "StreamError"
            (details,) = failed.value.cause.details
            assert details.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
            # A done callback that reads the error owns it, so the run completes.
            owned = await stream_client.start_workflow(
                LeavesAFailedAppend.run,
                args=[f"missing-{uuid.uuid4()}", True],
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            assert await owned.result() == "done"
    # The Workflow's logger logs the error that no caller got when the write fails.
    warnings = [
        r.getMessage() for r in caplog.records if "no caller has" in r.getMessage()
    ]
    assert len(warnings) == 1 and lost_id in warnings[0]


@workflow.defn
class Gathers:
    @workflow.run
    async def run(self) -> list[str]:
        tokens = await workflow.create_stream(own_id(), payload_type=str)
        tokens.append_nowait("0")
        # The first write starts, so the next ones wait behind it.
        await asyncio.sleep(0)
        results = await asyncio.gather(*(tokens.append(str(n)) for n in range(1, 5)))
        return [str(offset) for offset in results]


async def test_concurrent_appends_keep_their_order(stream_client: Client):
    async with new_worker(stream_client, Gathers) as worker:
        handle = await stream_client.start_workflow(
            Gathers.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        offsets = await handle.result()
        items = await read(handle)
    assert data(items) == ["0", "1", "2", "3", "4"]
    # The four writes that waited were sent as one Local Activity, and each call got the offset
    # of its own Record.
    assert (
        offsets
        == [str(item.offset) for item in items if isinstance(item, streams.Record)][1:]
    )
    history = await handle.fetch_history()
    markers = [
        e
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_MARKER_RECORDED
    ]
    # The create, the first write and the four that waited.
    assert len(markers) == 3
    # Replay uses the markers and writes nothing.
    await Replayer(
        workflows=[Gathers], stream_config=stream_client.config()["stream_config"]
    ).replay_workflow(history)
    assert data(await read(handle)) == ["0", "1", "2", "3", "4"]


class _RandomCodec(PayloadCodec):
    # Adds random bytes to each payload, as the random IV of an encryption codec does.
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/random"},
                data=os.urandom(8) + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            _inner(p.data[8:]) if _encoded(p, b"binary/random") else p for p in payloads
        ]


class _GrowingCodec(PayloadCodec):
    # Triples each payload, so a merge under the Workflow's cap exceeds the Store's batch limit
    # after the codec.
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={
                    "encoding": b"binary/grown",
                    "size": str(len(p.SerializeToString())).encode(),
                },
                data=p.SerializeToString() * 3,
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            _inner(p.data[: int(p.metadata["size"])])
            if _encoded(p, b"binary/grown")
            else p
            for p in payloads
        ]


def _encoded(payload: Payload, encoding: bytes) -> bool:
    return payload.metadata.get("encoding") == encoding


def _inner(data: bytes) -> Payload:
    inner = Payload()
    inner.ParseFromString(data)
    return inner


def with_codec(client: Client, codec: PayloadCodec) -> Client:
    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=codec
    )
    return Client(**config)


async def test_retried_write_goes_in_once(
    stream_client: Client, monkeypatch: pytest.MonkeyPatch
):
    original = _ExtensionCalls.call_async
    sent: list[list[bytes]] = []

    async def loses_the_first_answer(
        self: Any, method: str, request: Any, response: Any
    ) -> Any:
        answer = await original(self, method, request, response)
        if method == "AppendRecords":
            sent.append([record.body.SerializeToString() for record in request.records])
            if len(sent) == 1:
                raise RuntimeError("the answer is lost")
        return answer

    monkeypatch.setattr(_ExtensionCalls, "call_async", loses_the_first_answer)
    client = with_codec(stream_client, _RandomCodec())
    async with new_worker(client, Gathers) as worker:
        handle = await client.start_workflow(
            Gathers.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        items = await read(handle)
    assert data(items) == ["0", "1", "2", "3", "4"]
    # The retry sends the bytes of the first attempt, so the Store's dedupe matches it as the
    # same batch, even though the codec adds random bytes.
    assert sent[0] == sent[1]


@workflow.defn
class AppendsToAMissingStream:
    @workflow.run
    async def run(self) -> None:
        # No create, so the Store refuses the append, and no retry sends it again.
        stream = workflow.get_stream(streams.StreamRef[str](id=own_id()))
        await stream.append("x" * 100_000)


async def test_refused_write_keeps_no_pending_bytes(stream_client: Client):
    async with new_worker(stream_client, AppendsToAMissingStream) as worker:
        handle = await stream_client.start_workflow(
            AppendsToAMissingStream.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()
    missing = stream_id(handle)
    assert not [key for key in _writes._PENDING._entries if key[1] == missing]


def _entry(size: int) -> Any:
    return _writes._Encoded([], size)


def test_pending_appends_keep_the_newest_epoch_and_a_bounded_size():
    pending = _writes._Pending(100)
    one, two = ("ns", "s", "la:1"), ("ns", "s", "la:2")
    first, retried = _entry(10), _entry(10)
    pending.put(one, (5, 1, 0, 0), first)
    # A rerun under a later epoch replaces the earlier one.
    pending.put(one, (9, 1, 0, 0), retried)
    assert pending.get(one, (5, 1, 0, 0)) is None
    assert pending.get(one, (9, 1, 0, 0)) is retried
    # A write of an earlier epoch is fenced, so its put and its pop don't change the later entry.
    pending.put(one, (5, 1, 0, 0), first)
    pending.pop(one, (5, 1, 0, 0))
    assert pending.get(one, (9, 1, 0, 0)) is retried
    # Past the size limit, the oldest entry is evicted.
    pending.put(two, (9, 1, 0, 0), _entry(95))
    assert pending.get(one, (9, 1, 0, 0)) is None
    assert pending.get(two, (9, 1, 0, 0)) is not None


@workflow.defn
class AppendsLargeValues:
    @workflow.run
    async def run(self, size: int) -> list[str]:
        tokens = await workflow.create_stream(own_id(), payload_type=str)
        results = await asyncio.gather(
            tokens.append(*["a" * size] * 3), tokens.append(*["b" * size] * 3)
        )
        return [str(offset) for offset in results]


async def test_merge_that_the_codec_grows_past_the_batch_limit(stream_client: Client):
    # The two appends merge under the Workflow's cap. The codec makes the merge larger than the
    # batch limit, so each append is sent as its own batch.
    client = with_codec(stream_client, _GrowingCodec())
    async with new_worker(client, AppendsLargeValues) as worker:
        handle = await client.start_workflow(
            AppendsLargeValues.run,
            300_000,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        offsets = await handle.result()
        items = await read(handle)
    written = [item for item in items if isinstance(item, streams.Record)]
    assert [len(r.data) for r in written] == [300_000] * 6
    assert offsets == [str(written[2].offset), str(written[5].offset)]


@workflow.defn
class AppendsTooMuch:
    @workflow.run
    async def run(self) -> str:
        tokens = await workflow.create_stream(own_id(), payload_type=str)
        try:
            await tokens.append(*["x" * 1_400_000] * 4)
        except streams.StreamError as err:
            return reason(err)
        return "written"


async def test_batch_over_the_store_limit_raises_too_large(stream_client: Client):
    # Each Record is under the Store's Record limit, and the append's batch is over 4 MiB.
    async with new_worker(stream_client, AppendsTooMuch) as worker:
        handle = await stream_client.start_workflow(
            AppendsTooMuch.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "STREAM_ERROR_REASON_TOO_LARGE"
        assert data(await read(handle)) == []


@workflow.defn
class CreatesOnTheWorkerBackend:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("on the Worker's backend")
        await workflow.get_stream(stream.ref).close()
        return stream.ref.backend or ""


@workflow.defn
class CreatesThenCloses:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("token")
        await stream.close()


async def test_create_of_an_expired_workflow_stream_raises(client: Client):
    # Retention is a backend setting. A Workflow's standalone Stream expires one Retention after
    # its close, since an open Stream has no last_write_ttl by default. A later client create
    # finds it expired.
    config = client.config()
    config["stream_config"] = streams.StreamConfig(
        backends=[MemoryStreamBackend(retention=timedelta(milliseconds=500))]
    )
    memory_client = Client(**config)
    async with new_worker(memory_client, CreatesThenCloses) as worker:
        handle = await memory_client.start_workflow(
            CreatesThenCloses.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        await asyncio.sleep(1.5)
        with pytest.raises(streams.StreamError) as expired:
            await memory_client.create_stream(
                stream_id(handle),
                payload_type=str,
                id_conflict_policy=USE_EXISTING,
            )
        assert expired.value.reason == StreamErrorReason.STREAM_ERROR_REASON_EXPIRED


async def test_workflow_create_calls_no_selector(client: Client):
    # A Worker serves only one backend, so a Workflow's create goes to it, even when the
    # client's config has a selector.
    backend = MemoryStreamBackend(name="mem")
    selected: list[str] = []

    def select(ctx: streams.StreamSelectContext) -> streams.StreamBackend:
        selected.append(ctx.id)
        return backend

    config = client.config()
    config["stream_config"] = streams.StreamConfig(
        backends=[backend], backend_selector=select
    )
    memory_client = Client(**config)
    async with new_worker(memory_client, CreatesOnTheWorkerBackend) as worker:
        handle = await memory_client.start_workflow(
            CreatesOnTheWorkerBackend.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "mem"
    assert not selected
    assert data(await read(handle)) == ["on the Worker's backend"]
    assert await stream_status(memory_client, stream_id(handle), "mem") == CLOSED


async def _event_types(handle: WorkflowHandle[Any, Any]) -> list[int]:
    return [event.event_type async for event in handle.fetch_history_events()]


async def _close_is_scheduled(handle: WorkflowHandle[Any, Any]) -> None:
    assert EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED in await _event_types(handle)


async def _cancel_is_handled(handle: WorkflowHandle[Any, Any]) -> None:
    types = await _event_types(handle)
    requested = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
    assert requested in types
    assert (
        EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED in types[types.index(requested) :]
    )


@workflow.defn
class ClosesThenReturns:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("a")
        stream.close_nowait()
        return "done"


async def test_cancel_during_the_run_end_waits_for_the_close(stream_client: Client):
    # The first Worker polls no Activities, so the run waits at its end for the close Activity.
    async with new_worker(
        stream_client, ClosesThenReturns, no_remote_activities=True
    ) as worker:
        handle = await stream_client.start_workflow(
            ClosesThenReturns.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await assert_eventually(lambda: _close_is_scheduled(handle))
        await handle.cancel()
        await assert_eventually(lambda: _cancel_is_handled(handle))
        async with new_worker(stream_client, task_queue=worker.task_queue):
            with pytest.raises(WorkflowFailureError) as failed:
                await handle.result()
        assert await status(handle) == CLOSED
    assert isinstance(failed.value.cause, CancelledError)
    types = await _event_types(handle)
    assert EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED not in types
    assert data(await read(handle)) == ["a"]


class _SlowCodec(PayloadCodec):
    # Delays the write of one value past the Workflow Task's heartbeat.
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        if any(b"slow" in payload.data for payload in payloads):
            await asyncio.sleep(3)
        return list(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return list(payloads)


@workflow.defn
class WritesFromASignalAtTheEnd:
    def __init__(self) -> None:
        self.late: workflow.WorkflowStream[str] | None = None

    @workflow.run
    async def run(self) -> str:
        self.late = await workflow.create_stream(own_id("late"), payload_type=str)
        tokens = await workflow.create_stream(own_id(), payload_type=str)
        tokens.close_nowait()
        return "done"

    @workflow.signal
    def write(self) -> None:
        assert self.late is not None
        self.late.append_nowait("slow")


async def _signal_is_handled(handle: WorkflowHandle[Any, Any]) -> None:
    types = await _event_types(handle)
    signaled = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
    assert signaled in types
    assert EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED in types[types.index(signaled) :]


async def test_run_end_waits_for_a_write_that_starts_while_it_waits(
    stream_client: Client,
):
    config = stream_client.config()
    config["data_converter"] = DataConverter(payload_codec=_SlowCodec())
    slow = Client(**config)
    async with new_worker(
        slow, WritesFromASignalAtTheEnd, no_remote_activities=True
    ) as worker:
        # A short task timeout makes Core complete the task before the slow write ends.
        handle = await slow.start_workflow(
            WritesFromASignalAtTheEnd.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            task_timeout=timedelta(seconds=1),
        )
        await assert_eventually(lambda: _close_is_scheduled(handle))
        await handle.signal(WritesFromASignalAtTheEnd.write)
        await assert_eventually(lambda: _signal_is_handled(handle))
        async with new_worker(slow, task_queue=worker.task_queue):
            assert await handle.result() == "done"
    assert data(await read(handle, "late")) == ["slow"]


class _TagsWithContext(DefaultPayloadConverter, WithSerializationContext):
    # Puts the Workflow ID of the serialization context in front of each str value.
    def __init__(self, workflow_id: str = "") -> None:
        super().__init__()
        self._workflow_id = workflow_id

    def with_context(self, context: SerializationContext) -> "_TagsWithContext":
        if isinstance(context, WorkflowSerializationContext):
            return _TagsWithContext(context.workflow_id)
        return self

    def to_payloads(self, values: Sequence[Any]) -> list[Payload]:
        if self._workflow_id:
            values = [
                f"{self._workflow_id}:{v}" if isinstance(v, str) else v for v in values
            ]
        return super().to_payloads(values)


@workflow.defn
class AppendsOneValue:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("plain")
        await stream.close()


async def test_workflow_records_get_no_serialization_context(stream_client: Client):
    # Activities and clients convert Records with no context, so Workflows do too.
    config = stream_client.config()
    config["data_converter"] = DataConverter(payload_converter_class=_TagsWithContext)
    tagging = Client(**config)
    async with new_worker(tagging, AppendsOneValue) as worker:
        handle = await tagging.start_workflow(
            AppendsOneValue.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
    assert data(await read(handle)) == ["plain"]


@workflow.defn
class ContinuesInItsBlock:
    @workflow.run
    async def run(self, ref: streams.StreamRef[str] | None) -> str:
        if ref is None:
            async with await workflow.create_stream(
                own_id(), payload_type=str
            ) as stream:
                await stream.append("run 1")
                workflow.continue_as_new(stream.ref)
        try:
            await workflow.get_stream(ref).append("run 2")
        except streams.StreamError as err:
            return reason(err)
        return "wrote"


async def test_continue_as_new_in_a_block_closes_the_stream(stream_client: Client):
    # The run that continues as new has no retry run, so its block end closes the Stream.
    async with new_worker(stream_client, ContinuesInItsBlock) as worker:
        handle = await stream_client.start_workflow(
            ContinuesInItsBlock.run,
            None,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "STREAM_ERROR_REASON_CLOSED"
        assert data(await read(handle)) == ["run 1"]
        assert await status(handle) == CLOSED
