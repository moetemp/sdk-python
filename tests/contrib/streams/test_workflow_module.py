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

from temporalio import activity, common, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType, StreamErrorReason
from temporalio.client import (
    Client,
    WorkflowFailureError,
    WorkflowHandle,
)
from temporalio.common import RetryPolicy
from temporalio.common._streams import _writes
from temporalio.common._streams._backend import _ExtensionCalls
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
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner
from tests.contrib.streams._support import workflow_stream
from tests.helpers import assert_eventually, assert_task_fail_eventually, new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@common.defn(name="tokens")
class Tokens(common.StreamDefinition[str]): ...


async def read(
    handle: WorkflowHandle[Any, Any],
    definition: type[common.StreamDefinition[Any]] = Tokens,
) -> list[object]:
    stream = workflow_stream(handle, definition)
    return [item async for item in stream.subscribe(wait_new_records=False)]


def data(items: Sequence[object]) -> list[object]:
    return [item.data for item in items if isinstance(item, common.Record)]


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        # Two channels are two Streams.
        turns = await workflow.create_stream(Turns)
        tokens = await workflow.create_stream(Tokens)
        await turns.publish(Turn("user", prompt), Turn("model"))
        await tokens.publish(*prompt.split())
        # A run's end doesn't close these Streams, so the run closes them before it returns.
        await turns.close(result=prompt.upper())
        await tokens.close()
        return prompt.upper()


async def test_create_publish_and_close(stream_client: Client):
    async with new_worker(stream_client, Agent) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "HI THERE"
        turns = await read(handle, Turns)
        tokens = await read(handle, Tokens)
    assert data(turns) == [Turn("user", "hi there"), Turn("model")]
    assert data(tokens) == ["hi", "there"]
    closed = turns[-1]
    assert isinstance(closed, common.Closed) and closed.result == "HI THERE"
    assert isinstance(tokens[-1], common.Closed)


@common.defn(name="first")
class First(common.StreamDefinition[str]): ...


@common.defn(name="second")
class Second(common.StreamDefinition[str]): ...


@workflow.defn
class Blocks:
    @workflow.run
    async def run(self) -> None:
        async with workflow.create_stream(First) as first:
            await first.publish("token-0")
        try:
            async with workflow.create_stream(Second) as second:
                await second.publish("token-1")
                raise ApplicationError("turn failed")
        except ApplicationError:
            pass


async def test_async_with_closes_the_stream(stream_client: Client):
    async with new_worker(stream_client, Blocks) as worker:
        handle = await stream_client.start_workflow(
            Blocks.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        first = await read(handle, First)
        second = await read(handle, Second)
    assert data(first) == ["token-0"]
    assert isinstance(first[-1], common.Closed) and first[-1].failure is None
    assert data(second) == ["token-1"]
    assert isinstance(second[-1], common.Closed)
    assert str(second[-1].failure) == "turn failed"


@workflow.defn
class Continuing:
    @workflow.run
    async def run(self, stream: common.WorkflowStream[str] | None) -> str:
        if stream is None:
            stream = await workflow.create_stream(Tokens)
            await stream.publish("run 1")
            workflow.continue_as_new(stream)
        assert stream._payload_type is str
        assert stream == workflow.stream_handle(Tokens)
        await stream.publish("run 2")
        await stream.close(result="done")
        return "done"


@workflow.defn
class CreatesInEachRun:
    @workflow.run
    async def run(self, first: bool) -> str:
        # The id has the first run id, so the run after continue-as-new gets the same Stream.
        # The Stream exists then, so its create ignores the other Retention.
        retention = timedelta(days=1 if first else 2)
        stream = await workflow.create_stream(Tokens, retention=retention)
        await stream.publish("run 1" if first else "run 2")
        if first:
            workflow.continue_as_new(False)
        await stream.close()
        return stream.id


async def test_create_in_the_next_run_gets_the_same_stream(stream_client: Client):
    async with new_worker(stream_client, CreatesInEachRun) as worker:
        handle = await stream_client.start_workflow(
            CreatesInEachRun.run,
            True,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        stream_id = await handle.result()
        items = await read(handle)
    assert stream_id == workflow_stream(handle, Tokens).id
    assert data(items) == ["run 1", "run 2"]
    assert isinstance(items[-1], common.Closed)


async def test_continue_as_new_passes_the_stream(stream_client: Client):
    async with new_worker(stream_client, Continuing) as worker:
        handle = await stream_client.start_workflow(
            Continuing.run, None, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "done"
        items = await read(handle)
    assert data(items) == ["run 1", "run 2"]
    closed = [item for item in items if isinstance(item, common.Closed)]
    assert len(closed) == 1 and closed[0].result == "done"


@workflow.defn
class Mistakes:
    def __init__(self) -> None:
        self._stream: common.WorkflowStream[str] | None = None

    @workflow.run
    async def run(self) -> list[str]:
        errors = []
        stream = await workflow.create_stream(Tokens)
        self._stream = stream
        await stream.close(result="early")
        try:
            await stream.publish("late")
        except common.StreamClosedError as err:
            errors.append(StreamErrorReason.Name(err.reason))
        try:
            await workflow.create_stream(Tokens)
        except common.StreamClosedError as err:
            errors.append(StreamErrorReason.Name(err.reason))
        return errors

    @workflow.query
    def publish_in_query(self) -> str:
        assert self._stream is not None
        try:
            self._stream._append(("x",))
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
        assert await handle.result() == [
            "STREAM_ERROR_REASON_CLOSED",
            "STREAM_ERROR_REASON_CLOSED",
        ]
        assert await handle.query(Mistakes.publish_in_query) == "ReadOnlyContextError"
        items = await read(handle)
    assert isinstance(items[-1], common.Closed) and items[-1].result == "early"


@workflow.defn
class ClosesAReceivedStream:
    @workflow.run
    async def run(self, stream: common.WorkflowStream[str]) -> str:
        await stream.publish("from workflow")
        try:
            await stream.close(result="ended by a writer")
        except common.StreamNotOwnerError as err:
            return StreamErrorReason.Name(err.reason)
        return "closed"


async def test_workflow_closes_only_its_own_streams(stream_client: Client):
    stream = await stream_client.create_stream(Tokens, id=f"owned-{uuid.uuid4()}")
    async with new_worker(stream_client, ClosesAReceivedStream) as worker:
        assert (
            await stream_client.execute_workflow(
                ClosesAReceivedStream.run,
                common.WorkflowStream.from_handle(stream),
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "STREAM_ERROR_REASON_NOT_OWNER"
        )
    # The Workflow that received the Stream can't close it, so it stays open.
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert data(items) == ["from workflow"]


@workflow.defn
class Failing:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("before")
        raise ApplicationError("run failed", non_retryable=True)


async def test_run_end_closes_nothing(stream_client: Client):
    async with new_worker(stream_client, Failing) as worker:
        handle = await stream_client.start_workflow(
            Failing.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        items = await read(handle)
    # The Stream stays open, so a later reader still waits for a close.
    assert data(items) == ["before"]
    assert not any(isinstance(item, common.Closed) for item in items)


@workflow.defn
class FailsInItsBlockOnce:
    @workflow.run
    async def run(self) -> str:
        attempt = workflow.info().attempt
        async with workflow.create_stream(Tokens) as stream:
            await stream.publish(f"run-{attempt}")
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
    written = [item for item in items if isinstance(item, common.Record)]
    superseded = [item for item in items if isinstance(item, common.Superseded)]
    assert [r.data for r in written] == ["run-1", "run-2"]
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["run-1"]
    # Only the block end of the run that succeeds closes the Stream.
    closes = [item for item in items if isinstance(item, common.Closed)]
    assert len(closes) == 1 and items[-1] is closes[0]
    assert closes[0].failure is None


@workflow.defn
class EndsItsBlock:
    @workflow.run
    async def run(self, mode: str) -> str:
        try:
            async with workflow.create_stream(Tokens) as stream:
                await stream.publish(mode)
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
    # run's end, with the block's exception.
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
            items = await read(handle)
            assert data(items) == [mode]
            closed = items[-1]
            assert isinstance(closed, common.Closed), mode
            assert closed.status == common.CloseStatus.FAILED
            assert isinstance(closed.failure, ApplicationError)
            assert closed.failure.message == "block failed"
        # A close at the run's end is a command after the run's code, even on replay.
        replayer = Replayer(
            workflows=[EndsItsBlock],
            stream_backends=stream_client.config()["stream_backends"],
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
        async with workflow.create_stream(Tokens) as stream:
            await stream.publish("started")
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


async def test_cancel_during_an_activity_closes_as_canceled(stream_client: Client):
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
            items = await read(handle)
            assert data(items) == ["started"]
            closed = items[-1]
            assert isinstance(closed, common.Closed), f"local={local}"
            assert closed.status == common.CloseStatus.CANCELED


@workflow.defn
class CronBlock:
    @workflow.run
    async def run(self) -> str:
        async with workflow.create_stream(Tokens) as stream:
            await stream.publish("turn")
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
        reader = workflow_stream(handle, Tokens).subscribe(
            create_wait=timedelta(seconds=20)
        )
        first = await asyncio.wait_for(anext(reader), 20)
        second = await asyncio.wait_for(anext(reader), 20)
        await reader.aclose()
        await handle.terminate()
    assert isinstance(first, common.Record) and isinstance(second, common.Record)
    items = await read(handle)
    assert not any(isinstance(item, common.Closed) for item in items)


failed_close_tasks: set[str] = set()


@workflow.defn
class SchedulesACloseThenFailsItsTask:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("token")
        stream.close_nowait(result="closed")
        workflow_id = workflow.info().workflow_id
        if workflow_id not in failed_close_tasks:
            failed_close_tasks.add(workflow_id)
            raise RuntimeError("fail the Workflow Task that schedules the close")


async def test_failed_task_writes_no_close(stream_client: Client):
    # The test's set must keep its value across the task's retry, so the Workflow has no
    # sandbox.
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
    closes = [item for item in items if isinstance(item, common.Closed)]
    assert len(closes) == 1 and closes[0].result == "closed"
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
class CreatesOn:
    @workflow.run
    async def run(self, backend: str | None) -> None:
        await workflow.create_stream(Tokens, backend=backend)


async def test_worker_config_errors_fail_the_task(
    client: Client, stream_client: Client
):
    # A Worker with no plugin, or with another backend, fails the Workflow Task, so a Worker
    # with the right config can continue the run.
    for task_client, backend, message in (
        (client, None, "No Stream backend"),
        (stream_client, "elsewhere", "backend elsewhere"),
    ):
        async with new_worker(task_client, CreatesOn) as worker:
            handle = await task_client.start_workflow(
                CreatesOn.run,
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
        self._stream = workflow.create_stream_nowait(Tokens)
        self._prompt = prompt
        self._done = False

    @workflow.run
    async def run(self, prompt: str) -> None:
        assert prompt == self._prompt
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def token(self, text: str) -> None:
        self._stream.publish_nowait(text, self._prompt)

    @workflow.update
    def finish(self) -> str:
        self._stream.close_nowait(result="finished")
        try:
            self._stream.publish_nowait("late")
        except common.StreamClosedError as err:
            return StreamErrorReason.Name(err.reason)
        finally:
            self._done = True
        return "wrote"


async def test_sync_handlers_publish_and_close_with_no_await(stream_client: Client):
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
        items = await read(handle)
    assert data(items) == ["a", "p"]
    assert isinstance(items[-1], common.Closed) and items[-1].result == "finished"


@workflow.defn
class CreatesInInit:
    @workflow.init
    def __init__(self, prompt: str) -> None:
        self._stream = workflow.create_stream_nowait(Tokens)
        self._stream.publish_nowait(prompt)

    @workflow.run
    async def run(self, prompt: str) -> str:
        await self._stream.publish(prompt.upper())
        await self._stream.close(result="done")
        return "done"


async def test_init_creates_and_publishes_with_no_await(stream_client: Client):
    async with new_worker(stream_client, CreatesInInit) as worker:
        handle = await stream_client.start_workflow(
            CreatesInInit.run,
            "hi",
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "done"
        items = await read(handle)
    assert data(items) == ["hi", "HI"]
    assert isinstance(items[-1], common.Closed) and items[-1].result == "done"


@workflow.defn
class PublishesWithNoAwait:
    @workflow.run
    async def run(self) -> list[int]:
        tokens = await workflow.create_stream(Tokens)
        first = tokens.publish("a")
        second = tokens.publish("b")
        last = await tokens.publish("c")
        # Nobody awaits this write, and the run's end still waits for it.
        tokens.publish("d")
        return [first.result(), second.result(), last]


async def test_publish_starts_the_write_and_returns_its_future(stream_client: Client):
    async with new_worker(stream_client, PublishesWithNoAwait) as worker:
        handle = await stream_client.start_workflow(
            PublishesWithNoAwait.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        offsets = await handle.result()
        items = await read(handle)
    written = [item for item in items if isinstance(item, common.Record)]
    assert [r.data for r in written] == ["a", "b", "c", "d"]
    assert offsets == [r.offset for r in written[:3]]


@workflow.defn
class LeavesAFailedPublish:
    @workflow.run
    async def run(self, stream_id: str, retrieve: bool) -> str:
        # Nobody created the Stream, so the backend refuses the write.
        missing = common.WorkflowStream[str](id=stream_id)
        future = missing.publish("lost")
        errors: list[BaseException | None] = []
        if retrieve:
            future.add_done_callback(lambda done: errors.append(done.exception()))
            await workflow.wait_condition(lambda: bool(errors))
        return "done"


async def test_publish_error_that_no_caller_gets_raises_at_the_run_end(
    stream_client: Client, caplog: pytest.LogCaptureFixture
):
    # The error raises as if the run didn't catch it, as with publish_nowait.
    lost_id = f"missing-{uuid.uuid4()}"
    with caplog.at_level(logging.WARNING, logger="temporalio.workflow"):
        async with new_worker(stream_client, LeavesAFailedPublish) as worker:
            lost = await stream_client.start_workflow(
                LeavesAFailedPublish.run,
                args=[lost_id, False],
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            with pytest.raises(WorkflowFailureError) as failed:
                await lost.result()
            assert isinstance(failed.value.cause, ApplicationError)
            assert failed.value.cause.type == "StreamNotFoundError"
            # A done callback that reads the error owns it, so the run completes.
            owned = await stream_client.start_workflow(
                LeavesAFailedPublish.run,
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
    async def run(self) -> list[int]:
        tokens = await workflow.create_stream(Tokens)
        tokens.publish_nowait("0")
        # The first write starts, so the next ones wait behind it.
        await asyncio.sleep(0)
        return list(
            await asyncio.gather(*(tokens.publish(str(n)) for n in range(1, 5)))
        )


async def test_concurrent_publishes_keep_their_order(stream_client: Client):
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
        == [item.offset for item in items if isinstance(item, common.Record)][1:]
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
        workflows=[Gathers], stream_backends=stream_client.config()["stream_backends"]
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
class PublishesToAMissingStream:
    @workflow.run
    async def run(self) -> None:
        # No create, so the Store refuses the append, and no retry sends it again.
        stream = workflow.stream_handle(Tokens)
        await stream.publish("x" * 100_000)


async def test_refused_write_keeps_no_pending_bytes(stream_client: Client):
    async with new_worker(stream_client, PublishesToAMissingStream) as worker:
        handle = await stream_client.start_workflow(
            PublishesToAMissingStream.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()
    stream_id = workflow_stream(handle, Tokens).id
    assert not [key for key in _writes._PENDING._entries if key[1] == stream_id]


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
class PublishesLargeValues:
    @workflow.run
    async def run(self, size: int) -> list[int]:
        tokens = await workflow.create_stream(Tokens)
        return list(
            await asyncio.gather(
                tokens.publish(*["a" * size] * 3), tokens.publish(*["b" * size] * 3)
            )
        )


async def test_merge_that_the_codec_grows_past_the_batch_limit(stream_client: Client):
    # The two publishes merge under the Workflow's cap. The codec makes the merge larger than the
    # batch limit, so each publish is sent as its own batch.
    client = with_codec(stream_client, _GrowingCodec())
    async with new_worker(client, PublishesLargeValues) as worker:
        handle = await client.start_workflow(
            PublishesLargeValues.run,
            300_000,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        offsets = await handle.result()
        items = await read(handle)
    written = [item for item in items if isinstance(item, common.Record)]
    assert [len(r.data) for r in written] == [300_000] * 6
    assert offsets == [written[2].offset, written[5].offset]


@workflow.defn
class PublishesTooMuch:
    @workflow.run
    async def run(self) -> str:
        tokens = await workflow.create_stream(Tokens)
        try:
            await tokens.publish(*["x" * 1_400_000] * 4)
        except common.StreamTooLargeError as err:
            return StreamErrorReason.Name(err.reason)
        return "written"


async def test_batch_over_the_store_limit_raises_too_large(stream_client: Client):
    # Each Record is under the Store's Record limit, and the publish's batch is over 4 MiB.
    async with new_worker(stream_client, PublishesTooMuch) as worker:
        handle = await stream_client.start_workflow(
            PublishesTooMuch.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "STREAM_ERROR_REASON_TOO_LARGE"
        assert data(await read(handle)) == []


@workflow.defn
class CreatesOnTheDefault:
    @workflow.run
    async def run(self) -> None:
        await workflow.create_stream(Tokens)


@workflow.defn
class CreatesOnANamedBackend:
    @workflow.run
    async def run(self, backend: str) -> str:
        stream = await workflow.create_stream(Tokens, backend=backend)
        await stream.publish(f"on {backend}")
        await workflow.stream_handle(Tokens).close(result="closed")
        return stream.backend or ""


@workflow.defn
class CreatesAfterAnIdleTime:
    @workflow.run
    async def run(self) -> str:
        await workflow.create_stream(Tokens)
        await workflow.sleep(timedelta(seconds=1.5))
        try:
            await workflow.create_stream(Tokens)
        except common.StreamExpiredError as err:
            return StreamErrorReason.Name(err.reason)
        return "created"


async def test_create_of_an_expired_workflow_stream_raises(client: Client):
    # An open Workflow Stream expires one safety TTL after its last write.
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(default=True, safety_ttl=timedelta(milliseconds=500))
    ]
    expiring = Client(**config)
    async with new_worker(expiring, CreatesAfterAnIdleTime) as worker:
        assert (
            await expiring.execute_workflow(
                CreatesAfterAnIdleTime.run,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "STREAM_ERROR_REASON_EXPIRED"
        )


async def test_default_is_native_with_no_default_backend(client: Client):
    # With no backend marked default=True, the default is native, which this SDK lacks. A create
    # that names the registered backend still works.
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend()]
    memory_client = Client(**config)
    async with new_worker(
        memory_client, CreatesOnTheDefault, CreatesOnANamedBackend
    ) as worker:
        handle = await memory_client.start_workflow(
            CreatesOnTheDefault.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await assert_task_fail_eventually(handle, message_contains="default is native")
        await handle.terminate()
        named = await memory_client.start_workflow(
            CreatesOnANamedBackend.run,
            "memory",
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await named.result() == "memory"
    stream = workflow_stream(named, Tokens, backend="memory")
    items = [item async for item in stream.subscribe()]
    assert data(items) == ["on memory"]
    assert isinstance(items[-1], common.Closed) and items[-1].result == "closed"


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
        stream = await workflow.create_stream(Tokens)
        await stream.publish("a")
        stream.close_nowait(result="bye")
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
    assert isinstance(failed.value.cause, CancelledError)
    types = await _event_types(handle)
    assert EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED not in types
    items = await read(handle)
    assert data(items) == ["a"]
    assert isinstance(items[-1], common.Closed) and items[-1].result == "bye"


@common.defn(name="late")
class Late(common.StreamDefinition[str]): ...


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
        self.late: common.WorkflowStream[str] | None = None

    @workflow.run
    async def run(self) -> str:
        self.late = await workflow.create_stream(Late)
        tokens = await workflow.create_stream(Tokens)
        tokens.close_nowait()
        return "done"

    @workflow.signal
    def write(self) -> None:
        assert self.late is not None
        self.late.publish_nowait("slow")


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
    assert data(await read(handle, Late)) == ["slow"]


class _TagsWithContext(DefaultPayloadConverter, WithSerializationContext):
    # Puts the Workflow id of the serialization context in front of each str value.
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
class PublishesOneValue:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("plain")
        await stream.close()


async def test_workflow_records_get_no_serialization_context(stream_client: Client):
    # Activities and clients convert Records with no context, so Workflows do too.
    config = stream_client.config()
    config["data_converter"] = DataConverter(payload_converter_class=_TagsWithContext)
    tagging = Client(**config)
    async with new_worker(tagging, PublishesOneValue) as worker:
        handle = await tagging.start_workflow(
            PublishesOneValue.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
    assert data(await read(handle)) == ["plain"]
