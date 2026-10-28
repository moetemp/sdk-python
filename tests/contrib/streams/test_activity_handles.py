import asyncio
import contextlib
import contextvars
import ctypes
import gc
import inspect
import sys
import threading
import time
import uuid
from collections.abc import Coroutine, Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from temporalio import activity, common, workflow
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.common._streams import _activity
from temporalio.exceptions import CancelledError
from tests.contrib.streams._support import workflow_stream
from tests.helpers import new_worker


@common.defn(name="tokens")
class Tokens(common.StreamDefinition[str]): ...


@common.defn(name="other")
class Other(common.StreamDefinition[str]): ...


@dataclass
class CallModelInput:
    prompt: str
    stream: common.ActivityStream[str]


@dataclass
class SyncInput:
    prompt: str
    stream: common.SyncActivityStream[str]


async def read(
    handle: WorkflowHandle[Any, Any],
    definition: type[common.StreamDefinition[Any]] = Tokens,
) -> list[object]:
    stream = workflow_stream(handle, definition)
    return [item async for item in stream.subscribe(wait_new_records=False)]


def records(items: Sequence[object]) -> list[common.Record[Any]]:
    return [item for item in items if isinstance(item, common.Record)]


@activity.defn
async def call_model(input: CallModelInput) -> list[int]:
    assert input.stream._payload_type is str
    offsets = [await input.stream.publish(word) for word in input.prompt.split()]
    async with common.BufferedPublisher(
        input.stream, max_delay=timedelta(seconds=10)
    ) as buffered:
        buffered.publish("model-a")
        buffered.publish("model-b")
    offsets.append(await input.stream.get_offset())
    return offsets


@activity.defn
def call_model_sync(input: SyncInput) -> int:
    last = input.stream.publish(*input.prompt.split())
    assert input.stream.get_offset() == last + 1
    return last


@activity.defn
async def read_back(stream: common.ActivityStream[str]) -> list[Any]:
    return [
        item.data
        async for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, common.Record)
    ]


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> list[Any]:
        stream = await workflow.create_stream(Tokens)
        offsets = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, common.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        last = await workflow.execute_activity(
            call_model_sync,
            SyncInput("sync words", common.SyncActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        read = await workflow.execute_activity(
            read_back,
            common.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.close()
        return [offsets, last, read]


async def test_activity_publishes_and_returns_offsets(stream_client: Client):
    async with new_worker(
        stream_client,
        Agent,
        activities=[call_model, call_model_sync, read_back],
        activity_executor=_executor(),
    ) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        offsets, last, read_tokens = await handle.result()
        items = await read(handle)
    data = records(items)
    written = ["hi", "there", "model-a", "model-b", "sync", "words"]
    assert [record.data for record in data] == written
    assert offsets[:2] == [data[0].offset, data[1].offset]
    assert offsets[2] == data[3].offset + 1
    assert last == data[5].offset
    assert read_tokens == written
    assert isinstance(items[-1], common.Closed)


def _executor() -> Any:
    import concurrent.futures

    return concurrent.futures.ThreadPoolExecutor(max_workers=2)


attempts: dict[str, int] = {}


@activity.defn
async def flaky(input: CallModelInput) -> None:
    attempts[input.prompt] = attempts.get(input.prompt, 0) + 1
    if attempts[input.prompt] == 1:
        await input.stream.publish("attempt-1")
        raise RuntimeError("first attempt fails")


@workflow.defn
class Retrying:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("start")
        await workflow.execute_activity(
            flaky,
            CallModelInput(key, common.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def test_retry_withdraws_a_received_stream(stream_client: Client):
    key = str(uuid.uuid4())
    async with new_worker(stream_client, Retrying, activities=[flaky]) as worker:
        handle = await stream_client.start_workflow(
            Retrying.run, key, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        items = await read(handle)
    superseded = [item for item in items if isinstance(item, common.Superseded)]
    # The second attempt writes nothing, and its start still withdraws the first one's Record.
    assert len(superseded) == 1
    withdrawn = [r.data for r in records(items) if superseded[0].withdraws(r)]
    assert withdrawn == ["attempt-1"]


@dataclass
class FinishedStreams:
    key: str
    closed: common.ActivityStream[str]
    expired: common.ActivityStream[str]


@activity.defn
async def reads_after_a_failed_attempt(input: FinishedStreams) -> list[Any]:
    attempts[input.key] = attempts.get(input.key, 0) + 1
    if attempts[input.key] == 1:
        raise RuntimeError("first attempt fails")
    items = input.closed.subscribe(wait_new_records=False)
    return [item.data async for item in items if isinstance(item, common.Record)]


@workflow.defn
class ReadsFinishedStreams:
    @workflow.run
    async def run(self, key: str, expired: common.WorkflowStream[str]) -> list[Any]:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("turn")
        await stream.close()
        return await workflow.execute_activity(
            reads_after_a_failed_attempt,
            FinishedStreams(
                key,
                common.ActivityStream.from_handle(stream),
                common.ActivityStream.from_handle(expired),
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def test_retry_goes_on_with_a_closed_or_expired_stream(stream_client: Client):
    # On a Stream that accepts no write, the second attempt's open withdraws nothing, and the
    # attempt only reads.
    expired = await stream_client.create_stream(
        Tokens, id=f"expired-{uuid.uuid4()}", retention=timedelta(milliseconds=200)
    )
    await expired.publish("old")
    await asyncio.sleep(0.5)
    key = str(uuid.uuid4())
    async with new_worker(
        stream_client, ReadsFinishedStreams, activities=[reads_after_a_failed_attempt]
    ) as worker:
        result = await stream_client.execute_workflow(
            ReadsFinishedStreams.run,
            args=[key, common.WorkflowStream.from_handle(expired)],
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert result == ["turn"] and attempts[key] == 2


# Set by a test after its readers showed the first attempt's Record.
shown: dict[str, asyncio.Event] = {}


@activity.defn
async def fails_after_it_is_shown(input: CallModelInput) -> None:
    attempts[input.prompt] = attempts.get(input.prompt, 0) + 1
    tokens = input.stream
    if attempts[input.prompt] == 1:
        await tokens.publish("attempt-1")
        await shown[input.prompt].wait()
        raise RuntimeError("first attempt fails")
    await tokens.publish("attempt-2")


@workflow.defn
class ShownThenWithdrawn:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(Tokens)
        await workflow.execute_activity(
            fails_after_it_is_shown,
            CallModelInput(key, common.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )
        await stream.close()


async def test_reader_withdraws_a_shown_record_after_a_reconnect(
    stream_client: Client,
):
    key = str(uuid.uuid4())
    shown[key] = asyncio.Event()
    async with new_worker(
        stream_client, ShownThenWithdrawn, activities=[fails_after_it_is_shown]
    ) as worker:
        handle = await stream_client.start_workflow(
            ShownThenWithdrawn.run,
            key,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        stream = workflow_stream(handle, Tokens)
        live = stream.subscribe(create_wait=timedelta(seconds=20))
        reconnects = stream.subscribe(create_wait=timedelta(seconds=20))
        first = await anext(live)
        before = await anext(reconnects)
        # The reader closes its connection and keeps only the Records that it showed and its
        # resume offset, as a UI does across a page reload.
        resume_at = reconnects.next_offset
        await reconnects.aclose()
        shown[key].set()
        live_items = [first, *[item async for item in live]]
        resumed = [item async for item in stream.subscribe(offset=resume_at)]
    assert isinstance(first, common.Record) and first.data == "attempt-1"
    assert isinstance(before, common.Record) and before.offset == first.offset
    [live_superseded] = [i for i in live_items if isinstance(i, common.Superseded)]
    assert live_superseded.withdrawn_offsets == (first.offset,)
    # A new read got no Record of the failed attempt, so the item lists no offset. The kept
    # Record is still withdrawn.
    [superseded] = [i for i in resumed if isinstance(i, common.Superseded)]
    assert superseded.withdrawn_offsets == ()
    assert superseded.withdraws(before)
    assert [r.data for r in records(resumed)] == ["attempt-2"]
    assert isinstance(resumed[-1], common.Closed)


@activity.defn
async def wrong_backend(stream: common.ActivityStream[str]) -> str:
    try:
        await stream.publish("x")
    except common.StreamConfigError:
        return "config"
    return "wrote"


@activity.defn
async def close_it(stream: common.ActivityStream[str]) -> str:
    await stream.publish("last")
    try:
        await stream.close(result="closed by activity")
    except common.StreamNotOwnerError as err:
        return StreamErrorReason.Name(err.reason)
    return "closed"


@workflow.defn
class Closing:
    @workflow.run
    async def run(self) -> list[str]:
        stream = await workflow.create_stream(Tokens)
        backend = await workflow.execute_activity(
            wrong_backend,
            common.ActivityStream[str](id=stream.id, backend="elsewhere"),
            start_to_close_timeout=timedelta(seconds=30),
        )
        owner = await workflow.execute_activity(
            close_it,
            common.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
        # A Local Activity runs before its Workflow Task commits, so it can't close it either.
        local = await workflow.execute_local_activity(
            close_it,
            common.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
        # A handle with only the id names no Workflow, and no Activity can close a Stream.
        by_id = await workflow.execute_local_activity(
            close_it,
            common.ActivityStream[str](id=stream.id),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.close(result="closed by the run")
        return [backend, owner, local, by_id]


async def test_activity_checks_the_backend_and_the_owner(stream_client: Client):
    async with new_worker(
        stream_client, Closing, activities=[wrong_backend, close_it]
    ) as worker:
        handle = await stream_client.start_workflow(
            Closing.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        # An Activity writes to the Workflow's Stream but can't close it.
        assert await handle.result() == [
            "config",
            *["STREAM_ERROR_REASON_NOT_OWNER"] * 3,
        ]
        items = await read(handle)
    assert [r.data for r in records(items)] == ["last", "last", "last"]
    assert isinstance(items[-1], common.Closed)
    assert items[-1].result == "closed by the run"


class _Batches(common.ActivityStream[int]):
    # A Stream whose writes go to a list, so the batches show with no backend.
    def __init__(self, fail: bool = False) -> None:
        super().__init__(id="batches")
        self.batches: list[tuple[int, ...]] = []
        self.fail = fail
        self.in_flight = 0
        self.most_in_flight = 0

    def _access(self) -> Any:
        return SimpleNamespace(publish=self._write)

    async def _write(self, values: Sequence[int]) -> int:
        if self.fail:
            raise common.StreamStorageError("store down")
        self.in_flight += 1
        self.most_in_flight = max(self.most_in_flight, self.in_flight)
        await asyncio.sleep(0.01)
        self.in_flight -= 1
        self.batches.append(tuple(values))
        return sum(len(batch) for batch in self.batches)


async def test_buffered_publisher_batches_by_delay():
    stream = _Batches()
    publisher = common.BufferedPublisher(stream, max_delay=timedelta(milliseconds=20))
    publisher.publish(1)
    publisher.publish(2, 3)
    await asyncio.sleep(0.1)
    assert stream.batches == [(1, 2, 3)]
    publisher.publish(4)
    assert await publisher.flush() == 4
    assert stream.batches == [(1, 2, 3), (4,)]
    assert await publisher.flush() == 4


async def test_buffered_publisher_writes_a_full_batch_with_one_write_in_flight():
    stream = _Batches()
    publisher = common.BufferedPublisher(stream, max_delay=timedelta(minutes=10))
    publisher.publish(*range(1000))
    publisher.publish(*range(1000, 2500))
    await asyncio.sleep(0.2)
    # A full batch is written without waiting for the delay. The values published during its
    # write are in the next batch.
    assert [len(batch) for batch in stream.batches] == [1000, 1500]
    assert stream.most_in_flight == 1
    assert await publisher.flush() == 2500


async def test_buffered_publisher_raises_a_failed_batch():
    stream = _Batches(fail=True)
    with pytest.raises(common.StreamStorageError):
        async with common.BufferedPublisher(stream) as publisher:
            publisher.publish(1)
    with pytest.raises(TypeError):
        common.BufferedPublisher(stream).publish()


@activity.defn
async def local_step(stream: common.ActivityStream[str]) -> list[Any]:
    # Each Workflow write is an awaited Local Activity, so the run's create and Records are in
    # the Store before a later Local Activity starts.
    seen = [
        item.data
        async for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, common.Record)
    ]
    await stream.publish("local")
    return seen


@workflow.defn
class LocalOrder:
    @workflow.run
    async def run(self) -> list[Any]:
        stream = await workflow.create_stream(Tokens)
        await stream.publish("before")
        seen = await workflow.execute_local_activity(
            local_step,
            common.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.publish("after")
        await stream.close()
        return seen


async def test_workflow_writes_go_out_before_a_local_activity(stream_client: Client):
    async with new_worker(stream_client, LocalOrder, activities=[local_step]) as worker:
        handle = await stream_client.start_workflow(
            LocalOrder.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == ["before"]
        items = await read(handle)
    written = records(items)
    assert [record.data for record in written] == ["before", "local", "after"]
    producer = written[1].producer
    assert producer is not None and producer.id.startswith("la:")
    assert isinstance(items[-1], common.Closed)


async def test_buffered_publisher_stays_failed():
    stream = _Batches(fail=True)
    publisher = common.BufferedPublisher(stream)
    publisher.publish(1, 2)
    with pytest.raises(common.StreamStorageError):
        await publisher.flush()
    stream.fail = False
    # A later batch would leave a gap, so the publisher refuses it and keeps the error.
    with pytest.raises(common.StreamStorageError):
        publisher.publish(3)
    with pytest.raises(common.StreamStorageError):
        await publisher.flush()
    assert stream.batches == [] and publisher.unwritten == 2


@activity.defn
async def never_flushes(input: CallModelInput) -> None:
    publisher = common.BufferedPublisher(input.stream, max_delay=timedelta(minutes=10))
    publisher.publish(*input.prompt.split())


@dataclass
class ManyStreams:
    key: str
    targets: list[common.ActivityStream[str]]
    maybe: common.ActivityStream[str] | None = None


@activity.defn
async def flaky_many(input: ManyStreams) -> None:
    attempts[input.key] = attempts.get(input.key, 0) + 1
    assert input.maybe is not None
    if attempts[input.key] == 1:
        for stream in [*input.targets, input.maybe]:
            await stream.publish("attempt-1")
        raise RuntimeError("first attempt fails")


@activity.defn
def sync_read(stream: common.SyncActivityStream[str]) -> list[Any]:
    return [
        item.data
        for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, common.Record)
    ]


@workflow.defn
class Leftovers:
    @workflow.run
    async def run(self, key: str) -> list[Any]:
        stream = await workflow.create_stream(Tokens)
        other = await workflow.create_stream(Other)
        await workflow.execute_activity(
            never_flushes,
            CallModelInput("a b", common.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            flaky_many,
            ManyStreams(
                key,
                [common.ActivityStream.from_handle(stream)],
                common.ActivityStream.from_handle(other),
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )
        return await workflow.execute_activity(
            sync_read,
            common.SyncActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )


async def test_attempt_end_flushes_and_retry_finds_nested_streams(
    stream_client: Client,
):
    key = str(uuid.uuid4())
    async with new_worker(
        stream_client,
        Leftovers,
        activities=[never_flushes, flaky_many, sync_read],
        activity_executor=_executor(),
    ) as worker:
        handle = await stream_client.start_workflow(
            Leftovers.run, key, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        # The publisher was never flushed, so the end of the attempt wrote its values.
        assert await handle.result() == ["a", "b", "attempt-1"]
        items = await read(handle)
        other = await read(handle, Other)
    for read_items in (items, other):
        superseded = [i for i in read_items if isinstance(i, common.Superseded)]
        withdrawn = [r.data for r in records(read_items) if superseded[0].withdraws(r)]
        assert withdrawn == ["attempt-1"]


@contextlib.contextmanager
def _loop_in_thread() -> Iterator[asyncio.AbstractEventLoop]:
    # Each test closes its loop. The finalizer of a loop left open can run in a later caller
    # thread and swallow the cancel that a test raises there.
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(5)
        loop.close()


def _long_poll(
    started: threading.Event, stopped: threading.Event
) -> Coroutine[Any, Any, None]:
    async def long_poll() -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            stopped.set()

    return long_poll()


def _caller(
    loop: asyncio.AbstractEventLoop,
    call: Coroutine[Any, Any, None],
    errors: list[BaseException],
) -> threading.Thread:
    stream: common.SyncActivityStream[Any] = common.SyncActivityStream(id="s")
    token = _activity.current.set(
        cast(Any, SimpleNamespace(loop=loop, backend_name="", default=True))
    )

    def run() -> None:
        try:
            stream._run(call)
        except BaseException as err:
            errors.append(err)

    caller = threading.Thread(target=contextvars.copy_context().run, args=(run,))
    _activity.current.reset(token)
    return caller


def _waits_for_the_call(caller: threading.Thread) -> bool:
    frame = sys._current_frames().get(caller.ident or 0)
    while frame is not None:
        if (
            frame.f_code.co_name == "result"
            and "concurrent" in frame.f_code.co_filename
        ):
            return True
        frame = frame.f_back
    return False


def test_sync_call_stops_when_the_thread_is_cancelled():
    started, stopped = threading.Event(), threading.Event()
    errors: list[BaseException] = []
    with _loop_in_thread() as loop:
        caller = _caller(loop, _long_poll(started, stopped), errors)
        # A garbage collection in the caller thread runs finalizers there, and a finalizer
        # swallows an exception that is raised in its thread.
        gc.collect()
        gc.disable()
        try:
            caller.start()
            assert started.wait(5)
            # Raise only while the caller waits on the call's future, so the test checks _run and
            # not where the scheduler happens to stop the caller thread.
            deadline = time.monotonic() + 5
            while not _waits_for_the_call(caller) and time.monotonic() < deadline:
                time.sleep(0.001)
            assert _waits_for_the_call(caller)
            # The Worker cancels a sync Activity by raising in its thread.
            assert caller.ident is not None
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(caller.ident), ctypes.py_object(CancelledError)
            )
            caller.join(5)
        finally:
            gc.enable()
        assert stopped.wait(5)
    assert errors and isinstance(errors[0], CancelledError)


def test_sync_call_on_a_closed_loop_closes_its_coroutine():
    errors: list[BaseException] = []
    loop = asyncio.new_event_loop()
    loop.close()
    call = _long_poll(threading.Event(), threading.Event())
    caller = _caller(loop, call, errors)
    caller.start()
    caller.join(5)
    assert errors and isinstance(errors[0], RuntimeError)
    # A closed coroutine can't warn at shutdown that nobody awaited it.
    assert inspect.getcoroutinestate(call) == inspect.CORO_CLOSED


def test_sync_call_stops_when_the_cancel_lands_right_after_the_schedule():
    started, stopped = threading.Event(), threading.Event()
    errors: list[BaseException] = []
    with _loop_in_thread() as loop:
        schedule = loop.call_soon_threadsafe
        cancelled: list[bool] = []

        def schedule_then_cancel(*args: Any) -> Any:
            handle = schedule(*args)
            # The Worker's cancel lands in the Activity thread as soon as the call is scheduled.
            if threading.current_thread() is caller and not cancelled:
                cancelled.append(True)
                assert started.wait(5)
                raise CancelledError()
            return handle

        setattr(loop, "call_soon_threadsafe", schedule_then_cancel)
        caller = _caller(loop, _long_poll(started, stopped), errors)
        caller.start()
        caller.join(5)
        assert stopped.wait(5)
    assert errors and isinstance(errors[0], CancelledError)


@activity.defn
async def plain(name: str) -> str:
    return f"hello {name}"


@workflow.defn
class NeverTouchesStreams:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            plain, "ada", start_to_close_timeout=timedelta(seconds=10)
        )


@workflow.defn
class ReceivesAStream:
    @workflow.run
    async def run(self, stream: common.WorkflowStream[str]) -> str:
        # The Activity gets no Stream, but the run received one.
        return await workflow.execute_activity(
            plain, stream.id, start_to_close_timeout=timedelta(seconds=10)
        )


@workflow.defn
class RetriesAfterContinueAsNew:
    @workflow.run
    async def run(self, key: str, stream: common.WorkflowStream[str] | None) -> None:
        if stream is None:
            created = await workflow.create_stream(Tokens)
            workflow.continue_as_new(args=[key, created])
        await workflow.execute_activity(
            flaky,
            CallModelInput(key, common.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def keyed(handle: WorkflowHandle[Any, Any], activity_type: str) -> list[bool]:
    # Whether each schedule of the Activity has the Producer key.
    history = await handle.fetch_history()
    return [
        "__temporal_stream_key" in attributes.header.fields
        for event in history.events
        if (
            attributes := event.activity_task_scheduled_event_attributes
        ).activity_type.name
        == activity_type
    ]


async def test_only_a_run_that_uses_streams_keys_its_activities(stream_client: Client):
    # The key is in History, so a run with no Stream adds none. A run that created, bound or
    # received a Stream adds it to each Activity, so an Activity that writes keeps its Producer id.
    key = str(uuid.uuid4())
    async with new_worker(
        stream_client,
        NeverTouchesStreams,
        ReceivesAStream,
        Retrying,
        RetriesAfterContinueAsNew,
        activities=[plain, flaky],
    ) as worker:
        none = await stream_client.start_workflow(
            NeverTouchesStreams.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await none.result() == "hello ada"
        assert await keyed(none, "plain") == [False]
        creates = await stream_client.start_workflow(
            Retrying.run, key, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await creates.result()
        assert await keyed(creates, "flaky") == [True]
        receives = await stream_client.start_workflow(
            ReceivesAStream.run,
            common.WorkflowStream.from_handle(workflow_stream(creates, Tokens)),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await receives.result()
        assert await keyed(receives, "plain") == [True]
        # The next run gets the Stream as its argument. Its retried Activity withdraws the first
        # attempt's Records, as in the run that created the Stream.
        continued_key = str(uuid.uuid4())
        continued = await stream_client.start_workflow(
            RetriesAfterContinueAsNew.run,
            args=[continued_key, None],
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await continued.result()
        assert await keyed(continued, "flaky") == [True]
        items = await read(continued)
    superseded = [item for item in items if isinstance(item, common.Superseded)]
    assert len(superseded) == 1
    assert [r.data for r in records(items) if superseded[0].withdraws(r)] == [
        "attempt-1"
    ]
