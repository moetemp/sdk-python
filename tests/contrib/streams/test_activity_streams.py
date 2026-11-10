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

from temporalio import activity, streams, workflow
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import (
    BufferedAppender,
    Client,
    Stream,
    SyncStream,
    WorkflowHandle,
)
from temporalio.common import RetryPolicy
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError
from temporalio.streams import _activity, _activity_stream
from temporalio.streams.superseded import Superseded, with_superseded
from temporalio.streams.system import models
from tests.contrib.streams._support import stream_status
from tests.helpers import new_worker


def own_id(name: str = "tokens") -> str:
    # A Workflow picks the id of each Stream that it creates. These tests use its Workflow ID.
    return f"{workflow.info().workflow_id}-{name}"


def reason(err: streams.StreamError) -> str:
    return StreamErrorReason.Name(StreamErrorReason.ValueType(err.reason))


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.StreamRef[str]


@dataclass
class SyncInput:
    prompt: str
    stream: streams.StreamRef[str]


async def read(handle: WorkflowHandle[Any, Any], name: str = "tokens") -> list[object]:
    stream = handle._client.get_stream(f"{handle.id}-{name}", payload_type=str)
    return [item async for item in stream.read(wait_new_records=False)]


async def read_superseded(
    handle: WorkflowHandle[Any, Any], name: str = "tokens"
) -> list[object]:
    # The opt-in library adds the Superseded items that an Activity retry withdraws.
    stream = handle._client.get_stream(f"{handle.id}-{name}", payload_type=str)
    return [item async for item in with_superseded(stream.read(wait_new_records=False))]


async def closed(handle: WorkflowHandle[Any, Any], name: str = "tokens") -> bool:
    status = await stream_status(handle._client, f"{handle.id}-{name}")
    return status == models.StreamStatus.StreamStatusClosed


def records(items: Sequence[object]) -> list[streams.Record[Any]]:
    return [item for item in items if isinstance(item, streams.Record)]


@activity.defn
async def call_model(input: CallModelInput) -> list[str]:
    stream = activity.client().get_stream(input.stream)
    offsets = [str(await stream.append(word)) for word in input.prompt.split()]
    async with BufferedAppender(stream, max_delay=timedelta(seconds=10)) as buffered:
        buffered.append("model-a")
        buffered.append("model-b")
    offsets.append(str(await stream.get_offset()))
    return offsets


class SyncActivities:
    # A sync Activity has no activity.client(), so it keeps the Worker's client.
    def __init__(self, client: Client) -> None:
        self._client = client

    @activity.defn
    def call_model_sync(self, input: SyncInput) -> str:
        stream = SyncStream(self._client.get_stream(input.stream))
        last = stream.append(*input.prompt.split())
        assert stream.get_offset() == last
        return str(last)

    @activity.defn
    def appends_without_the_sync_stream(self, ref: streams.StreamRef[str]) -> str:
        try:
            self._client.get_stream(ref).append("x")
        except streams.StreamError as err:
            return reason(err)
        return "wrote"

    @activity.defn
    def sync_read(self, ref: streams.StreamRef[str]) -> list[Any]:
        stream = SyncStream(self._client.get_stream(ref))
        return [
            item.data
            for item in stream.read(wait_new_records=False)
            if isinstance(item, streams.Record)
        ]


@activity.defn
async def read_back(ref: streams.StreamRef[str]) -> list[Any]:
    stream = activity.client().get_stream(ref)
    return [
        item.data
        async for item in stream.read(wait_new_records=False)
        if isinstance(item, streams.Record)
    ]


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> list[Any]:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        offsets = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, stream.ref),
            start_to_close_timeout=timedelta(seconds=30),
        )
        last = await workflow.execute_activity_method(
            SyncActivities.call_model_sync,
            SyncInput("sync words", stream.ref),
            start_to_close_timeout=timedelta(seconds=30),
        )
        read = await workflow.execute_activity(
            read_back,
            stream.ref,
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.close()
        return [offsets, last, read]


async def test_activity_appends_and_returns_offsets(stream_client: Client):
    async with new_worker(
        stream_client,
        Agent,
        activities=[
            call_model,
            SyncActivities(stream_client).call_model_sync,
            read_back,
        ],
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
    assert offsets[:2] == [str(data[0].offset), str(data[1].offset)]
    assert streams.StreamOffset.parse(offsets[2]) == data[3].offset
    assert streams.StreamOffset.parse(last) == data[5].offset
    assert read_tokens == written
    assert await closed(handle)


def _executor() -> Any:
    import concurrent.futures

    return concurrent.futures.ThreadPoolExecutor(max_workers=2)


attempts: dict[str, int] = {}


@activity.defn
async def flaky(input: CallModelInput) -> None:
    attempts[input.prompt] = attempts.get(input.prompt, 0) + 1
    if attempts[input.prompt] == 1:
        await activity.client().get_stream(input.stream).append("attempt-1")
        raise RuntimeError("first attempt fails")


@workflow.defn
class Retrying:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("start")
        await workflow.execute_activity(
            flaky,
            CallModelInput(key, stream.ref),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


@dataclass
class FinishedStreams:
    key: str
    closed: streams.StreamRef[str]
    expired: streams.StreamRef[str]


@activity.defn
async def reads_after_a_failed_attempt(input: FinishedStreams) -> list[Any]:
    attempts[input.key] = attempts.get(input.key, 0) + 1
    if attempts[input.key] == 1:
        raise RuntimeError("first attempt fails")
    items = activity.client().get_stream(input.closed).read(wait_new_records=False)
    return [item.data async for item in items if isinstance(item, streams.Record)]


@workflow.defn
class ReadsFinishedStreams:
    @workflow.run
    async def run(self, key: str, expired: streams.StreamRef[str]) -> list[Any]:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("turn")
        await stream.close()
        return await workflow.execute_activity(
            reads_after_a_failed_attempt,
            FinishedStreams(
                key,
                stream.ref,
                expired,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def test_retry_goes_on_with_a_closed_or_expired_stream(client: Client):
    # On a Stream that accepts no write, the second attempt's open withdraws nothing, and the
    # attempt only reads. Retention is a backend setting, so the aged Stream expires while the
    # closed one, created fresh in the run, is still within Retention.
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(retention=timedelta(seconds=2), default=True)
    ]
    memory_client = Client(**config)
    expired = await memory_client.create_stream(
        f"expired-{uuid.uuid4()}", payload_type=str
    )
    await expired.append("old")
    await asyncio.sleep(3)
    key = str(uuid.uuid4())
    async with new_worker(
        memory_client, ReadsFinishedStreams, activities=[reads_after_a_failed_attempt]
    ) as worker:
        result = await memory_client.execute_workflow(
            ReadsFinishedStreams.run,
            args=[key, expired.ref],
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert result == ["turn"] and attempts[key] == 2


# Set by a test after its readers showed the first attempt's Record.
shown: dict[str, asyncio.Event] = {}


@activity.defn
async def fails_after_it_is_shown(input: CallModelInput) -> None:
    attempts[input.prompt] = attempts.get(input.prompt, 0) + 1
    tokens = activity.client().get_stream(input.stream)
    if attempts[input.prompt] == 1:
        await tokens.append("attempt-1")
        await shown[input.prompt].wait()
        raise RuntimeError("first attempt fails")
    await tokens.append("attempt-2")


@workflow.defn
class ShownThenWithdrawn:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await workflow.execute_activity(
            fails_after_it_is_shown,
            CallModelInput(key, stream.ref),
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
        stream = stream_client.get_stream(f"{handle.id}-tokens", payload_type=str)
        live = with_superseded(stream.read(create_wait=timedelta(seconds=20)))
        reconnects = stream.read(create_wait=timedelta(seconds=20))
        first = await anext(live)
        before = await anext(reconnects)
        # The reader closes its connection and keeps only the Records that it showed and its
        # resume offset, as a UI does across a page reload.
        resume_at = reconnects.next_offset
        await reconnects.aclose()
        shown[key].set()
        live_items = [first, *[item async for item in live]]
        resumed = [
            item async for item in with_superseded(stream.read(offset=resume_at))
        ]
    assert isinstance(first, streams.Record) and first.data == "attempt-1"
    assert isinstance(before, streams.Record) and before.offset == first.offset
    [live_superseded] = [i for i in live_items if isinstance(i, Superseded)]
    assert live_superseded.withdrawn_offsets == (first.offset,)
    # A fresh read after the failed attempt has no Record of it, so it adds no Superseded item.
    # A reader that kept the shown Record checks it with withdraws().
    assert live_superseded.withdraws(before)
    assert not any(isinstance(i, Superseded) for i in resumed)
    assert [r.data for r in records(resumed)] == ["attempt-2"]


@activity.defn
async def wrong_backend(ref: streams.StreamRef[str]) -> str:
    try:
        await activity.client().get_stream(ref).append("x")
    except streams.StreamConfigError:
        return "config"
    return "wrote"


@activity.defn
async def close_it(ref: streams.StreamRef[str]) -> str:
    stream = activity.client().get_stream(ref)
    await stream.append("last")
    try:
        await stream.close()
    except streams.StreamError as err:
        return reason(err)
    return "closed"


@workflow.defn
class Closing:
    @workflow.run
    async def run(self) -> list[str]:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        backend = await workflow.execute_activity(
            wrong_backend,
            streams.StreamRef[str](id=stream.ref.id, backend="elsewhere"),
            start_to_close_timeout=timedelta(seconds=30),
        )
        owner = await workflow.execute_activity(
            close_it,
            stream.ref,
            start_to_close_timeout=timedelta(seconds=30),
        )
        # A Local Activity runs before its Workflow Task commits, so it can't close it either.
        local = await workflow.execute_local_activity(
            close_it,
            stream.ref,
            start_to_close_timeout=timedelta(seconds=30),
        )
        # A ref with only the id has no owner, and still no Activity can close the Stream.
        by_id = await workflow.execute_local_activity(
            close_it,
            streams.StreamRef[str](id=stream.ref.id),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.close()
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
    assert await closed(handle)


@activity.defn
async def refused_calls(ref: streams.StreamRef[str]) -> list[str]:
    stream = activity.client().get_stream(ref)
    refused = []
    try:
        async with stream:
            refused.append("entered")
    except streams.StreamError as err:
        refused.append(reason(err))
    return refused


@workflow.defn
class RefusedCalls:
    @workflow.run
    async def run(self) -> list[str]:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        refused = await workflow.execute_activity(
            refused_calls, stream.ref, start_to_close_timeout=timedelta(seconds=30)
        )
        refused.append(
            await workflow.execute_activity_method(
                SyncActivities.appends_without_the_sync_stream,
                stream.ref,
                start_to_close_timeout=timedelta(seconds=30),
            )
        )
        try:
            # Workflow code has no Stream, so this is for a caller that has no type checks.
            BufferedAppender(cast(Any, stream))
        except streams.StreamError as err:
            refused.append(reason(err))
        await stream.close()
        return refused


async def test_calls_that_an_activity_cant_make_raise(stream_client: Client):
    async with new_worker(
        stream_client,
        RefusedCalls,
        activities=[
            refused_calls,
            SyncActivities(stream_client).appends_without_the_sync_stream,
        ],
        activity_executor=_executor(),
    ) as worker:
        handle = await stream_client.start_workflow(
            RefusedCalls.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == [
            "STREAM_ERROR_REASON_NOT_OWNER",
            *["STREAM_ERROR_REASON_UNSUPPORTED"] * 2,
        ]
        items = await read(handle)
    assert records(items) == []


@activity.defn
async def leaves_a_failed_write(ref: streams.StreamRef[str]) -> str:
    # Nobody created the Stream, and no caller waits for the write.
    activity.client().get_stream(ref).append("lost")
    return "returned"


@activity.defn
async def enters_a_client_stream(ref: streams.StreamRef[str]) -> str:
    stream = activity.client().get_stream(ref)
    try:
        async with stream:
            await stream.append("in the block")
    except streams.StreamError as err:
        return reason(err)
    return "closed"


@workflow.defn
class LostAndClientWrites:
    @workflow.run
    async def run(
        self, missing_id: str, client_stream: streams.StreamRef[str]
    ) -> list[str]:
        try:
            lost = await workflow.execute_activity(
                leaves_a_failed_write,
                streams.StreamRef[str](id=missing_id),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        except ActivityError as err:
            assert isinstance(err.cause, ApplicationError)
            # The type is always `StreamError`, so the reason in the details tells the cases apart.
            (details,) = err.cause.details
            lost = f"{err.cause.type}/{StreamErrorReason.Name(details.reason)}"
        closed = await workflow.execute_activity(
            enters_a_client_stream,
            client_stream,
            start_to_close_timeout=timedelta(seconds=30),
        )
        return [lost, closed]


async def test_attempt_end_raises_a_lost_write_and_a_client_stream_refuses_the_block(
    stream_client: Client,
):
    stream = await stream_client.create_stream(
        f"client-{uuid.uuid4()}", payload_type=str
    )
    async with new_worker(
        stream_client,
        LostAndClientWrites,
        activities=[leaves_a_failed_write, enters_a_client_stream],
    ) as worker:
        result = await stream_client.execute_workflow(
            LostAndClientWrites.run,
            args=[f"missing-{uuid.uuid4()}", stream.ref],
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    # An Activity closes no Stream, so the block fails before it runs, with no call.
    assert result == [
        "StreamError/STREAM_ERROR_REASON_NOT_FOUND",
        "STREAM_ERROR_REASON_NOT_OWNER",
    ]
    items = [item async for item in stream.read(wait_new_records=False)]
    assert records(items) == []


def _offset(n: int) -> streams.StreamOffset:
    return streams.StreamOffset(n.to_bytes(8, "big"))


class _Batches(Stream[int]):
    # A Stream whose writes go to a list, so the batches show with no backend.
    def __init__(self, fail: bool = False) -> None:
        super().__init__(streams.StreamRef(id="batches"), None, cast(Any, None))
        self.batches: list[tuple[int, ...]] = []
        self.fail = fail
        self.in_flight = 0
        self.most_in_flight = 0

    def append(
        self, *values: int, **options: Any
    ) -> asyncio.Future[streams.StreamOffset]:
        return asyncio.ensure_future(self._write(values))

    async def _write(self, values: Sequence[int]) -> streams.StreamOffset:
        if self.fail:
            raise streams.StreamError(
                "store down",
                reason=StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE,
            )
        self.in_flight += 1
        self.most_in_flight = max(self.most_in_flight, self.in_flight)
        await asyncio.sleep(0.01)
        self.in_flight -= 1
        self.batches.append(tuple(values))
        return _offset(sum(len(batch) for batch in self.batches))


async def test_buffered_appender_batches_by_delay():
    stream = _Batches()
    appender = BufferedAppender(stream, max_delay=timedelta(milliseconds=20))
    appender.append(1)
    appender.append(2, 3)
    await asyncio.sleep(0.1)
    assert stream.batches == [(1, 2, 3)]
    appender.append(4)
    assert await appender.flush() == _offset(4)
    assert stream.batches == [(1, 2, 3), (4,)]
    assert await appender.flush() == _offset(4)


async def test_buffered_appender_writes_a_full_batch_with_one_write_in_flight():
    stream = _Batches()
    appender = BufferedAppender(stream, max_delay=timedelta(minutes=10))
    appender.append(*range(1000))
    appender.append(*range(1000, 2500))
    await asyncio.sleep(0.2)
    # A full batch is written without waiting for the delay. The values appended during its
    # write are in the next batch.
    assert [len(batch) for batch in stream.batches] == [1000, 1500]
    assert stream.most_in_flight == 1
    assert await appender.flush() == _offset(2500)


async def test_buffered_appender_raises_a_failed_batch():
    stream = _Batches(fail=True)
    with pytest.raises(streams.StreamError):
        async with BufferedAppender(stream) as appender:
            appender.append(1)
    with pytest.raises(TypeError):
        BufferedAppender(stream).append()


@activity.defn
async def local_step(ref: streams.StreamRef[str]) -> list[Any]:
    # Each Workflow write is an awaited Local Activity, so the run's create and Records are in
    # the Store before a later Local Activity starts.
    stream = activity.client().get_stream(ref)
    seen = [
        item.data
        async for item in stream.read(wait_new_records=False)
        if isinstance(item, streams.Record)
    ]
    await stream.append("local")
    return seen


@workflow.defn
class LocalOrder:
    @workflow.run
    async def run(self) -> list[Any]:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await stream.append("before")
        seen = await workflow.execute_local_activity(
            local_step,
            stream.ref,
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.append("after")
        await stream.close()
        return seen


class TwoClients:
    def __init__(self, other: Client) -> None:
        self._other = other

    @activity.defn
    async def writes_with_both(self, ref: streams.StreamRef[str]) -> None:
        await activity.client().get_stream(ref).append("worker client")
        await self._other.get_stream(ref).append("other client")


@workflow.defn
class WritesWithTwoClients:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        await workflow.execute_activity_method(
            TwoClients.writes_with_both,
            stream.ref,
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.close()


async def test_only_the_workers_client_writes_as_the_attempt(stream_client: Client):
    # Another client in the Activity writes as that client, so a retry doesn't withdraw it.
    other = Client(**stream_client.config())
    async with new_worker(
        stream_client,
        WritesWithTwoClients,
        activities=[TwoClients(other).writes_with_both],
    ) as worker:
        handle = await stream_client.start_workflow(
            WritesWithTwoClients.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await read(handle)
    kinds = {r.data: type(r.producer.kind) for r in records(items) if r.producer}
    assert kinds == {
        "worker client": streams.ProducerKindActivity,
        "other client": streams.ProducerKindClient,
    }


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
    assert await closed(handle)


async def test_buffered_appender_stays_failed():
    stream = _Batches(fail=True)
    appender = BufferedAppender(stream)
    appender.append(1, 2)
    with pytest.raises(streams.StreamError):
        await appender.flush()
    stream.fail = False
    # A later batch would leave a gap, so the appender refuses it and keeps the error.
    with pytest.raises(streams.StreamError):
        appender.append(3)
    with pytest.raises(streams.StreamError):
        await appender.flush()
    assert stream.batches == [] and appender.unwritten == 2


@activity.defn
async def never_flushes(input: CallModelInput) -> None:
    stream = activity.client().get_stream(input.stream)
    appender = BufferedAppender(stream, max_delay=timedelta(minutes=10))
    appender.append(*input.prompt.split())


@dataclass
class ManyStreams:
    key: str
    targets: list[streams.StreamRef[str]]
    maybe: streams.StreamRef[str] | None = None


@activity.defn
async def flaky_many(input: ManyStreams) -> None:
    attempts[input.key] = attempts.get(input.key, 0) + 1
    assert input.maybe is not None
    if attempts[input.key] == 1:
        for ref in [*input.targets, input.maybe]:
            await activity.client().get_stream(ref).append("attempt-1")
        raise RuntimeError("first attempt fails")


@workflow.defn
class Leftovers:
    @workflow.run
    async def run(self, key: str) -> list[Any]:
        stream = await workflow.create_stream(own_id(), payload_type=str)
        other = await workflow.create_stream(own_id("other"), payload_type=str)
        await workflow.execute_activity(
            never_flushes,
            CallModelInput("a b", stream.ref),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            flaky_many,
            ManyStreams(
                key,
                [stream.ref],
                other.ref,
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )
        return await workflow.execute_activity_method(
            SyncActivities.sync_read,
            stream.ref,
            start_to_close_timeout=timedelta(seconds=30),
        )


async def test_attempt_end_flushes_and_retry_finds_nested_streams(
    stream_client: Client,
):
    key = str(uuid.uuid4())
    async with new_worker(
        stream_client,
        Leftovers,
        activities=[never_flushes, flaky_many, SyncActivities(stream_client).sync_read],
        activity_executor=_executor(),
    ) as worker:
        handle = await stream_client.start_workflow(
            Leftovers.run, key, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        # The appender was never flushed, so the end of the attempt wrote its values.
        assert await handle.result() == ["a", "b", "attempt-1"]
        items = await read(handle)
        other = await read(handle, "other")
    # The retry writes nothing, so the failed attempt's Records stay visible on both Streams.
    assert "attempt-1" in [r.data for r in records(items)]
    assert [r.data for r in records(other)] == ["attempt-1"]


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
    token = _activity.current.set(cast(Any, SimpleNamespace(loop=loop)))

    def run() -> None:
        try:
            _activity_stream.run_blocking(call)
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
            # Raise only while the caller waits on the call's future, so the test checks
            # run_blocking and not where the scheduler happens to stop the caller thread.
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
    async def run(self, ref: streams.StreamRef[str]) -> str:
        # The Activity gets no Stream, but the run received one.
        return await workflow.execute_activity(
            plain, ref.id, start_to_close_timeout=timedelta(seconds=10)
        )


@workflow.defn
class RetriesAfterContinueAsNew:
    @workflow.run
    async def run(self, key: str, ref: streams.StreamRef[str] | None) -> None:
        if ref is None:
            created = await workflow.create_stream(own_id(), payload_type=str)
            workflow.continue_as_new(args=[key, created.ref])
        await workflow.execute_activity(
            flaky,
            CallModelInput(key, ref),
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
            streams.StreamRef[str](id=f"{creates.id}-tokens"),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await receives.result()
        assert await keyed(receives, "plain") == [True]
        # The next run gets the Stream as its argument, so its Activities carry the Producer key
        # too, as in the run that created the Stream.
        continued_key = str(uuid.uuid4())
        continued = await stream_client.start_workflow(
            RetriesAfterContinueAsNew.run,
            args=[continued_key, None],
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await continued.result()
        assert await keyed(continued, "flaky") == [True]
