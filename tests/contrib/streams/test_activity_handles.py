import asyncio
import contextvars
import ctypes
import threading
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib import streams
from temporalio.contrib.streams import _activity
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.exceptions import CancelledError
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.ActivityStream[Events]


@dataclass
class SyncInput:
    prompt: str
    stream: streams.SyncActivityStream[Events]


async def read(
    client: Client, handle: WorkflowHandle[Any, Any], name: str = "events"
) -> list[object]:
    stream = streams.StreamClient(client).workflow_stream(
        handle, streams.with_name(Events, name)
    )
    return [item async for item in stream.subscribe(create_wait=timedelta(seconds=20))]


def records(items: Sequence[object]) -> list[streams.Record[Any]]:
    return [item for item in items if isinstance(item, streams.Record)]


@activity.defn
async def call_model(input: CallModelInput) -> list[int]:
    assert input.stream.definition is Events
    tokens = input.stream.topic(Events.tokens)
    offsets = [await tokens.publish(word) for word in input.prompt.split()]
    async with streams.BufferedPublisher(
        input.stream.topic(Events.turns), max_delay=timedelta(seconds=10)
    ) as turns:
        turns.publish(Turn("model", "a"))
        turns.publish(Turn("model", "b"))
    offsets.append(await input.stream.get_offset())
    return offsets


@activity.defn
def call_model_sync(input: SyncInput) -> int:
    last = input.stream.topic(Events.tokens).publish(*input.prompt.split())
    assert input.stream.get_offset() == last + 1
    return last


@activity.defn
async def read_back(stream: streams.ActivityStream[Events]) -> list[Any]:
    return [
        item.data
        async for item in stream.subscribe(Events.tokens, wait_new_records=False)
        if isinstance(item, streams.Record)
    ]


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> list[Any]:
        stream = await stream_workflow.create_stream(Events)
        offsets = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        last = await workflow.execute_activity(
            call_model_sync,
            SyncInput("sync words", streams.SyncActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        read = await workflow.execute_activity(
            read_back,
            streams.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
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
        items = await read(stream_client, handle)
    data = records(items)
    assert [record.data for record in data] == [
        "hi",
        "there",
        Turn("model", "a"),
        Turn("model", "b"),
        "sync",
        "words",
    ]
    assert offsets[:2] == [data[0].offset, data[1].offset]
    assert offsets[2] == data[3].offset + 1
    assert last == data[5].offset
    assert read_tokens == ["hi", "there", "sync", "words"]
    assert isinstance(items[-1], streams.Closed)


def _executor() -> Any:
    import concurrent.futures

    return concurrent.futures.ThreadPoolExecutor(max_workers=2)


attempts: dict[str, int] = {}


@activity.defn
async def flaky(input: CallModelInput) -> None:
    attempts[input.prompt] = attempts.get(input.prompt, 0) + 1
    if attempts[input.prompt] == 1:
        await input.stream.topic(Events.tokens).publish("attempt-1")
        raise RuntimeError("first attempt fails")


@workflow.defn
class Retrying:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await stream_workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("start")
        await workflow.execute_activity(
            flaky,
            CallModelInput(key, streams.ActivityStream.from_handle(stream)),
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
        items = await read(stream_client, handle)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    # The second attempt writes nothing, and its start still withdraws the first one's Record.
    assert len(superseded) == 1
    withdrawn = [r.data for r in records(items) if superseded[0].withdraws(r)]
    assert withdrawn == ["attempt-1"]


# Set by a test once its readers showed the first attempt's Record.
shown: dict[str, asyncio.Event] = {}


@activity.defn
async def fails_after_it_is_shown(input: CallModelInput) -> None:
    attempts[input.prompt] = attempts.get(input.prompt, 0) + 1
    tokens = input.stream.topic(Events.tokens)
    if attempts[input.prompt] == 1:
        await tokens.publish("attempt-1")
        await shown[input.prompt].wait()
        raise RuntimeError("first attempt fails")
    await tokens.publish("attempt-2")


@workflow.defn
class ShownThenWithdrawn:
    @workflow.run
    async def run(self, key: str) -> None:
        stream = await stream_workflow.create_stream(Events)
        await workflow.execute_activity(
            fails_after_it_is_shown,
            CallModelInput(key, streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


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
        stream = streams.StreamClient(stream_client).workflow_stream(handle, Events)
        live = stream.subscribe(Events.tokens, create_wait=timedelta(seconds=20))
        reconnects = stream.subscribe(Events.tokens, create_wait=timedelta(seconds=20))
        first = await anext(live)
        before = await anext(reconnects)
        # The reader drops its connection and keeps only the Records it showed and where to
        # go on from, as a UI does across a page reload.
        resume_at = reconnects.next_offset
        await reconnects.aclose()
        shown[key].set()
        live_items = [first, *[item async for item in live]]
        resumed = [
            item
            async for item in stream.subscribe(Events.tokens, from_offset=resume_at)
        ]
    assert isinstance(first, streams.Record) and first.data == "attempt-1"
    assert isinstance(before, streams.Record) and before.offset == first.offset
    [live_superseded] = [i for i in live_items if isinstance(i, streams.Superseded)]
    assert live_superseded.withdrawn_offsets == (first.offset,)
    # A new read gave no Record of the failed attempt, so it names no offset. The kept
    # Record is still withdrawn.
    [superseded] = [i for i in resumed if isinstance(i, streams.Superseded)]
    assert superseded.withdrawn_offsets == ()
    assert superseded.withdraws(before)
    assert [r.data for r in records(resumed)] == ["attempt-2"]
    assert isinstance(resumed[-1], streams.Closed)


@activity.defn
async def wrong_backend(stream: streams.ActivityStream[Events]) -> str:
    try:
        await stream.topic(Events.tokens).publish("x")
    except streams.StreamConfigError:
        return "config"
    return "wrote"


@activity.defn
async def close_it(stream: streams.ActivityStream[Events]) -> str:
    await stream.topic(Events.tokens).publish("last")
    try:
        await stream.close(result="closed by activity")
    except streams.StreamNotOwnerError as err:
        return err.reason
    return "closed"


@workflow.defn
class Closing:
    @workflow.run
    async def run(self) -> list[str]:
        stream = await stream_workflow.create_stream(Events, close_with_run=False)
        backend = await workflow.execute_activity(
            wrong_backend,
            streams.ActivityStream(id=stream.id, backend="elsewhere"),
            start_to_close_timeout=timedelta(seconds=30),
        )
        owner = await workflow.execute_activity(
            close_it,
            streams.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
        # A Local Activity is part of the run chain, so it may close the chain's Stream.
        local = await workflow.execute_local_activity(
            close_it,
            streams.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
        return [backend, owner, local]


async def test_activity_checks_the_backend_and_the_owner(stream_client: Client):
    async with new_worker(
        stream_client, Closing, activities=[wrong_backend, close_it]
    ) as worker:
        handle = await stream_client.start_workflow(
            Closing.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        # An Activity writes to the Workflow's Stream but can't end it for every reader.
        assert await handle.result() == ["config", "stream_not_owner", "closed"]
        items = await read(stream_client, handle)
    assert [r.data for r in records(items)] == ["last", "last"]
    assert isinstance(items[-1], streams.Closed)
    assert items[-1].result == "closed by activity"


@workflow.defn
class CreatesAndLeavesOpen:
    @workflow.run
    async def run(self) -> str:
        stream = await stream_workflow.create_stream(Events, close_with_run=False)
        return stream.id


@workflow.defn
class ClosesAnotherChainsStream:
    @workflow.run
    async def run(self, stream_id: str) -> str:
        # The handle has only the id, so the SDK can't tell that another chain owns it.
        return await workflow.execute_local_activity(
            close_it,
            streams.ActivityStream(id=stream_id),
            start_to_close_timeout=timedelta(seconds=30),
        )


async def test_store_refuses_another_chains_close(stream_client: Client):
    async with new_worker(
        stream_client,
        CreatesAndLeavesOpen,
        ClosesAnotherChainsStream,
        activities=[close_it],
    ) as worker:
        owner = await stream_client.start_workflow(
            CreatesAndLeavesOpen.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        stream_id = await owner.result()
        assert (
            await stream_client.execute_workflow(
                ClosesAnotherChainsStream.run,
                stream_id,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "stream_not_owner"
        )
    stream = streams.StreamClient(stream_client).get_handle(Events, id=stream_id)
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [type(item) for item in items] == [streams.Record]


@dataclass
class _Batches(streams.ActivityTopic[int]):
    batches: list[tuple[int, ...]] = field(default_factory=list)
    fail: bool = False
    in_flight: int = 0
    most_in_flight: int = 0

    def __post_init__(self) -> None:
        super().__init__(None, "t", int)

    async def publish(self, *values: int) -> int:
        if self.fail:
            raise streams.StreamStorageError("store down")
        self.in_flight += 1
        self.most_in_flight = max(self.most_in_flight, self.in_flight)
        await asyncio.sleep(0.01)
        self.in_flight -= 1
        self.batches.append(values)
        return sum(len(batch) for batch in self.batches)


async def test_buffered_publisher_batches_by_delay():
    topic = _Batches()
    publisher = streams.BufferedPublisher(topic, max_delay=timedelta(milliseconds=20))
    publisher.publish(1)
    publisher.publish(2, 3)
    await asyncio.sleep(0.1)
    assert topic.batches == [(1, 2, 3)]
    publisher.publish(4)
    assert await publisher.flush() == 4
    assert topic.batches == [(1, 2, 3), (4,)]
    assert await publisher.flush() == 4


async def test_buffered_publisher_writes_a_full_batch_with_one_write_in_flight():
    topic = _Batches()
    publisher = streams.BufferedPublisher(topic, max_delay=timedelta(minutes=10))
    publisher.publish(*range(1000))
    publisher.publish(*range(1000, 2500))
    await asyncio.sleep(0.2)
    # A full batch goes with no wait for the delay, and the values published during its
    # write go in the next one.
    assert [len(batch) for batch in topic.batches] == [1000, 1500]
    assert topic.most_in_flight == 1
    assert await publisher.flush() == 2500


async def test_buffered_publisher_raises_a_failed_batch():
    topic = _Batches(fail=True)
    with pytest.raises(streams.StreamStorageError):
        async with streams.BufferedPublisher(topic) as publisher:
            publisher.publish(1)
    with pytest.raises(TypeError):
        streams.BufferedPublisher(topic).publish()


@activity.defn
async def local_step(stream: streams.ActivityStream[Events]) -> list[Any]:
    # Each Workflow write is an awaited Local Activity, so the run's create and Records are in
    # the Store before a later Local Activity starts.
    seen = [
        item.data
        async for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, streams.Record)
    ]
    await stream.topic(Events.tokens).publish("local")
    return seen


@workflow.defn
class LocalOrder:
    @workflow.run
    async def run(self) -> list[Any]:
        stream = await stream_workflow.create_stream(Events)
        tokens = stream.topic(Events.tokens)
        await tokens.publish("before")
        seen = await workflow.execute_local_activity(
            local_step,
            streams.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await tokens.publish("after")
        return seen


async def test_workflow_writes_go_out_before_a_local_activity(stream_client: Client):
    async with new_worker(stream_client, LocalOrder, activities=[local_step]) as worker:
        handle = await stream_client.start_workflow(
            LocalOrder.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == ["before"]
        items = await read(stream_client, handle)
    written = records(items)
    assert [record.data for record in written] == ["before", "local", "after"]
    producer = written[1].producer
    assert producer is not None and producer.id.startswith("la:")
    assert isinstance(items[-1], streams.Closed)


async def test_buffered_publisher_stays_failed():
    topic = _Batches(fail=True)
    publisher = streams.BufferedPublisher(topic)
    publisher.publish(1, 2)
    with pytest.raises(streams.StreamStorageError):
        await publisher.flush()
    topic.fail = False
    # A later batch would leave a gap, so the publisher refuses it and keeps the error.
    with pytest.raises(streams.StreamStorageError):
        publisher.publish(3)
    with pytest.raises(streams.StreamStorageError):
        await publisher.flush()
    assert topic.batches == [] and publisher.unwritten == 2


@activity.defn
async def never_flushes(input: CallModelInput) -> None:
    publisher = streams.BufferedPublisher(
        input.stream.topic(Events.tokens), max_delay=timedelta(minutes=10)
    )
    publisher.publish(*input.prompt.split())


@dataclass
class ManyStreams:
    key: str
    targets: list[streams.ActivityStream[Events]]
    maybe: streams.ActivityStream[Events] | None = None


@activity.defn
async def flaky_many(input: ManyStreams) -> None:
    attempts[input.key] = attempts.get(input.key, 0) + 1
    assert input.maybe is not None
    if attempts[input.key] == 1:
        for stream in [*input.targets, input.maybe]:
            await stream.topic(Events.tokens).publish("attempt-1")
        raise RuntimeError("first attempt fails")


@activity.defn
def sync_read(stream: streams.SyncActivityStream[Events]) -> list[Any]:
    return [
        item.data
        for item in stream.subscribe(Events.tokens, wait_new_records=False)
        if isinstance(item, streams.Record)
    ]


@workflow.defn
class Leftovers:
    @workflow.run
    async def run(self, key: str) -> list[Any]:
        stream = await stream_workflow.create_stream(Events)
        other = await stream_workflow.create_stream(streams.with_name(Events, "other"))
        await workflow.execute_activity(
            never_flushes,
            CallModelInput("a b", streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            flaky_many,
            ManyStreams(
                key,
                [streams.ActivityStream.from_handle(stream)],
                streams.ActivityStream.from_handle(other),
            ),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )
        return await workflow.execute_activity(
            sync_read,
            streams.SyncActivityStream.from_handle(stream),
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
        items = await read(stream_client, handle)
        other = await read(stream_client, handle, "other")
    for read_items in (items, other):
        superseded = [i for i in read_items if isinstance(i, streams.Superseded)]
        withdrawn = [r.data for r in records(read_items) if superseded[0].withdraws(r)]
        assert withdrawn == ["attempt-1"]


def test_sync_call_stops_when_the_thread_is_cancelled():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    started = threading.Event()
    stopped = threading.Event()

    async def long_poll() -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            stopped.set()

    stream: streams.SyncActivityStream[Any] = streams.SyncActivityStream(id="s")
    token = _activity.current.set(
        cast(Any, SimpleNamespace(loop=loop, backend_name=""))
    )
    errors: list[BaseException] = []

    def call() -> None:
        try:
            stream._run(long_poll())
        except BaseException as err:
            errors.append(err)

    caller = threading.Thread(target=contextvars.copy_context().run, args=(call,))
    _activity.current.reset(token)
    caller.start()
    assert started.wait(5)
    # The Worker cancels a sync Activity by raising in its thread.
    assert caller.ident is not None
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(caller.ident), ctypes.py_object(CancelledError)
    )
    caller.join(5)
    assert stopped.wait(5)
    assert errors and isinstance(errors[0], CancelledError)
    loop.call_soon_threadsafe(loop.stop)
