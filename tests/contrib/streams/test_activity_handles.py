import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
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


async def read(handle: WorkflowHandle[Any, Any]) -> list[object]:
    stream = streams.for_handle(
        handle, name="events", wait_for_create=timedelta(seconds=20)
    )
    stream.topic("turns", type=Turn)
    stream.topic("tokens", type=str)
    return [item async for item in stream.subscribe(idle_timeout=timedelta(seconds=20))]


def records(items: list[object]) -> list[streams.Record[Any]]:
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
        items = await read(handle)
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
        items = await read(handle)
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    # The second attempt writes nothing, and its start still withdraws the first one's Record.
    assert len(superseded) == 1
    withdrawn = [r.data for r in records(items) if superseded[0].withdraws(r)]
    assert withdrawn == ["attempt-1"]


@activity.defn
async def wrong_backend(stream: streams.ActivityStream[Events]) -> str:
    try:
        await stream.topic(Events.tokens).publish("x")
    except streams.StreamUnsupportedError as err:
        return err.reason
    return "wrote"


@activity.defn
async def close_it(stream: streams.ActivityStream[Events]) -> None:
    await stream.topic(Events.tokens).publish("last")
    await stream.close(result="closed by activity")


@workflow.defn
class Closing:
    @workflow.run
    async def run(self) -> str:
        stream = await stream_workflow.create_stream(Events, close_with_run=False)
        reason = await workflow.execute_activity(
            wrong_backend,
            streams.ActivityStream(id=stream.id, backend="elsewhere"),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await workflow.execute_activity(
            close_it,
            streams.ActivityStream.from_handle(stream),
            start_to_close_timeout=timedelta(seconds=30),
        )
        return reason


async def test_activity_closes_and_checks_the_backend(stream_client: Client):
    async with new_worker(
        stream_client, Closing, activities=[wrong_backend, close_it]
    ) as worker:
        handle = await stream_client.start_workflow(
            Closing.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "stream_unsupported"
        items = await read(handle)
    assert [r.data for r in records(items)] == ["last"]
    assert isinstance(items[-1], streams.Closed)
    assert items[-1].result == "closed by activity"


@dataclass
class _Batches(streams.ActivityTopic[int]):
    batches: list[tuple[int, ...]] = field(default_factory=list)
    fail: bool = False

    def __post_init__(self) -> None:
        super().__init__(None, "t", int)

    async def publish(self, *values: int) -> int:
        if self.fail:
            raise streams.StreamStorageError("store down")
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


async def test_buffered_publisher_raises_a_failed_batch():
    topic = _Batches(fail=True)
    with pytest.raises(streams.StreamStorageError):
        async with streams.BufferedPublisher(topic) as publisher:
            publisher.publish(1)
    with pytest.raises(TypeError):
        streams.BufferedPublisher(topic).publish()
