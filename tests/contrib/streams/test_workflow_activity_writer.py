"""A Workflow publishes to its own stream through the built-in Activity.

The live tests run on memory, and on Redis when ``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RawValue, RetryPolicy
from temporalio.contrib.streams import (
    RecordKind,
    StreamError,
    StreamOutcomeUnknownError,
    StreamProducer,
    StreamStorePlugin,
    get_stream_handle,
    topic,
    workflow_activity_writer,
)
from temporalio.contrib.streams._workflow_activity import (
    PUBLISH_ACTIVITY,
    publish_activity,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Worker
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
from tests.helpers import new_worker

EVENTS = topic("events", dict)
FAST_RETRY = RetryPolicy(initial_interval=timedelta(milliseconds=50))


def redis_store() -> StreamStorePlugin:
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return RedisStreams(url, key_prefix=f"activity-writer-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis"])
async def streams(request: pytest.FixtureRequest, client: Client) -> Client:
    store = MemoryStreams() if request.param == "memory" else redis_store()
    return await connect_with(client, store)


@workflow.defn
class Publishes:
    @workflow.run
    async def run(self) -> list[str]:
        writer = workflow_activity_writer(EVENTS, retry_policy=FAST_RETRY)
        cursors = [await writer.publish({"step": "one"})]
        cursors.append(await writer.publish_batch([{"step": "two"}, {"step": "three"}]))
        # An empty batch schedules nothing and answers the last cursor.
        cursors.append(await writer.publish_batch([]))
        cursors.append(await writer.finish())
        # Finishing again is a no-op.
        cursors.append(await writer.finish())
        out = [c.token for c in cursors]
        try:
            await writer.publish({"step": "late"})
        except ValueError as error:
            out.append(str(error))
        return out


async def test_a_workflow_publishes_in_order_and_a_client_reads(streams: Client):
    workflow_id = new_workflow_id()
    async with new_worker(streams, Publishes) as worker:
        handle = await streams.start_workflow(
            Publishes.run, id=workflow_id, task_queue=worker.task_queue
        )
        cursors = await handle.result()
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=EVENTS))
    assert [r.kind for r in records] == [RecordKind.DATA] * 3 + [RecordKind.FINISH]
    assert [r.value for r in records[:3]] == [
        {"step": "one"},
        {"step": "two"},
        {"step": "three"},
    ]
    # Each call is one Activity, which writes as itself in its first attempt.
    run_id = handle.result_run_id
    producers = [r.producer_id for r in records]
    assert all(p.endswith(f"@{run_id}") for p in producers)
    assert len(set(producers)) == 3
    assert [(r.attempt, r.sequence) for r in records] == [
        (1, 1),
        (1, 1),
        (1, 2),
        (1, 1),
    ]
    # A call answers the cursor of its last record.
    one, batch, empty, finish, again, late = cursors
    assert one == records[0].cursor.token
    assert batch == empty == records[2].cursor.token
    assert finish == again == records[3].cursor.token
    assert "already finished" in late


@workflow.defn
class PublishesConcurrently:
    @workflow.run
    async def run(self, count: int) -> None:
        # Two writers on one topic still go in call order.
        first = workflow_activity_writer(EVENTS, retry_policy=FAST_RETRY)
        second = workflow_activity_writer(EVENTS, retry_policy=FAST_RETRY)
        await asyncio.gather(
            *(
                (first if index % 2 else second).publish({"i": index})
                for index in range(count)
            )
        )


async def test_concurrent_calls_on_a_topic_keep_their_order(client: Client):
    streams = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesConcurrently) as worker:
        await streams.execute_workflow(
            PublishesConcurrently.run, 6, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=EVENTS))
    assert [r.value for r in records] == [{"i": index} for index in range(6)]


@workflow.defn
class PublishesOnce:
    @workflow.run
    async def run(self) -> None:
        writer = workflow_activity_writer(EVENTS, retry_policy=FAST_RETRY)
        await writer.publish_batch([{"step": "one"}, {"step": "two"}])


async def test_an_activity_retry_is_superseded_and_read_once(
    streams: Client, monkeypatch: pytest.MonkeyPatch
):
    append = StreamProducer.append

    async def append_then_fail(self: StreamProducer[Any], *values: Any) -> Any:
        cursor = await append(self, *values)
        if activity.info().attempt == 1:
            raise RuntimeError("the Worker lost the answer after the append")
        return cursor

    monkeypatch.setattr(StreamProducer, "append", append_then_fail)
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesOnce) as worker:
        await streams.execute_workflow(
            PublishesOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=EVENTS))
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.SUPERSEDED,
        RecordKind.DATA,
        RecordKind.DATA,
    ]
    # A reader that drops a superseded attempt's records sees each value once.
    kept: list[Any] = []
    for record in records:
        if record.kind is RecordKind.SUPERSEDED:
            kept.clear()
        else:
            kept.append(record.value)
    assert kept == [{"step": "one"}, {"step": "two"}]
    assert {r.attempt for r in records[3:]} == {2}


async def test_an_unknown_outcome_is_repeated_and_written_once(
    streams: Client, monkeypatch: pytest.MonkeyPatch
):
    append = StreamProducer.append
    calls: list[int] = []

    async def append_then_lose(self: StreamProducer[Any], *values: Any) -> Any:
        sequence = self._sequence
        cursor = await append(self, *values)
        calls.append(len(values))
        if len(calls) == 1:
            # As when the store's answer is lost: the producer keeps its sequence.
            self._sequence = sequence
            raise StreamOutcomeUnknownError("the answer was lost")
        return cursor

    monkeypatch.setattr(StreamProducer, "append", append_then_lose)
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesOnce) as worker:
        await streams.execute_workflow(
            PublishesOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=EVENTS))
    # The repeat went to the store, which wrote the batch once, in attempt 1.
    assert calls == [2, 2]
    assert [r.value for r in records] == [{"step": "one"}, {"step": "two"}]
    assert {r.attempt for r in records} == {1}


@workflow.defn
class PublishesTooMuch:
    @workflow.run
    async def run(self) -> str:
        try:
            await workflow_activity_writer(EVENTS).publish({"pad": "x" * (2 << 20)})
        except StreamError as error:
            return str(error)
        return "published"


async def test_an_oversized_value_fails_at_the_call(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, PublishesTooMuch) as worker:
        message = await streams.execute_workflow(
            PublishesTooMuch.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
    assert "per-record limit" in message


async def test_a_worker_registers_the_publish_activity_once(client: Client):
    store = MemoryStreams()
    streams = await connect_with(client, store)
    worker = Worker(streams, task_queue=str(uuid.uuid4()), workflows=[Publishes])
    assert worker.config(active_config=True).get("activities") == [publish_activity]
    with pytest.warns(UserWarning, match="same plugin type"):
        worker = Worker(
            streams,
            task_queue=str(uuid.uuid4()),
            workflows=[Publishes],
            plugins=[store],
        )
    assert worker.config(active_config=True).get("activities") == [publish_activity]
    assert PUBLISH_ACTIVITY == "__temporal_streams_publish"


async def test_the_activity_refuses_input_that_is_not_a_payloads_message():
    with pytest.raises(ApplicationError, match="not a Payloads message") as raised:
        await ActivityEnvironment().run(
            publish_activity, "events", False, RawValue(Payload(data=b"x"))
        )
    assert raised.value.non_retryable
