"""A Workflow publishes to its own stream through buffered Local Activities.

The live tests run on memory, and on Redis when ``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import shutil
import subprocess
import time
import urllib.parse
import uuid
from collections import defaultdict
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.bridge.streams_generated import StreamService
from temporalio.client import (
    Client,
    WorkflowFailureError,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
)
from temporalio.common import RawValue
from temporalio.contrib.streams import (
    RecordKind,
    StreamError,
    StreamOutcomeUnknownError,
    StreamProducer,
    StreamStorePlugin,
    StreamUnsupportedError,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams._workflow import read_activity
from temporalio.contrib.streams._workflow_writer import (
    PUBLISH_ACTIVITY,
    _FlushGate,
    _RunWriters,
    close_activity,
    publish_activity,
    run_then_drain,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
from tests.helpers import new_worker

STEPS = topic("steps", str)
# The Workflows that count their runs in this process run unsandboxed.
UNSANDBOXED = UnsandboxedWorkflowRunner()


def redis_store() -> StreamStorePlugin:
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return RedisStreams(url, key_prefix=f"workflow-writer-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis"])
async def streams(request: pytest.FixtureRequest, client: Client) -> Client:
    store = MemoryStreams() if request.param == "memory" else redis_store()
    return await connect_with(client, store)


def markers(history: object) -> int:
    return sum(
        1
        for e in getattr(history, "events")
        if e.HasField("marker_recorded_event_attributes")
    )


# What the tests let a running Local Activity go on with, by Workflow id.
released: dict[str, asyncio.Event] = defaultdict(asyncio.Event)


@activity.defn
async def wait_until_released() -> None:
    await released[activity.info().workflow_id or ""].wait()


@workflow.defn
class PublishesThenWaits:
    @workflow.run
    async def run(self) -> None:
        steps = workflow_writer(STEPS)
        for step in ("plan", "call model", "call tool"):
            steps.publish(step)
        # A long Local Activity, such as a model call, keeps the task open.
        await workflow.execute_local_activity(
            wait_until_released, start_to_close_timeout=timedelta(seconds=30)
        )


async def test_records_show_while_the_task_runs_and_batch_in_one_flush(
    streams: Client,
):
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, PublishesThenWaits, activities=[wait_until_released]
    ) as worker:
        handle = await streams.start_workflow(
            PublishesThenWaits.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = get_stream_handle(streams, workflow_id).read(topic=STEPS)
        early = [await anext(stream) for _ in range(3)]
        await stream.aclose()
        # No Workflow Task has completed, so no marker is recorded yet.
        events = (await handle.fetch_history()).events
        assert not any(
            e.HasField("workflow_task_completed_event_attributes") for e in events
        )
        released[workflow_id].set()
        await handle.result()
    assert [r.value for r in early] == ["plan", "call model", "call tool"]
    # Three publishes in one Workflow Task are one flush, so one producer.
    run_id = handle.result_run_id
    assert len({r.producer_id for r in early}) == 1
    assert early[0].producer_id.startswith("__temporal_local_")
    assert early[0].producer_id.endswith(f"@{run_id}")
    assert [r.sequence for r in early] == [1, 2, 3]
    # One marker for the flush and one for the waiting Local Activity.
    assert markers(await handle.fetch_history()) == 2


@workflow.defn
class PublishesDuringAFlush:
    @workflow.run
    async def run(self) -> list[str]:
        steps = workflow_writer(STEPS)
        for index in range(6):
            steps.publish(str(index))
            # Lets the first flush start, so the rest buffer behind it.
            await asyncio.sleep(0)
        out = [(await steps.finish()).token, (await steps.finish()).token]
        try:
            steps.publish("late")
        except ValueError as error:
            out.append(str(error))
        try:
            workflow_writer("other").publish("x" * (2 << 20))
        except StreamError as error:
            out.append(str(error))
        return out


async def test_publishes_during_a_flush_land_next_and_in_order(client: Client):
    streams = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesDuringAFlush) as worker:
        finish, again, late, oversized = await streams.execute_workflow(
            PublishesDuringAFlush.run, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    assert [r.kind for r in records] == [RecordKind.DATA] * 6 + [RecordKind.FINISH]
    assert [r.value for r in records[:6]] == [str(index) for index in range(6)]
    # The first flush carries the first value. The rest wait for it and go
    # in the second flush, with the FINISH.
    producers = [r.producer_id for r in records]
    assert producers[0] != producers[1]
    assert set(producers[1:]) == {producers[1]}
    assert finish == again == records[-1].cursor.token
    assert "already finished" in late
    assert "per-record limit" in oversized


async def test_the_run_state_goes_with_the_run(client: Client):
    streams = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, PublishesDuringAFlush, max_cached_workflows=0
    ) as worker:
        handle = await streams.start_workflow(
            PublishesDuringAFlush.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    gc.collect()
    run_id = handle.result_run_id
    assert not [
        o for o in gc.get_objects() if type(o) is _RunWriters and o.run_id == run_id
    ]


@workflow.defn
class PublishesThenEnds:
    @workflow.run
    async def run(self, how: str, runs_left: int) -> str:
        steps = workflow_writer(STEPS)
        if how == "cancel":
            try:
                await workflow.wait_condition(lambda: False)
            except asyncio.CancelledError:
                steps.publish(f"{how} {runs_left} a")
                steps.publish(f"{how} {runs_left} b")
                raise
        steps.publish(f"{how} {runs_left} a")
        steps.publish(f"{how} {runs_left} b")
        # Nothing waits for the flush: the plugin drains the buffer.
        if how == "continue" and runs_left:
            workflow.continue_as_new(args=[how, runs_left - 1])
        if how == "fail":
            raise ApplicationError("failed on purpose", non_retryable=True)
        return "done"


@pytest.mark.parametrize("how", ["return", "continue", "fail", "cancel"])
async def test_the_buffer_drains_before_the_run_ends(client: Client, how: str):
    streams = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesThenEnds) as worker:
        handle = await streams.start_workflow(
            PublishesThenEnds.run,
            args=[how, 1],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        if how == "cancel":
            await handle.cancel()
        if how in ("fail", "cancel"):
            with pytest.raises(WorkflowFailureError):
                await handle.result()
        else:
            await handle.result()
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    runs = [1, 0] if how == "continue" else [1]
    assert [r.value for r in records] == [
        f"{how} {run} {part}" for run in runs for part in "ab"
    ]


# How many Workflow Tasks each Workflow failed on purpose, by Workflow id.
failed_tasks: dict[str, int] = defaultdict(int)


@workflow.defn
class FailsAfterAFlush:
    @workflow.run
    async def run(self) -> None:
        steps = workflow_writer(STEPS)
        steps.publish("a")
        steps.publish("b")
        await steps.flush()
        workflow_id = workflow.info().workflow_id
        if not failed_tasks[workflow_id]:
            failed_tasks[workflow_id] += 1
            # Fails the task before the flush's marker is recorded, so the
            # next task runs the flush again.
            raise RuntimeError("fail this Workflow Task")


async def test_a_failed_task_reruns_its_flush_as_superseded(streams: Client):
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, FailsAfterAFlush, workflow_runner=UNSANDBOXED
    ) as worker:
        handle = await streams.start_workflow(
            FailsAfterAFlush.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.SUPERSEDED,
        RecordKind.DATA,
        RecordKind.DATA,
    ]
    assert [r.value for r in records if r.kind is RecordKind.DATA] == list("abab")
    # The rerun is the same flush, so the same producer in a later attempt.
    first, rerun = records[0], records[3]
    assert first.producer_id == rerun.producer_id
    assert records[2].supersession is not None
    assert records[2].supersession.producer_id == first.producer_id
    assert (
        records[2].supersession.previous_attempt,
        records[2].supersession.attempt,
    ) == (
        first.attempt,
        rerun.attempt,
    )
    # The replay takes the flush's result from its marker, with no store.
    history = await handle.fetch_history()
    assert markers(history) == 1
    await Replayer(
        workflows=[FailsAfterAFlush],
        workflow_runner=UNSANDBOXED,
        plugins=[MemoryStreams()],
    ).replay_workflow(history)
    # Without the plugin's interceptor the writer can't drain, so it refuses.
    with pytest.raises(Exception):
        await Replayer(
            workflows=[FailsAfterAFlush], workflow_runner=UNSANDBOXED
        ).replay_workflow(history)


@workflow.defn
class PublishesFromReadOnlyHandlers:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.query
    def publish_in_query(self) -> None:
        workflow_writer(STEPS).publish("from a query")

    @workflow.update
    def finish(self) -> None:
        self.done = True

    @finish.validator
    def publish_in_validator(self) -> None:
        try:
            workflow_writer(STEPS).publish("from a validator")
        except workflow.ReadOnlyContextError as error:
            # Rejects the Update. Left to propagate, it fails the Workflow Task.
            raise ValueError(str(error)) from error


async def test_a_query_or_validator_cannot_publish(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, PublishesFromReadOnlyHandlers) as worker:
        handle = await streams.start_workflow(
            PublishesFromReadOnlyHandlers.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowQueryFailedError, match="query or an update"):
            await handle.query(PublishesFromReadOnlyHandlers.publish_in_query)
        with pytest.raises(WorkflowUpdateFailedError) as failed:
            await handle.execute_update(PublishesFromReadOnlyHandlers.finish)
        assert "query or an update" in str(failed.value.cause)
        await handle.terminate()
    # Nothing was published.
    assert markers(await handle.fetch_history()) == 0


@workflow.defn
class WritesWithoutAStore:
    @workflow.run
    async def run(self) -> str:
        try:
            workflow_writer(STEPS)
        except StreamUnsupportedError as error:
            return str(error)
        return "opened"


async def test_a_worker_without_the_store_cannot_write(client: Client):
    async with new_worker(client, WritesWithoutAStore) as worker:
        said = await client.execute_workflow(
            WritesWithoutAStore.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
    assert "Worker interceptor" in said


@workflow.defn
class FlushesOnce:
    @workflow.run
    async def run(self) -> None:
        steps = workflow_writer(STEPS)
        steps.publish("one")
        steps.publish("two")
        await steps.flush()


async def test_a_local_retry_of_a_flush_is_superseded(
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
    async with new_worker(streams, FlushesOnce) as worker:
        await streams.execute_workflow(
            FlushesOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.SUPERSEDED,
        RecordKind.DATA,
        RecordKind.DATA,
    ]
    assert [r.value for r in records if r.kind is RecordKind.DATA] == [
        "one",
        "two",
        "one",
        "two",
    ]


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
    async with new_worker(streams, FlushesOnce) as worker:
        await streams.execute_workflow(
            FlushesOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    # The repeat went to the store, which wrote the batch once.
    assert calls == [2, 2]
    assert [r.value for r in records] == ["one", "two"]


async def test_a_worker_registers_the_builtin_activities_once(client: Client):
    store = MemoryStreams()
    streams = await connect_with(client, store)
    worker = Worker(streams, task_queue=str(uuid.uuid4()), workflows=[FlushesOnce])
    assert worker.config(active_config=True).get("activities") == [
        publish_activity,
        read_activity,
        close_activity,
    ]
    with pytest.warns(UserWarning, match="same plugin type"):
        worker = Worker(
            streams,
            task_queue=str(uuid.uuid4()),
            workflows=[FlushesOnce],
            plugins=[store],
        )
    assert worker.config(active_config=True).get("activities") == [
        publish_activity,
        read_activity,
        close_activity,
    ]
    assert PUBLISH_ACTIVITY == "__temporal_streams_publish"


async def test_the_activity_refuses_input_that_is_not_a_payloads_message():
    with pytest.raises(ApplicationError, match="not a Payloads message") as raised:
        await ActivityEnvironment().run(
            publish_activity, "steps", False, RawValue(Payload(data=b"x"))
        )
    assert raised.value.non_retryable


async def test_closing_the_run_outside_its_loop_does_not_drain():
    async def never() -> None:
        await asyncio.Event().wait()

    run = run_then_drain(never())
    run.send(None)
    # Closing throws GeneratorExit into the run, outside any Workflow loop.
    run.close()


async def test_the_flush_gate_spaces_flush_starts_per_run_and_topic():
    gate = _FlushGate(capacity=2)
    key = ("wf", "run", "steps")
    await gate.wait(key, 200)
    started = time.monotonic()
    await gate.wait(key, 200)
    assert time.monotonic() - started >= 0.18
    # Another topic of the run, or no interval, does not wait.
    started = time.monotonic()
    await gate.wait(("wf", "run", "other"), 200)
    await gate.wait(key, 0)
    assert time.monotonic() - started < 0.1
    # The gate forgets the least recent key past its capacity.
    await gate.wait(("wf", "run", "third"), 200)
    assert list(gate._started) == [key, ("wf", "run", "third")]


@activity.defn
async def tick() -> None:
    pass


@workflow.defn
class PublishesBetweenSteps:
    @workflow.run
    async def run(self, steps: int, interval_ms: int) -> None:
        writer = workflow_writer(
            STEPS,
            min_flush_interval=timedelta(milliseconds=interval_ms) or None,
        )
        for index in range(steps):
            writer.publish(str(index))
            # Each step resolves a Local Activity, so a flush could start often.
            await workflow.execute_local_activity(
                tick, start_to_close_timeout=timedelta(seconds=10)
            )
        await writer.finish()


async def test_the_flush_interval_caps_the_flush_count(client: Client):
    streams = await connect_with(client, MemoryStreams())
    counts = {}
    for interval_ms in (0, 300):
        workflow_id = new_workflow_id()
        async with new_worker(
            streams, PublishesBetweenSteps, activities=[tick]
        ) as worker:
            started = time.monotonic()
            await streams.execute_workflow(
                PublishesBetweenSteps.run,
                args=[30, interval_ms],
                id=workflow_id,
                task_queue=worker.task_queue,
            )
            elapsed = time.monotonic() - started
        records = await read_all(
            get_stream_handle(streams, workflow_id).read(topic=STEPS)
        )
        data = [r for r in records if r.kind is RecordKind.DATA]
        assert [r.value for r in data] == [str(index) for index in range(30)]
        counts[interval_ms] = len({r.producer_id for r in records})
        if interval_ms:
            # At most one flush start per interval, plus the first and the finish.
            assert counts[interval_ms] <= elapsed / (interval_ms / 1000) + 2
    assert counts[300] < counts[0]


def redis_cli(*args: str) -> str:
    url = urllib.parse.urlparse(os.environ.get("STREAMS_REDIS_URL", ""))
    command = ["redis-cli", "-h", url.hostname or "localhost"]
    command += ["-p", str(url.port or 6379), *args]
    return subprocess.run(command, capture_output=True, text=True, check=True).stdout


async def test_each_flush_retires_its_dedupe_state(client: Client):
    if not shutil.which("redis-cli"):
        pytest.skip("needs redis-cli to look at the store")
    streams = await connect_with(client, redis_store())
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesBetweenSteps, activities=[tick]) as worker:
        await streams.execute_workflow(
            PublishesBetweenSteps.run,
            args=[10, 0],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    flushes = {r.producer_id for r in records}
    assert len(flushes) > 1
    (meta,) = [
        key
        for key in redis_cli("--scan", "--pattern", f"*{workflow_id}*").split()
        if key.endswith(":t:steps:meta")
    ]
    # Every flush's attempt returned, so none left a session field behind.
    assert "hw:" not in redis_cli("hkeys", meta)


async def test_a_failed_retire_does_not_fail_the_flush(
    streams: Client, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    async def refuse(*_args: object) -> object:
        raise RuntimeError("the store is gone for a moment")

    monkeypatch.setattr(StreamService, "retire_producer", refuse)
    workflow_id = new_workflow_id()
    with caplog.at_level(logging.WARNING):
        async with new_worker(streams, FlushesOnce) as worker:
            await streams.execute_workflow(
                FlushesOnce.run, id=workflow_id, task_queue=worker.task_queue
            )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    assert [r.value for r in records] == ["one", "two"]
    assert "Could not retire stream producer" in caplog.text
