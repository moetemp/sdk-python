"""A Workflow publishes to its own stream from its Workflow Task.

Each activation's records go with its completion, and Core appends them
before it reports the completion. The live tests run on memory, and on Redis
when ``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import asyncio
import gc
import json
import os
import shutil
import subprocess
import urllib.parse
import uuid
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import (
    Client,
    WorkflowFailureError,
    WorkflowHistory,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
)
from temporalio.contrib.streams import (
    RecordKind,
    StreamError,
    StreamStorePlugin,
    StreamUnsupportedError,
    SupersessionTracker,
    activity_handle,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams._local_activity import StreamsInterceptor
from temporalio.contrib.streams._workflow import read_activity
from temporalio.contrib.streams._workflow_writer import _RunWriters
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
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


def redis_cli(*args: str) -> str:
    url = urllib.parse.urlparse(os.environ.get("STREAMS_REDIS_URL", ""))
    command = ["redis-cli", "-h", url.hostname or "localhost"]
    command += ["-p", str(url.port or 6379), *args]
    return subprocess.run(command, capture_output=True, text=True, check=True).stdout


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
# What each Local Activity saw as the newest record when it started, by Workflow id.
seen_at_start: dict[str, list[str]] = defaultdict(list)


@activity.defn
async def wait_until_released() -> None:
    workflow_id = activity.info().workflow_id or ""
    # Core appends the activation's records before it dispatches this call.
    seen_at_start[workflow_id].append(
        (await activity_handle().latest(topic=STEPS)).token
    )
    await released[workflow_id].wait()


@workflow.defn
class PublishesThenWaits:
    @workflow.run
    async def run(self) -> None:
        steps = workflow_writer(STEPS)
        for step in ("plan", "call model"):
            steps.publish(step)
        # A long Local Activity, such as a model call, keeps the task open.
        await workflow.execute_local_activity(
            wait_until_released, start_to_close_timeout=timedelta(seconds=30)
        )
        steps.publish("call tool")
        await workflow.execute_local_activity(
            wait_until_released, start_to_close_timeout=timedelta(seconds=30)
        )


async def test_records_show_per_activation_while_the_task_runs(streams: Client):
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, PublishesThenWaits, activities=[wait_until_released]
    ) as worker:
        handle = await streams.start_workflow(
            PublishesThenWaits.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = get_stream_handle(streams, workflow_id).read(topic=STEPS)
        early = [await anext(stream) for _ in range(2)]
        released[workflow_id].set()
        early.append(await anext(stream))
        await stream.aclose()
        # The records showed while the first Workflow Task still ran.
        events = (await handle.fetch_history()).events
        assert not any(
            e.HasField("workflow_task_completed_event_attributes") for e in events
        )
        await handle.result()
    assert [r.value for r in early] == ["plan", "call model", "call tool"]
    run_id = handle.result_run_id
    assert {r.producer_id for r in early} == {f"@{run_id}"}
    assert [r.sequence for r in early] == [1, 2, 3]
    # One task, so one attempt.
    assert len({r.attempt for r in early}) == 1
    # Each Local Activity saw the records its activation published.
    assert seen_at_start[workflow_id] == [early[1].cursor.token, early[2].cursor.token]
    # Publishing adds no event: the two markers are the Local Activities'.
    assert markers(await handle.fetch_history()) == 2


@workflow.defn
class PublishesAndFinishes:
    @workflow.run
    async def run(self) -> list[str]:
        steps = workflow_writer(STEPS)
        for index in range(6):
            steps.publish(str(index))
        await steps.flush()
        await steps.finish()
        # FINISH is per topic, so another writer's finish writes nothing more.
        await workflow_writer(STEPS).finish()
        out = []
        try:
            steps.publish("late")
        except ValueError as error:
            out.append(str(error))
        try:
            workflow_writer("other").publish("x" * (2 << 20))
        except StreamError as error:
            out.append(str(error))
        return out


@pytest.mark.parametrize("sandboxed", [True, False])
async def test_a_run_publishes_in_order_then_finishes_once(
    client: Client, sandboxed: bool
):
    streams = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(
        streams,
        PublishesAndFinishes,
        workflow_runner=SandboxedWorkflowRunner() if sandboxed else UNSANDBOXED,
    ) as worker:
        handle = await streams.start_workflow(
            PublishesAndFinishes.run, id=workflow_id, task_queue=worker.task_queue
        )
        late, oversized = await handle.result()
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    assert [r.kind for r in records] == [RecordKind.DATA] * 6 + [RecordKind.FINISH]
    assert [r.value for r in records[:6]] == [str(index) for index in range(6)]
    assert {r.producer_id for r in records} == {f"@{handle.result_run_id}"}
    assert "already finished" in late
    assert "per-record limit" in oversized
    # A replay sends nothing to Core, so it needs no connected store.
    await Replayer(
        workflows=[PublishesAndFinishes], plugins=[MemoryStreams()]
    ).replay_workflow(await handle.fetch_history())


async def test_the_run_state_goes_with_the_run(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(
        streams, PublishesAndFinishes, max_cached_workflows=0
    ) as worker:
        await streams.execute_workflow(
            PublishesAndFinishes.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
    gc.collect()
    assert not [o for o in gc.get_objects() if type(o) is _RunWriters]


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
        # The records go with the completion that ends the run.
        if how == "continue" and runs_left:
            workflow.continue_as_new(args=[how, runs_left - 1])
        if how == "fail":
            raise ApplicationError("failed on purpose", non_retryable=True)
        return "done"


@pytest.mark.parametrize("how", ["return", "continue", "fail", "cancel"])
async def test_the_last_records_land_before_the_run_ends(client: Client, how: str):
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


@activity.defn
async def tick() -> None:
    pass


# How many Workflow Tasks each Workflow failed on purpose, by Workflow id.
failed_tasks: dict[str, int] = defaultdict(int)


@workflow.defn
class FailsAfterAPublish:
    @workflow.run
    async def run(self) -> None:
        steps = workflow_writer(STEPS)
        steps.publish("before")
        # A completed task, so what follows has a later prefix.
        await workflow.sleep(timedelta(milliseconds=10))
        steps.publish("a")
        steps.publish("b")
        # Ends the activation, so Core appends a and b, and the task goes on.
        await workflow.execute_local_activity(
            tick, start_to_close_timeout=timedelta(seconds=10)
        )
        workflow_id = workflow.info().workflow_id
        if not failed_tasks[workflow_id]:
            failed_tasks[workflow_id] += 1
            raise RuntimeError("fail this Workflow Task")


def prefix(attempt: int) -> int:
    return attempt >> 16


async def test_a_failed_task_reruns_as_superseded_and_keeps_earlier_tasks(
    streams: Client,
):
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, FailsAfterAPublish, activities=[tick], workflow_runner=UNSANDBOXED
    ) as worker:
        handle = await streams.start_workflow(
            FailsAfterAPublish.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.DATA,
        RecordKind.SUPERSEDED,
        RecordKind.DATA,
        RecordKind.DATA,
    ]
    before, first, _, superseded, rerun, _ = records
    assert [r.value for r in records if r.kind is RecordKind.DATA] == [
        "before",
        "a",
        "b",
        "a",
        "b",
    ]
    assert {r.producer_id for r in records if r.producer_id} == {
        f"@{handle.result_run_id}"
    }
    # The rerun keeps the failed task's prefix and raises its retry.
    assert prefix(before.attempt) < prefix(first.attempt) == prefix(rerun.attempt)
    assert first.attempt < rerun.attempt
    assert superseded.supersession is not None
    assert (
        superseded.supersession.previous_attempt,
        superseded.supersession.attempt,
    ) == (first.attempt, rerun.attempt)
    # The sequence restarts per attempt.
    assert (first.sequence, rerun.sequence) == (1, 1)
    # The tracker withdraws the failed task's records only.
    tracker: SupersessionTracker[str] = SupersessionTracker()
    withdrawn = [w.value for r in records for w in tracker.observe(r)]
    assert withdrawn == ["a", "b"]
    # It holds only the newest task's records, since earlier tasks completed.
    assert [r.value for r in tracker.standing()] == ["a", "b"]


@activity.defn
async def outlasts_the_task_timeout() -> None:
    await asyncio.sleep(2.5)


@workflow.defn
class PublishesAroundAHeartbeat:
    @workflow.run
    async def run(self) -> None:
        steps = workflow_writer(STEPS)
        steps.publish("a")
        # Core heartbeats the task at 80% of its timeout while this runs.
        await workflow.execute_local_activity(
            outlasts_the_task_timeout, start_to_close_timeout=timedelta(seconds=10)
        )
        steps.publish("b")


async def test_a_heartbeat_task_moves_the_prefix_with_no_supersession(
    streams: Client,
):
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, PublishesAroundAHeartbeat, activities=[outlasts_the_task_timeout]
    ) as worker:
        await streams.execute_workflow(
            PublishesAroundAHeartbeat.run,
            id=workflow_id,
            task_queue=worker.task_queue,
            task_timeout=timedelta(seconds=2),
        )
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=STEPS))
    assert [r.value for r in records] == ["a", "b"]
    assert prefix(records[0].attempt) < prefix(records[1].attempt)


@workflow.defn
class PublishesWhenAsked:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.go)
        workflow_writer(STEPS).publish("late")
        await workflow.wait_condition(lambda: False)

    @workflow.signal
    def publish(self) -> None:
        self.go = True


async def test_a_store_that_refuses_the_append_fails_the_task(client: Client):
    if not shutil.which("redis-cli"):
        pytest.skip("needs redis-cli to make the store refuse writes")
    streams = await connect_with(client, redis_store())
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesWhenAsked) as worker:
        handle = await streams.start_workflow(
            PublishesWhenAsked.run, id=workflow_id, task_queue=worker.task_queue
        )
        _, limit = redis_cli("config", "get", "maxmemory").split()
        # With no room left, Redis refuses every write.
        redis_cli("config", "set", "maxmemory", "1")
        try:
            await handle.signal(PublishesWhenAsked.publish)
            message = await asyncio.wait_for(_first_task_failure(handle), 20)
        finally:
            redis_cli("config", "set", "maxmemory", limit)
        await handle.terminate()
    assert "StreamOutputNotAppended" in message


async def _first_task_failure(handle: Any) -> str:
    while True:
        for event in (await handle.fetch_history()).events:
            if event.HasField("workflow_task_failed_event_attributes"):
                failure = event.workflow_task_failed_event_attributes.failure
                return f"{failure.application_failure_info.type} {failure.message}"
        await asyncio.sleep(0.2)


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


async def test_a_worker_registers_the_read_activity_once(client: Client):
    store = MemoryStreams()
    streams = await connect_with(client, store)
    worker = Worker(streams, task_queue=str(uuid.uuid4()), workflows=[FlushesOnce])
    assert worker.config(active_config=True).get("activities") == [read_activity]
    with pytest.warns(UserWarning, match="same plugin type"):
        worker = Worker(
            streams,
            task_queue=str(uuid.uuid4()),
            workflows=[FlushesOnce],
            plugins=[store],
        )
    config = worker.config(active_config=True)
    assert config.get("activities") == [read_activity]
    interceptors = config.get("interceptors") or []
    assert sum(isinstance(i, StreamsInterceptor) for i in interceptors) == 1


@workflow.defn
class PublishesPastSixteenMiB:
    @workflow.run
    async def run(self) -> None:
        steps = workflow_writer("big")
        try:
            for _ in range(12):
                steps.publish("x" * (1400 << 10))
        except StreamError:
            await workflow.sleep(timedelta(milliseconds=1))


async def test_an_activation_past_sixteen_mib_replays_the_same(client: Client):
    streams = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams, PublishesPastSixteenMiB) as worker:
        handle = await streams.start_workflow(
            PublishesPastSixteenMiB.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic="big"))
    assert len(records) == 12
    await Replayer(
        workflows=[PublishesPastSixteenMiB], plugins=[MemoryStreams()]
    ).replay_workflow(await handle.fetch_history())


_HISTORIES = Path(__file__).parent / "histories"


async def test_a_history_of_the_local_activity_writer_does_not_replay():
    # Recorded with the writer that flushed through Local Activities. Its
    # markers have no Local Activity call to match on replay.
    path = _HISTORIES / "FlushesOnce-local-activity-writer.json"
    history = WorkflowHistory.from_json("recorded", json.loads(path.read_text()))
    with pytest.raises(Exception, match="(?i)nondetermin"):
        await Replayer(
            workflows=[FlushesOnce], plugins=[MemoryStreams()]
        ).replay_workflow(history)
