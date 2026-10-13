"""A Local Activity publishes to the stream of the Workflow that runs it.

Its producer id holds across reruns and local retries, and its attempt
grows, so a rerun reaches readers as ``SUPERSEDED`` rather than as a stale
append. The live tests run on memory, and on Redis when
``STREAMS_REDIS_URL`` is set.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import uuid
from collections import defaultdict
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    RecordKind,
    StreamStorePlugin,
    StreamUnsupportedError,
    activity_handle,
    get_stream_handle,
    topic,
)
from temporalio.contrib.streams._local_activity import (
    LOCAL_ACTIVITY_HEADER,
    StreamsInterceptor,
    producer_attempt,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
from tests.helpers import new_worker

TOKENS = topic("tokens", str)
# The Workflows count their runs in this process, so they run unsandboxed.
UNSANDBOXED = UnsandboxedWorkflowRunner()


def redis_store() -> StreamStorePlugin:
    url = os.environ.get("STREAMS_REDIS_URL", "")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run on Redis")
    return RedisStreams(url, key_prefix=f"local-activity-{uuid.uuid4().hex}")


@pytest.fixture(params=["memory", "redis"])
async def streams(request: pytest.FixtureRequest, client: Client) -> Client:
    store = MemoryStreams() if request.param == "memory" else redis_store()
    return await connect_with(client, store)


def test_the_attempt_grows_with_history_then_task_then_local_attempt():
    ordered = [
        producer_attempt(3, 1, 1),
        producer_attempt(3, 1, 2),
        producer_attempt(3, 2, 1),
        producer_attempt(4, 1, 1),
        producer_attempt(4, 3, 9_999),
        producer_attempt(5, 0, 1),
    ]
    assert ordered == sorted(set(ordered))
    assert producer_attempt(3, 1, 1) == 3_00001_0001
    assert producer_attempt(51_200, 99_999, 9_999) < 2**53


@pytest.mark.parametrize(
    "parts", [(0, 1, 10_000), (0, 100_000, 1), (2**53 // 10**9 + 1, 1, 1)]
)
def test_an_attempt_too_large_to_encode_is_refused(parts: tuple[int, int, int]):
    with pytest.raises(StreamUnsupportedError, match="has no stream producer"):
        producer_attempt(*parts)


async def test_an_unnumbered_local_activity_has_no_stream_producer():
    env = ActivityEnvironment()
    env.info = dataclasses.replace(ActivityEnvironment.default_info(), is_local=True)

    async def local() -> None:
        activity_handle()

    with pytest.raises(StreamUnsupportedError, match="Worker interceptor"):
        await env.run(local)


# What the tests let a running Local Activity go on with, by Workflow id.
released: dict[str, asyncio.Event] = defaultdict(asyncio.Event)


@activity.defn
async def stream_until_released(tokens: list[str]) -> str:
    producer = activity_handle().producer(topic=TOKENS)
    await producer.append(*tokens)
    await released[activity.info().workflow_id or ""].wait()
    await producer.finish()
    return producer.producer_id


@workflow.defn
class RunsStreamingLocalActivity:
    @workflow.run
    async def run(self, tokens: list[str]) -> str:
        return await workflow.execute_local_activity(
            stream_until_released, tokens, start_to_close_timeout=timedelta(seconds=30)
        )


async def test_a_local_activity_streams_while_its_task_runs(streams: Client):
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, RunsStreamingLocalActivity, activities=[stream_until_released]
    ) as worker:
        handle = await streams.start_workflow(
            RunsStreamingLocalActivity.run,
            ["a", "b"],
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        stream = get_stream_handle(streams, workflow_id).read(topic=TOKENS)
        # The Local Activity waits for this read, so its Workflow Task is open.
        early = [await anext(stream), await anext(stream)]
        await stream.aclose()
        released[workflow_id].set()
        producer_id = await handle.result()
    run_id = handle.result_run_id
    assert producer_id == f"local-1@{run_id}"
    assert [r.value for r in early] == ["a", "b"]
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=TOKENS))
    assert [r.kind for r in records] == [RecordKind.DATA] * 2 + [RecordKind.FINISH]
    # The first Workflow Task started at event 3 and is in its first attempt.
    assert {(r.producer_id, r.attempt) for r in records} == {
        (producer_id, producer_attempt(3, 1, 1))
    }
    assert [r.sequence for r in records] == [1, 2, 3]


# The History length each run of a Workflow saw when it scheduled its Local
# Activity, whether it was replaying then, and how many Workflow Tasks it failed.
scheduled_at: dict[str, list[tuple[int, bool]]] = defaultdict(list)
failed_tasks: dict[str, int] = defaultdict(int)


@activity.defn
async def stream_one_run(slow: bool) -> None:
    info = activity.info()
    runs = len(scheduled_at[info.workflow_id or ""])
    producer = activity_handle().producer(topic=TOKENS)
    await producer.append(f"run {runs}", f"more from run {runs}")
    if slow and runs == 1:
        # A Signal between two Workflow Tasks makes the first one end before
        # it on replay, rather than merge with the next.
        await (
            activity.client()
            .get_workflow_handle(info.workflow_id or "")
            .signal(FailsItsTask.poke)
        )
        # Longer than the Workflow Task timeout, so Core completes the task
        # as a heartbeat while this runs.
        await asyncio.sleep(3)
    await producer.finish()


@workflow.defn
class FailsItsTask:
    @workflow.run
    async def run(self, failures: int, slow: bool) -> None:
        workflow_id = workflow.info().workflow_id
        scheduled_at[workflow_id].append(
            (
                workflow.info().get_current_history_length(),
                workflow.unsafe.is_replaying(),
            )
        )
        await workflow.execute_local_activity(
            stream_one_run, slow, start_to_close_timeout=timedelta(seconds=30)
        )
        if failed_tasks[workflow_id] < failures:
            failed_tasks[workflow_id] += 1
            # Fails the Workflow Task before the Local Activity's marker is
            # recorded, so the next task runs the Local Activity again.
            raise RuntimeError("fail this Workflow Task")

    @workflow.signal
    def poke(self) -> None:
        pass


async def run_failing(
    client: Client, failures: int, slow: bool
) -> tuple[str, list[tuple[int, bool]]]:
    workflow_id = new_workflow_id()
    async with new_worker(
        client,
        FailsItsTask,
        activities=[stream_one_run],
        workflow_runner=UNSANDBOXED,
    ) as worker:
        handle = await client.start_workflow(
            FailsItsTask.run,
            args=[failures, slow],
            id=workflow_id,
            task_queue=worker.task_queue,
            task_timeout=timedelta(seconds=2),
        )
        await handle.result()
    # Taken first, since the Replayer below runs the Workflow again.
    schedules = list(scheduled_at[workflow_id])
    history = await handle.fetch_history()
    # No header is compared on replay, with or without the interceptor.
    for interceptors in [[], [StreamsInterceptor()]]:
        await Replayer(
            workflows=[FailsItsTask],
            workflow_runner=UNSANDBOXED,
            interceptors=interceptors,
        ).replay_workflow(history)
    return workflow_id, schedules


def assert_each_run_supersedes(records: list, runs: int) -> None:
    kinds = [RecordKind.DATA, RecordKind.DATA, RecordKind.FINISH]
    assert [r.kind for r in records] == (kinds + [RecordKind.SUPERSEDED]) * (
        runs - 1
    ) + kinds
    written = [r for r in records if r.kind is not RecordKind.SUPERSEDED]
    assert len({r.producer_id for r in written}) == 1
    attempts = [r.attempt for r in written[::3]]
    assert attempts == sorted(set(attempts))
    assert [r.value for r in written if r.kind is RecordKind.DATA] == [
        text
        for run in range(1, runs + 1)
        for text in [f"run {run}", f"more from run {run}"]
    ]


async def test_a_rerun_after_failed_tasks_supersedes_the_last_run(streams: Client):
    workflow_id, schedules = await run_failing(streams, failures=2, slow=False)
    # The server records a task's first failure only, so the second and the
    # third run schedule at the same History length outside replay. A length
    # stamped by the Workflow would repeat, and the store would refuse it.
    assert schedules[1] == schedules[2]
    assert [replaying for _, replaying in schedules] == [False, False, False]
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=TOKENS))
    assert_each_run_supersedes(records, runs=3)
    assert [r.attempt for r in records if r.kind is RecordKind.FINISH] == [
        producer_attempt(3, 1, 1),
        producer_attempt(4, 2, 1),
        producer_attempt(4, 3, 1),
    ]
    supersessions = [r.supersession for r in records if r.supersession]
    assert [(s.previous_attempt, s.attempt) for s in supersessions] == [
        (producer_attempt(3, 1, 1), producer_attempt(4, 2, 1)),
        (producer_attempt(4, 2, 1), producer_attempt(4, 3, 1)),
    ]


async def test_a_rerun_after_a_task_heartbeat_supersedes(streams: Client):
    workflow_id, schedules = await run_failing(streams, failures=1, slow=True)
    # The first task completed as a heartbeat while the Local Activity ran,
    # so the rerun is scheduled by replaying it, at the length it had then.
    assert schedules == [(3, False), (3, True)]
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=TOKENS))
    assert_each_run_supersedes(records, runs=2)


# The Activity id each attempt of a Local Activity saw, by Workflow id.
activity_ids: dict[str, list[str]] = defaultdict(list)


@activity.defn
async def stream_then_fail_once() -> None:
    info = activity.info()
    activity_ids[info.workflow_id or ""].append(info.activity_id)
    producer = activity_handle().producer(topic=TOKENS)
    await producer.append(f"attempt {info.attempt}")
    if info.attempt == 1:
        raise ApplicationError("retry me")
    await producer.finish()


@workflow.defn
class BacksOffItsLocalActivity:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_local_activity(
            stream_then_fail_once,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=500)),
            # Longer backoffs than this run as a Workflow timer, and the retry
            # is scheduled again under a new Activity id.
            local_retry_threshold=timedelta(milliseconds=100),
        )


async def test_a_backed_off_retry_keeps_its_producer_id(streams: Client):
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, BacksOffItsLocalActivity, activities=[stream_then_fail_once]
    ) as worker:
        handle = await streams.start_workflow(
            BacksOffItsLocalActivity.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()
    first, retry = activity_ids[workflow_id]
    assert first != retry
    records = await read_all(get_stream_handle(streams, workflow_id).read(topic=TOKENS))
    assert [r.kind for r in records] == [
        RecordKind.DATA,
        RecordKind.SUPERSEDED,
        RecordKind.DATA,
        RecordKind.FINISH,
    ]
    assert {r.producer_id for r in records} == {f"local-1@{handle.result_run_id}"}
    assert [r.value for r in records if r.kind is RecordKind.DATA] == [
        "attempt 1",
        "attempt 2",
    ]


@workflow.defn
class CountsItsLocalActivities:
    @workflow.run
    async def run(self) -> list[str]:
        ids = []
        for _ in range(2):
            ids.append(
                await workflow.execute_local_activity(
                    stream_until_released,
                    ["x"],
                    start_to_close_timeout=timedelta(seconds=10),
                )
            )
        return ids


async def test_each_local_activity_call_is_its_own_producer(client: Client):
    # The plugin on the client and on the Worker still numbers each call once.
    store = MemoryStreams()
    streams = await connect_with(client, store)
    workflow_id = new_workflow_id()
    released[workflow_id].set()
    async with new_worker(
        streams,
        CountsItsLocalActivities,
        activities=[stream_until_released],
        plugins=[store],
    ) as worker:
        handle = await streams.start_workflow(
            CountsItsLocalActivities.run, id=workflow_id, task_queue=worker.task_queue
        )
        ids = await handle.result()
    run_id = handle.result_run_id
    assert ids == [f"local-1@{run_id}", f"local-2@{run_id}"]
    history = await handle.fetch_history()
    markers = [
        e.marker_recorded_event_attributes
        for e in history.events
        if e.HasField("marker_recorded_event_attributes")
    ]
    # The number rides in a header of the call, which no event records.
    assert len(markers) == 2
    assert all(LOCAL_ACTIVITY_HEADER not in m.header.fields for m in markers)
