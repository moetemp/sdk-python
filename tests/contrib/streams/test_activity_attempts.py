"""An Activity's producer attempt comes from the attempt's start time.

A reset sets an Activity's Temporal attempt back to one, so the Temporal
attempt alone can't order the attempts of one Activity. Core composes the
producer attempt from the attempt's start time and its Temporal attempt.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import ResetActivityRequest
from temporalio.bridge.proto.streams import ActivityProducer
from temporalio.client import Client, WorkflowHistory
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    RecordKind,
    Supersession,
    activity_handle,
    get_stream_handle,
    topic,
    workflow_reader,
)
from temporalio.contrib.streams._handles import (
    _activity_attempt,
    _set_attempt_start,
    attempt_key,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from tests.contrib.streams._support import (
    connect_with,
    new_workflow_id,
    read_all,
    streams_crate_dir,
)
from tests.helpers import new_worker

TOKENS = topic("tokens", str)
_HISTORIES = Path(__file__).parent / "histories"


@activity.defn
async def writes_then_retries() -> None:
    stream = activity_handle()
    out = stream.producer(topic=TOKENS)
    if activity.info().attempt == 1:
        await out.append("a1", "a2")
        raise ApplicationError("retry me")
    await out.append("b1")
    await stream.close(topic=TOKENS, result="done")


@workflow.defn
class ReadsItsActivityStream:
    @workflow.run
    async def run(self) -> list[str]:
        await workflow.execute_activity(
            writes_then_retries,
            activity_id="tokens",
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=50), maximum_attempts=2
            ),
        )
        seen: list[str] = []
        async with workflow_reader(
            workflow.info().workflow_id, topic=TOKENS, replay_from_store=False
        ) as reader:
            async for batch in reader:
                seen.extend(f"{r.kind.name} {r.value}" for r in batch.records)
        return seen


def test_the_attempt_key_matches_the_core_vectors():
    # Found here, not at import, since the sandbox imports this module again
    # and refuses the cargo call there.
    vectors = streams_crate_dir() / "testdata/streams/activity_attempts.json"
    for vector in json.loads(vectors.read_text()):
        started = vector["started_time"]
        started_ms = None
        if started is not None:
            started_ms = started["seconds"] * 1000 + started["nanos"] // 1_000_000
        key = attempt_key(vector["attempt"], started_ms)
        expected = vector["key"]
        assert key == (None if expected is None else int(expected)), vector["name"]


def test_a_start_time_core_refuses_writes_the_temporal_attempt():
    identity = ActivityProducer(attempt=2)
    _set_attempt_start(identity, datetime(1970, 1, 1, tzinfo=timezone.utc))
    assert not identity.HasField("attempt_started_time")
    assert _activity_attempt(identity) == 2


async def test_a_history_from_before_the_attempt_key_replays():
    # Recorded with the Temporal attempt as the producer attempt. The read
    # markers hold the records, so replay reads nothing.
    path = _HISTORIES / "ReadsItsActivityStream.json"
    history = WorkflowHistory.from_json("recorded", json.loads(path.read_text()))
    await Replayer(
        workflows=[ReadsItsActivityStream], plugins=[MemoryStreams()]
    ).replay_workflow(history)


async def test_a_retry_writes_an_attempt_keyed_by_its_start(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(
        streams, ReadsItsActivityStream, activities=[writes_then_retries]
    ) as worker:
        handle = await streams.start_workflow(
            ReadsItsActivityStream.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        assert await handle.result() == [
            "DATA a1",
            "DATA a2",
            "SUPERSEDED None",
            "DATA b1",
        ]
    records = await read_all(get_stream_handle(streams, handle.id).read(topic=TOKENS))
    first, second = records[0].attempt, records[3].attempt
    assert records[2].supersession == Supersession(
        records[3].producer_id, first, second
    )
    # The low 11 bits carry the Temporal attempt, the rest the start time.
    assert (first & 0x7FF, second & 0x7FF) == (1, 2)
    assert first >> 16 <= second >> 16
    assert first >> 16 > 0


# Set by each call of the Activity below, so the test resets it in its backoff.
_calls: list[int] = []
_failed = asyncio.Event()


@activity.defn
async def writes_until_reset() -> None:
    _calls.append(activity.info().attempt)
    out = activity_handle().producer(topic=TOKENS)
    await out.append(f"c{len(_calls)}")
    if len(_calls) < 3:
        raise ApplicationError("retry me", next_retry_delay=timedelta(milliseconds=50))
    if len(_calls) == 3:
        _failed.set()
        raise ApplicationError("reset me", next_retry_delay=timedelta(minutes=5))


@workflow.defn
class RunsUntilReset:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            writes_until_reset,
            activity_id="tokens",
            start_to_close_timeout=timedelta(seconds=10),
        )


async def test_an_attempt_after_a_reset_supersedes_the_attempts_before_it(
    client: Client, env: WorkflowEnvironment
):
    if env.supports_time_skipping:
        pytest.skip("The time-skipping server can't reset an Activity")
    _calls.clear()
    _failed.clear()
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(
        streams, RunsUntilReset, activities=[writes_until_reset]
    ) as worker:
        handle = await streams.start_workflow(
            RunsUntilReset.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        # The third attempt waits 5 minutes for its retry, so the reset lands
        # in that backoff and starts Temporal attempt 1 at once.
        await asyncio.wait_for(_failed.wait(), 30)
        await asyncio.sleep(0.5)
        await streams.workflow_service.reset_activity(
            ResetActivityRequest(
                namespace=streams.namespace,
                execution=WorkflowExecution(workflow_id=handle.id),
                id="tokens",
            )
        )
        await asyncio.wait_for(handle.result(), 30)
    assert _calls == [1, 2, 3, 1]
    records = await read_all(get_stream_handle(streams, handle.id).read(topic=TOKENS))
    assert [(r.kind, r.value, r.stale) for r in records] == [
        (RecordKind.DATA, "c1", False),
        (RecordKind.SUPERSEDED, None, False),
        (RecordKind.DATA, "c2", False),
        (RecordKind.SUPERSEDED, None, False),
        (RecordKind.DATA, "c3", False),
        (RecordKind.SUPERSEDED, None, False),
        (RecordKind.DATA, "c4", False),
    ]
    assert records[6].attempt & 0x7FF == 1
    assert records[6].attempt > records[4].attempt
