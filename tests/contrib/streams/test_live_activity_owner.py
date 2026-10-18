"""Streams that a standalone Activity owns, against a server that serves them.

A standalone Activity owns a stream through its run. Each attempt writes as
a new producer attempt, the Activity closes the stream itself, and when the
run ends with no close, the server's notifier closes the stream with the
run's outcome. The stock dev server has no stream notifier, so every case
here skips there. ``scripts/streams_live_lane.sh`` runs them on a server
that has it, on memory, standalone Redis and a Redis Cluster.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.workflowservice.v1 import ResetActivityExecutionRequest
from temporalio.bridge.proto.streams import ReadRequest
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    BEGINNING,
    Cursor,
    RecordKind,
    StreamRef,
    activity_handle,
    get_stream_handle,
    topic,
    track_supersession,
    workflow_reader,
)
from temporalio.contrib.streams._plugin import store_for_client
from temporalio.contrib.streams._ref import stream_address
from temporalio.exceptions import ApplicationError, TerminatedError
from tests.contrib.streams._support import connect_with, new_workflow_id
from tests.contrib.streams.test_live_notifications import (
    skip_without_notifier,
    store,
)
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

TOKENS = topic("tokens", str)
# The lane checks owners every 2 seconds and debounces each stream's
# deliveries, so a close by the owner check lands in a few seconds.
OWNER_CHECK = 30.0


@pytest.fixture(params=["memory", "redis", "cluster"])
async def notifying(request: pytest.FixtureRequest, client: Client) -> Client:
    """A client whose store notifies, on a server with the notifier."""
    await skip_without_notifier(client)
    return await connect_with(client, store(request.param))


@workflow.defn
class Consume:
    """Reads an Activity's stream to its end and says what it saw."""

    @workflow.run
    async def run(self, ref: StreamRef) -> list[str]:
        seen: list[str] = []
        reader = workflow_reader(ref, topic=TOKENS)
        async with reader:
            async for batch in reader:
                for record in batch.records:
                    if record.kind is RecordKind.DATA:
                        seen.append(str(record.value))
                    else:
                        seen.append(record.kind.name)
        return seen


def _ref(handle: Any) -> StreamRef:
    return StreamRef.for_activity(handle.id, run_id=handle.run_id, topic=TOKENS)


async def _read(client: Client, ref: StreamRef) -> tuple[list[str], list[str]]:
    """What a client read sees, and what each ``SUPERSEDED`` withdrew."""
    seen: list[str] = []
    withdrawn: list[str] = []
    read = track_supersession(get_stream_handle(client, ref).read())
    async for record, gone in read:
        withdrawn.extend(str(r.value) for r in gone)
        assert not record.stale
        seen.append(
            str(record.value) if record.kind is RecordKind.DATA else record.kind.name
        )
    return seen, withdrawn


async def _close_result(client: Client, ref: StreamRef) -> Any:
    """The close result the stream runtime's read gives once the stream ended."""
    service = await store_for_client(client)._service_for(client)
    response = await service.read(
        ReadRequest(stream=stream_address(client.namespace, ref), after="$end")
    )
    assert response.done
    if not response.HasField("close_result"):
        return None
    return client.data_converter.payload_converter.from_payloads(
        [response.close_result]
    )[0]


# Steps 1 to 5: a retry supersedes, and both readers see the close result.


@activity.defn
async def produce() -> str:
    stream = activity_handle()
    out = stream.producer(topic=TOKENS)
    if activity.info().attempt == 1:
        await out.append("a1", "a2", "a3")
        raise ApplicationError("retry me")
    await out.append("b1", "b2")
    await stream.close(topic=TOKENS, result="done")
    return "ok"


async def test_live_both_readers_follow_a_standalone_activity_across_its_retry(
    notifying: Client,
):
    async with new_worker(notifying, Consume, activities=[produce]) as worker:
        owner = await notifying.start_activity(
            produce,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), maximum_attempts=2
            ),
        )
        ref = _ref(owner)
        reader = await notifying.start_workflow(
            Consume.run, ref, id=new_workflow_id(), task_queue=worker.task_queue
        )
        seen, withdrawn = await asyncio.wait_for(_read(notifying, ref), 60)
        assert await owner.result() == "ok"
        expected = ["a1", "a2", "a3", "SUPERSEDED", "b1", "b2"]
        assert seen == expected
        assert withdrawn == ["a1", "a2", "a3"]
        # Python readers expose no close result, so it is read from the stream runtime.
        assert await _close_result(notifying, ref) == "done"
        assert await asyncio.wait_for(reader.result(), 60) == expected


# Step 6: a standalone Activity follows a Workflow's stream and resumes from
# its heartbeat after a retry.

_followed: list[list[str]] = []


@activity.defn
async def follow(workflow_id: str) -> int:
    details = activity.info().heartbeat_details
    after = Cursor(details[0]) if details else BEGINNING
    seen: list[str] = []
    _followed.append(seen)
    stream = get_stream_handle(activity.client(), workflow_id, topic=TOKENS)
    async for record in stream.read(after=after):
        seen.append(str(record.value))
        activity.heartbeat(record.cursor.token)
        if activity.info().attempt == 1 and record.value == "p2":
            raise ApplicationError("dropped mid-stream")
    return len(seen)


async def test_live_a_standalone_activity_follows_a_stream_from_its_heartbeat(
    notifying: Client,
):
    _followed.clear()
    async with new_worker(notifying, Waits, activities=[follow]) as worker:
        workflow_id = await running_owner(notifying, worker.task_queue)
        stream = get_stream_handle(notifying, workflow_id, topic=TOKENS)
        producer = stream.producer(producer_id="p", attempt=1)
        await producer.append("p1", "p2", "p3")
        follower = await notifying.start_activity(
            follow,
            workflow_id,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), maximum_attempts=2
            ),
        )
        # The retry reads past the heartbeat and sleeps until the next record.
        await asyncio.sleep(3)
        await producer.append("p4")
        await stream.close()
        assert await asyncio.wait_for(follower.result(), 60) == 2
        assert _followed == [["p1", "p2"], ["p3", "p4"]]
        await notifying.get_workflow_handle(workflow_id).signal(Waits.finish)


# Step 7: an Activity that fails for good with no close.


@activity.defn
async def fails_hard() -> None:
    await activity_handle().producer(topic=TOKENS).append("x1")
    raise ApplicationError("boom", type="Boom", non_retryable=True)


async def test_live_an_activity_that_fails_with_no_close_fails_the_workflow_reader(
    notifying: Client,
):
    async with new_worker(notifying, Consume, activities=[fails_hard]) as worker:
        owner = await notifying.start_activity(
            fails_hard,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=10),
        )
        ref = _ref(owner)
        reader = await notifying.start_workflow(
            Consume.run, ref, id=new_workflow_id(), task_queue=worker.task_queue
        )
        seen, _ = await asyncio.wait_for(_read(notifying, ref), OWNER_CHECK)
        assert seen == ["x1"]
        with pytest.raises(WorkflowFailureError) as raised:
            await asyncio.wait_for(reader.result(), OWNER_CHECK)
        cause = raised.value.cause
        assert isinstance(cause, ApplicationError)
        assert (cause.type, cause.message) == ("Boom", "boom")


# Step 8: a reset after attempt 2 of 3.

_calls: dict[str, int] = {}
_second_failed: dict[str, asyncio.Event] = {}


@activity.defn
async def writes_until_reset() -> None:
    key = activity.info().activity_id
    _calls[key] = call = _calls.get(key, 0) + 1
    stream = activity_handle()
    await stream.producer(topic=TOKENS).append(f"r{call}")
    if call == 1:
        raise ApplicationError("retry me", next_retry_delay=timedelta(seconds=1))
    if call == 2:
        _second_failed[key].set()
        raise ApplicationError("reset me", next_retry_delay=timedelta(minutes=5))
    await stream.close(topic=TOKENS, result="after reset")


async def test_live_an_attempt_after_a_reset_supersedes_with_no_divergence(
    notifying: Client,
):
    activity_id = new_workflow_id()
    _second_failed[activity_id] = asyncio.Event()
    async with new_worker(
        notifying, Consume, activities=[writes_until_reset]
    ) as worker:
        owner = await notifying.start_activity(
            writes_until_reset,
            id=activity_id,
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        ref = _ref(owner)
        reader = await notifying.start_workflow(
            Consume.run, ref, id=new_workflow_id(), task_queue=worker.task_queue
        )
        await asyncio.wait_for(_second_failed[activity_id].wait(), 30)
        await asyncio.sleep(0.5)
        await notifying.workflow_service.reset_activity_execution(
            ResetActivityExecutionRequest(
                namespace=notifying.namespace,
                activity_id=activity_id,
                run_id=owner.run_id or "",
                identity="lane",
                request_id=str(uuid.uuid4()),
            )
        )
        await asyncio.wait_for(owner.result(), 30)
        expected = ["r1", "SUPERSEDED", "r2", "SUPERSEDED", "r3"]
        seen, withdrawn = await asyncio.wait_for(_read(notifying, ref), 30)
        assert seen == expected
        assert withdrawn == ["r1", "r2"]
        assert await _close_result(notifying, ref) == "after reset"
        assert await asyncio.wait_for(reader.result(), 30) == expected


# Step 9: a terminated Activity.


@activity.defn
async def runs_until_terminated() -> None:
    await activity_handle().producer(topic=TOKENS).append("t1")
    while True:
        activity.heartbeat()
        await asyncio.sleep(0.2)


async def test_live_a_terminated_activity_fails_the_workflow_reader(
    notifying: Client,
):
    async with new_worker(
        notifying, Consume, activities=[runs_until_terminated]
    ) as worker:
        owner = await notifying.start_activity(
            runs_until_terminated,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=60),
            heartbeat_timeout=timedelta(seconds=5),
        )
        ref = _ref(owner)
        reader = await notifying.start_workflow(
            Consume.run, ref, id=new_workflow_id(), task_queue=worker.task_queue
        )
        read = asyncio.ensure_future(_read(notifying, ref))
        await asyncio.sleep(3)
        await owner.terminate(reason="stop it")
        seen, _ = await asyncio.wait_for(read, OWNER_CHECK)
        assert seen == ["t1"]
        with pytest.raises(WorkflowFailureError) as raised:
            await asyncio.wait_for(reader.result(), OWNER_CHECK)
        assert isinstance(raised.value.cause, TerminatedError)
