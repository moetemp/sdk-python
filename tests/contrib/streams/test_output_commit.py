"""A Workflow's own publish rides the completion of the activation that made it.

The writer adds the records to the completion's commit, the Worker's
completion encoder runs each body through the codec, and Core appends them
when the activation completes and records a marker of them.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Sequence
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.bridge.proto.external_data import ExternalStreamMarkerData
from temporalio.bridge.proto.streams import (
    ReadRequest,
    StreamAddress,
    StreamOwnerKind,
)
from temporalio.client import Client, WorkflowHandle, WorkflowUpdateFailedError
from temporalio.contrib.streams import (
    RecordKind,
    StreamClosedError,
    StreamStorePlugin,
    Supersession,
    activity_handle,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.converter import DataConverter, ExternalStorage, PayloadCodec
from tests.contrib.streams._support import connect_with, new_workflow_id, read_all
from tests.helpers import new_worker
from tests.test_extstore import InMemoryTestDriver

EVENTS = topic("events", dict)
OTHER = topic("other", dict)

MARKER_NAME = "core_external_stream"


async def output_markers(handle: WorkflowHandle) -> list[ExternalStreamMarkerData]:
    markers = []
    async for event in handle.fetch_history_events():
        if event.event_type != EventType.EVENT_TYPE_MARKER_RECORDED:
            continue
        attributes = event.marker_recorded_event_attributes
        if attributes.marker_name == MARKER_NAME:
            markers.append(
                ExternalStreamMarkerData.FromString(
                    attributes.details["external_stream"].payloads[0].data
                )
            )
    return markers


@workflow.defn
class PublishTwoTopics:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": 1})
        workflow_writer(OTHER).publish({"n": 2})
        workflow_writer(EVENTS).publish({"n": 3})
        workflow_writer(EVENTS).finish()


async def test_an_accepted_task_commits_its_output_in_a_marker(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishTwoTopics) as worker:
        handle = await streams_client.start_workflow(
            PublishTwoTopics.run, id=workflow_id, task_queue=worker.task_queue
        )
        await handle.result()

    (marker,) = await output_markers(handle)
    manifest = marker.output
    assert manifest.run_id == handle.result_run_id
    assert [(t.topic, t.record_count, t.finished) for t in manifest.topics] == [
        ("events", 3, True),
        ("other", 1, False),
    ]

    stream = get_stream_handle(streams_client, workflow_id)
    events = await read_all(stream.read(topic=EVENTS))
    assert [r.value for r in events if r.kind is RecordKind.DATA] == [
        {"n": 1},
        {"n": 3},
    ]
    assert events[-1].kind is RecordKind.FINISH
    # The run writes as one producer, numbering each topic from one.
    assert [(r.producer_id, r.attempt, r.sequence) for r in events] == [
        (f"@{handle.result_run_id}", 1, sequence) for sequence in (1, 2, 3)
    ]
    assert [r.value for r in await read_all(stream.read(topic=OTHER))] == [{"n": 2}]


_task_attempts: dict[str, int] = {}


# Unsandboxed so the counter survives the failed task: a sandboxed run
# re-imports this module and would start it at zero every time.
@workflow.defn(sandboxed=False)
class FailsItsFirstTask:
    @workflow.run
    async def run(self, key: str) -> None:
        _task_attempts[key] = _task_attempts.get(key, 0) + 1
        workflow_writer(EVENTS).publish({"attempt": _task_attempts[key]})
        if _task_attempts[key] == 1:
            raise RuntimeError("fail this Workflow Task once")


async def test_a_failed_activation_publishes_nothing(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, FailsItsFirstTask) as worker:
        handle = await streams_client.start_workflow(
            FailsItsFirstTask.run,
            workflow_id,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        await handle.result()

    failed = [
        event
        async for event in handle.fetch_history_events()
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
    ]
    assert len(failed) == 1
    assert len(await output_markers(handle)) == 1
    records = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    # Only the retry's record: the failed activation's completion carried no
    # commit. The failed task is in History, so the retry writes as attempt 2.
    assert [(r.value, r.attempt) for r in records] == [({"attempt": 2}, 2)]


class NonceCodec(PayloadCodec):
    """Encrypts with a fresh nonce per call, so two encodings never match."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=uuid.uuid4().bytes + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(p.data[16:]) for p in payloads]


@workflow.defn
class PublishOnce:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"secret": "value"})


async def test_bodies_go_through_the_workflow_codec(client: Client):
    store = MemoryStreams()
    coded = await connect_with(client, store, DataConverter(payload_codec=NonceCodec()))
    workflow_id = new_workflow_id()
    async with new_worker(coded, PublishOnce) as worker:
        await coded.execute_workflow(
            PublishOnce.run, id=workflow_id, task_queue=worker.task_queue
        )
    service = await store._service_for(coded)
    raw = await service.read(
        ReadRequest(
            stream=StreamAddress(
                namespace=client.namespace,
                owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                workflow_id=workflow_id,
                topic=EVENTS.name,
            )
        )
    )
    (stored,) = [record.stored for record in raw.records]
    # The store holds ciphertext and the plaintext hash Core stamped from lang's.
    assert stored.body.metadata["encoding"] == b"binary/nonce"
    assert len(stored.metadata["temporal.io/content-hash"].data) == 64
    (record,) = await read_all(get_stream_handle(coded, workflow_id).read(topic=EVENTS))
    assert record.value == {"secret": "value"}


@activity.defn
async def nothing() -> None:
    pass


@workflow.defn
class PublishAroundALocalActivity:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        workflow_writer(EVENTS).publish({"n": "after"})


async def test_two_publishing_completions_in_one_task_commit_in_order(
    client: Client,
):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(
        streams_client, PublishAroundALocalActivity, activities=[nothing]
    ) as worker:
        handle = await streams_client.start_workflow(
            PublishAroundALocalActivity.run,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        await handle.result()

    markers = await output_markers(handle)
    assert len(markers) == 2
    records = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    assert [(r.value, r.sequence) for r in records] == [
        ({"n": "before"}, 1),
        ({"n": "after"}, 2),
    ]


_released: dict[str, asyncio.Event] = {}


@activity.defn
async def wait_for_release(key: str) -> None:
    await _released[key].wait()


@workflow.defn
class PublishThenWaitInALocalActivity:
    @workflow.run
    async def run(self, key: str) -> None:
        workflow_writer(EVENTS).publish({"n": "before"})
        await workflow.execute_local_activity(
            wait_for_release, key, start_to_close_timeout=timedelta(seconds=30)
        )
        workflow_writer(EVENTS).publish({"n": "after"})


async def test_a_publish_is_visible_while_its_task_runs_a_local_activity(
    client: Client,
):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    _released[workflow_id] = asyncio.Event()
    async with new_worker(
        streams_client,
        PublishThenWaitInALocalActivity,
        activities=[wait_for_release],
    ) as worker:
        handle = await streams_client.start_workflow(
            PublishThenWaitInALocalActivity.run,
            workflow_id,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        reader = get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
        try:
            first = await asyncio.wait_for(anext(reader), 10)
        finally:
            await reader.aclose()
        assert first.value == {"n": "before"}
        # The first Workflow Task still runs the local activity, so the server
        # has not accepted the task that published the record.
        completed = [
            event
            async for event in handle.fetch_history_events()
            if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
        ]
        assert completed == []
        _released[workflow_id].set()
        await handle.result()


_reruns: dict[str, int] = {}


# Unsandboxed so the counter survives the failed task.
@workflow.defn(sandboxed=False)
class FailsAfterItsRecordIsVisible:
    @workflow.run
    async def run(self, key: str) -> None:
        workflow_writer(EVENTS).publish({"n": 1})
        await workflow.execute_local_activity(
            nothing, start_to_close_timeout=timedelta(seconds=10)
        )
        _reruns[key] = _reruns.get(key, 0) + 1
        if _reruns[key] == 1:
            raise RuntimeError("fail this Workflow Task once")
        workflow_writer(EVENTS).finish()


async def test_a_rerun_of_a_failed_task_supersedes_what_it_published(
    client: Client,
):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(
        streams_client, FailsAfterItsRecordIsVisible, activities=[nothing]
    ) as worker:
        handle = await streams_client.start_workflow(
            FailsAfterItsRecordIsVisible.run,
            workflow_id,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        await handle.result()
    records = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    producer = f"@{handle.result_run_id}"
    # The first activation's record was visible before the task failed. The
    # rerun publishes it again as the run's next attempt, with the same
    # sequence, and readers see the change.
    assert [(r.kind, r.value, r.attempt, r.sequence) for r in records] == [
        (RecordKind.DATA, {"n": 1}, 1, 1),
        (RecordKind.SUPERSEDED, None, 2, 0),
        (RecordKind.DATA, {"n": 1}, 2, 1),
        (RecordKind.FINISH, None, 2, 2),
    ]
    assert records[1].supersession == Supersession(producer, 1, 2)


@workflow.defn
class PublishAfterARejectedUpdate:
    def __init__(self) -> None:
        self.go = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.go)
        workflow_writer(EVENTS).publish({"after": "rejected update"})

    @workflow.update
    def set_go(self, value: bool) -> None:
        self.go = value

    @set_go.validator
    def check_go(self, value: bool) -> None:
        if not value:
            raise ValueError("only True starts the publish")

    @workflow.signal
    def start(self) -> None:
        self.go = True


async def test_a_publish_right_after_a_rejected_update_commits(client: Client):
    # Core drops the speculative task of a rejected Update, and with it the
    # task's history floor, so the next publishing task must find its own.
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishAfterARejectedUpdate) as worker:
        handle = await streams_client.start_workflow(
            PublishAfterARejectedUpdate.run,
            id=workflow_id,
            task_queue=worker.task_queue,
        )
        with pytest.raises(WorkflowUpdateFailedError):
            await handle.execute_update(PublishAfterARejectedUpdate.set_go, False)
        await handle.signal(PublishAfterARejectedUpdate.start)
        await handle.result()
    (record,) = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    assert record.value == {"after": "rejected update"}


@workflow.defn
class PublishThenContinue:
    @workflow.run
    async def run(self, runs_left: int) -> None:
        workflow_writer(EVENTS).publish({"runs_left": runs_left})
        if runs_left:
            workflow.continue_as_new(runs_left - 1)
        workflow_writer(EVENTS).finish()


async def test_each_run_stamps_its_own_id_across_continue_as_new(client: Client):
    streams_client = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, PublishThenContinue) as worker:
        handle = await streams_client.start_workflow(
            PublishThenContinue.run, 1, id=workflow_id, task_queue=worker.task_queue
        )
        first_run = handle.first_execution_run_id
        await handle.result()
        last_run = (await handle.describe()).run_id

    records = await read_all(
        get_stream_handle(streams_client, workflow_id).read(topic=EVENTS)
    )
    assert [(r.kind, r.value, r.run_id) for r in records] == [
        (RecordKind.DATA, {"runs_left": 1}, first_run),
        (RecordKind.DATA, {"runs_left": 0}, last_run),
        (RecordKind.FINISH, None, last_run),
    ]
    assert first_run != last_run


@workflow.defn
class ClosesItsTopic:
    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        writer = workflow_writer(EVENTS)
        writer.publish({"n": 1})
        writer.finish()
        # This release has no close call yet, so the case commits Core's close
        # the way the Nexus layer's close does, next to the topic's FINISH.
        result = workflow.payload_converter().to_payloads(["closed"])[0]
        workflow._Runtime.current().workflow_stream_commit().closes.add(
            topic=EVENTS.name, result=result
        )
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


def store_for(backing: str) -> StreamStorePlugin:
    if backing == "memory":
        return MemoryStreams()
    url = os.environ.get("STREAMS_REDIS_URL")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run the Redis cases")
    return RedisStreams(url, key_prefix=f"close-{uuid.uuid4().hex}")


@pytest.mark.parametrize("backing", ["memory", "redis"])
async def test_a_committed_close_leaves_the_topic_closed(client: Client, backing: str):
    store = store_for(backing)
    streams_client = await connect_with(client, store)
    workflow_id = new_workflow_id()
    async with new_worker(streams_client, ClosesItsTopic) as worker:
        handle = await streams_client.start_workflow(
            ClosesItsTopic.run, id=workflow_id, task_queue=worker.task_queue
        )
        stream = get_stream_handle(streams_client, workflow_id)
        # Core closes the topic in the background once the server accepts the
        # task, so the case waits for the refusal.
        closed = False
        for attempt in range(1, 51):
            producer = stream.producer(
                topic=EVENTS, producer_id="late", attempt=attempt
            )
            try:
                await producer.append({"late": attempt})
            except StreamClosedError:
                closed = True
                break
            await asyncio.sleep(0.1)
        assert closed
        # The run goes on, so another topic of its stream still takes appends.
        other = stream.producer(topic=OTHER, producer_id="late", attempt=1)
        await other.append({"other": 1})
        await handle.signal(ClosesItsTopic.finish)
        await handle.result()


@activity.defn
async def appends_a_large_body() -> None:
    await activity_handle().producer(topic=EVENTS).append({"from": "x" * 4096})


@workflow.defn
class PublishesLargeBodies:
    @workflow.run
    async def run(self) -> None:
        workflow_writer(EVENTS).publish({"from": "w" * 4096})
        await workflow.execute_activity(
            appends_a_large_body, start_to_close_timeout=timedelta(seconds=10)
        )


async def test_large_bodies_go_to_external_storage_and_read_back(client: Client):
    driver = InMemoryTestDriver()
    converter = DataConverter(
        external_storage=ExternalStorage(drivers=[driver], payload_size_threshold=1024)
    )
    stored = await connect_with(client, MemoryStreams(), converter)
    workflow_id = new_workflow_id()
    async with new_worker(
        stored, PublishesLargeBodies, activities=[appends_a_large_body]
    ) as worker:
        await stored.execute_workflow(
            PublishesLargeBodies.run, id=workflow_id, task_queue=worker.task_queue
        )
    records = await read_all(get_stream_handle(stored, workflow_id).read(topic=EVENTS))
    assert [r.value for r in records] == [{"from": "w" * 4096}, {"from": "x" * 4096}]
    # Both bodies went to the driver, from the Workflow's completion and from
    # the Activity's append, so the store held claims.
    kept = b"".join(driver._storage.values())
    assert b"w" * 4096 in kept and b"x" * 4096 in kept
