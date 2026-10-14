"""A reader applies ``SUPERSEDED`` per producer, with the tracker's help."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from temporalio.client import Client
from temporalio.contrib.streams import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
    SupersessionTracker,
    get_stream_handle,
    track_supersession,
)
from temporalio.contrib.streams.memory import MemoryStreams
from tests.contrib.streams._support import connect_with, new_workflow_id
from tests.contrib.streams.test_workflow_writer import (
    STEPS,
    UNSANDBOXED,
    FailsAfterAFlush,
)
from tests.helpers import new_worker


def data(producer: str, attempt: int, value: str, stale: bool = False) -> StreamRecord:
    return StreamRecord(
        kind=RecordKind.DATA,
        cursor=Cursor(value),
        topic="t",
        producer_id=producer,
        attempt=attempt,
        value=value,
        stale=stale,
    )


def superseded(producer: str, previous: int, attempt: int) -> StreamRecord:
    return StreamRecord(
        kind=RecordKind.SUPERSEDED,
        cursor=Cursor(f"s{attempt}"),
        topic="t",
        producer_id=producer,
        attempt=attempt,
        supersession=Supersession(producer, previous, attempt),
    )


def test_superseded_withdraws_only_its_producers_earlier_attempts():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    for record in [data("a", 1, "a1"), data("b", 1, "b1"), data("a", 1, "a2")]:
        assert tracker.observe(record) == []
    withdrawn = tracker.observe(superseded("a", 1, 2))
    assert [r.value for r in withdrawn] == ["a1", "a2"]
    assert tracker.observe(data("a", 2, "a3")) == []
    # Producer b's record stands, in its place in the stream.
    assert [r.value for r in tracker.standing()] == ["b1", "a3"]


def test_a_stale_record_arrives_withdrawn():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    tracker.observe(data("a", 1, "a1"))
    tracker.observe(superseded("a", 1, 2))
    late = data("a", 1, "late", stale=True)
    assert tracker.observe(late) == [late]
    assert tracker.standing() == []


def test_unattributed_records_are_never_withdrawn_and_forget_drops_a_producer():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    tracker.observe(data("", 0, "owner"))
    tracker.observe(data("a", 1, "a1"))
    tracker.forget("a")
    assert tracker.observe(superseded("a", 1, 2)) == []
    # Neither is held: one can't be withdrawn, the other was forgotten.
    assert tracker.standing() == []


def test_the_tracker_holds_the_producers_it_saw_most_recently():
    tracker: SupersessionTracker[str] = SupersessionTracker(max_producers=2)
    for producer in ("a", "b"):
        tracker.observe(data(producer, 1, f"{producer}1"))
    # Seeing a again makes b the producer seen least recently.
    tracker.observe(data("a", 1, "a2"))
    tracker.observe(data("c", 1, "c1"))
    assert [r.value for r in tracker.standing()] == ["a1", "a2", "c1"]
    assert tracker.observe(superseded("b", 1, 2)) == []
    assert [r.value for r in tracker.observe(superseded("a", 1, 2))] == ["a1", "a2"]


def test_the_tracker_needs_room_for_one_producer():
    with pytest.raises(ValueError, match="max_producers"):
        SupersessionTracker(max_producers=0)


async def test_track_supersession_pairs_records_and_closes_the_read():
    closed = []

    async def records() -> AsyncIterator[StreamRecord[str]]:
        try:
            for record in [data("a", 1, "a1"), superseded("a", 1, 2)]:
                yield record
            yield data("a", 2, "never read")
        finally:
            closed.append(True)

    pairs = track_supersession(records())
    first, second = await anext(pairs), await anext(pairs)
    await pairs.aclose()
    assert first[1] == []
    assert [r.value for r in second[1]] == ["a1"]
    assert closed == [True]


async def test_a_reader_sees_each_value_once_after_a_flush_reruns(client: Client):
    streams = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, FailsAfterAFlush, workflow_runner=UNSANDBOXED
    ) as worker:
        await streams.execute_workflow(
            FailsAfterAFlush.run, id=workflow_id, task_queue=worker.task_queue
        )
    shown: list[str | None] = []
    read = get_stream_handle(streams, workflow_id).read(topic=STEPS)
    async for record, withdrawn in track_supersession(read):
        for old in withdrawn:
            shown.remove(old.value)
        if record.kind is RecordKind.DATA and not record.stale:
            shown.append(record.value)
    assert shown == ["a", "b"]
