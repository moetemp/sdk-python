"""A reader applies ``SUPERSEDED`` per producer, with the tracker's help."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest

from temporalio.client import Client
from temporalio.contrib.streams import (
    Cursor,
    LinkEvent,
    RecordKind,
    StreamLink,
    StreamRecord,
    StreamRef,
    Supersession,
    SupersessionTracker,
    get_stream_handle,
    track_supersession,
)
from temporalio.contrib.streams._supersession import _RETRY_BITS
from temporalio.contrib.streams.memory import MemoryStreams
from tests.contrib.streams._support import (
    connect_with,
    new_workflow_id,
    streams_crate_dir,
)
from tests.contrib.streams.test_workflow_writer import (
    STEPS,
    UNSANDBOXED,
    FailsAfterAPublish,
    tick,
)
from tests.helpers import new_worker


def data(
    producer: str, attempt: int, value: str, stale: bool = False, sequence: int = 0
) -> StreamRecord:
    return StreamRecord(
        kind=RecordKind.DATA,
        cursor=Cursor(value),
        topic="t",
        producer_id=producer,
        attempt=attempt,
        sequence=sequence,
        value=value,
        stale=stale,
    )


def close(producer: str) -> StreamRecord:
    return StreamRecord(
        kind=RecordKind.CLOSE,
        cursor=Cursor(f"close-{producer}"),
        topic="t",
        producer_id=producer,
        attempt=1,
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


def test_the_tracker_holds_producer_ids_up_to_the_read_states_bytes():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    # Ids as long as a Workflow writer's flushes, about 57 bytes.
    ids = [f"__temporal_local_{n}@{n:036d}" for n in range(300)]
    for producer in ids:
        tracker.observe(data(producer, 1, producer))
    held = {r.producer_id for r in tracker.standing()}
    budget = 16 << 10
    assert sum(len(p) + 20 for p in held) <= budget
    assert sum(len(p) + 20 for p in held) + len(ids[0]) + 20 > budget
    # The ones it kept are the newest.
    assert held == set(ids[-len(held) :])


def test_the_tracker_needs_room_for_one_producer():
    with pytest.raises(ValueError, match="max_producers"):
        SupersessionTracker(max_producers=0)


def test_one_producer_keeps_only_its_newest_records():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    for i in range(20_000):
        tracker.observe(data("activity", 1, str(i)))
    standing = tracker.standing()
    assert len(standing) == 10_000
    assert standing[0].value == "10000"
    withdrawn = tracker.observe(superseded("activity", 1, 2))
    assert len(withdrawn) == 10_000


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


def test_a_second_copy_of_a_batch_is_dropped():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    first = [data("a", 1, "a1", sequence=1), data("a", 1, "a2", sequence=2)]
    for record in [*first, data("b", 1, "b1", sequence=1)]:
        assert not tracker.drops(record)
        tracker.observe(record)
    # A retry after a lost answer landed both records again.
    copies = [data("a", 1, "a1", sequence=1), data("a", 1, "a2", sequence=2)]
    assert all(tracker.drops(copy) for copy in copies)
    assert all(tracker.observe(copy) == [] for copy in copies)
    assert not tracker.drops(data("a", 1, "a3", sequence=3))
    # Unnumbered records are never copies.
    assert not tracker.drops(data("c", 1, "c", sequence=0))
    tracker.observe(data("c", 1, "c", sequence=0))
    assert not tracker.drops(data("c", 1, "c", sequence=0))
    assert [r.value for r in tracker.standing()] == ["a1", "a2", "b1", "c"]


def test_a_copy_from_an_older_attempt_stays_stale():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    tracker.observe(data("a", 1, "a1", sequence=1))
    tracker.observe(superseded("a", 1, 2))
    tracker.observe(data("a", 2, "b1", sequence=1))
    late = data("a", 1, "a1", stale=True, sequence=1)
    assert not tracker.drops(late)
    assert tracker.observe(late) == [late]
    assert tracker.drops(data("a", 2, "b1", sequence=1))


def test_the_first_close_ends_the_stream():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    tracker.observe(data("a", 1, "a1", sequence=1))
    # A read never delivers a close, but the tracker must see it.
    assert tracker.drops(close("a"))
    assert tracker.observe(close("a")) == []
    assert tracker.closed
    # An append that raced the close, and a second closer.
    for late in (data("b", 1, "b1", sequence=1), close("b")):
        assert tracker.drops(late)
        assert tracker.observe(late) == []
    assert [r.value for r in tracker.standing()] == ["a1"]


async def test_track_supersession_skips_copies_and_stops_at_the_close():
    closed = []

    async def records() -> AsyncIterator[StreamRecord[str]]:
        try:
            yield data("a", 1, "a1", sequence=1)
            yield data("a", 1, "a1", sequence=1)
            yield data("a", 1, "a2", sequence=2)
            yield close("a")
            yield data("a", 1, "never read", sequence=3)
        finally:
            closed.append(True)

    seen = [record async for record, _ in track_supersession(records())]
    assert [r.value for r in seen] == ["a1", "a2"]
    assert closed == [True]


def test_a_close_of_another_topic_does_not_end_the_stream():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    tracker.observe(data("a", 1, "a1", sequence=1))
    other = dataclasses.replace(close("c"), topic="other")
    assert tracker.drops(other)
    tracker.observe(other)
    assert not tracker.closed
    tracker.observe(dataclasses.replace(close("c"), topic=""))
    assert tracker.closed


def test_the_tracker_forgets_the_copies_of_producers_it_saw_least_recently():
    tracker: SupersessionTracker[str] = SupersessionTracker(max_producers=2)
    for producer in ("a", "b", "c"):
        tracker.observe(data(producer, 1, producer, sequence=1))
    assert not tracker.drops(data("a", 1, "a", sequence=1))
    assert tracker.drops(data("c", 1, "c", sequence=1))


def test_a_workflow_rerun_withdraws_only_its_own_tasks_records():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    task_one, task_two = 1 << 16, 7 << 16
    for record in (data("@run", task_one | 1, "x"), data("@run", task_two | 1, "a")):
        assert tracker.observe(record) == []
    withdrawn = tracker.observe(superseded("@run", task_two | 1, task_two | 2))
    assert [r.value for r in withdrawn] == ["a"]
    # A later task is a continuation, not a rerun, so it withdraws nothing.
    tracker.observe(data("@run", task_two | 2, "a"))
    assert tracker.observe(superseded("@run", task_one | 1, task_one | 2)) == []


_VECTORS = streams_crate_dir() / "testdata/workflow_attempts.json"


def test_the_retry_bits_match_cores_vectors():
    vectors = json.loads(_VECTORS.read_text())
    assert vectors["retry_bits"] == _RETRY_BITS
    for case in vectors["attempts"]:
        assert case["attempt"] >> _RETRY_BITS == case["prefix"], case


@pytest.mark.parametrize(
    "case",
    json.loads(_VECTORS.read_text())["supersession"],
    ids=lambda case: case["label"],
)
def test_a_workflow_supersession_withdraws_what_cores_vectors_say(
    case: dict[str, Any],
):
    previous, next = case["previous"]["attempt"], case["next"]["attempt"]
    tracker: SupersessionTracker[str] = SupersessionTracker()
    if previous >> _RETRY_BITS:
        # A completed earlier task, whose record no supersession withdraws.
        tracker.observe(data("@run", 1, "earlier task"))
    tracker.observe(data("@run", previous, "previous"))
    withdrawn = (
        tracker.observe(superseded("@run", previous, next))
        if case["superseded"]
        else []
    )
    tracker.observe(data("@run", next, "next"))
    assert [r.value for r in withdrawn] == (["previous"] if case["superseded"] else [])


_READER_RULES = _VECTORS.with_name("reader_rules.json")


@pytest.mark.parametrize(
    "case",
    # A Core from before the reader rules has no vectors for them.
    json.loads(_READER_RULES.read_text())["cases"] if _READER_RULES.exists() else [],
    ids=lambda case: case["label"],
)
def test_the_tracker_drops_what_cores_reader_rules_drop(case: dict[str, Any]):
    records: list[StreamRecord[str]] = [
        StreamRecord(
            kind=RecordKind(stored.get("kind", 1)),
            cursor=Cursor(str(index)),
            topic=stored.get("topic", "out"),
            producer_id=stored["producer"],
            attempt=stored["attempt"],
            sequence=stored["sequence"],
            stale=index in case.get("stale", []),
        )
        for index, stored in enumerate(case["records"])
    ]
    tracker: SupersessionTracker[str] = SupersessionTracker()
    start = case.get("resume_after")
    if start is not None:
        # A resumed read knows only the record at its cursor.
        tracker.observe(records[start])
    delivered = []
    for index, record in enumerate(records):
        if start is not None and index <= start:
            continue
        if not tracker.drops(record):
            delivered.append(index)
        tracker.observe(record)
    assert delivered == case["delivered"]
    assert tracker.closed == case.get("closed", False)


def test_a_workflow_producer_holds_only_its_newest_tasks_records():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    tracker.observe(data("@run", 1 << 16 | 1, "x"))
    tracker.observe(data("@run", 3 << 16 | 1, "y"))
    assert [r.value for r in tracker.standing()] == ["y"]


async def test_a_reader_sees_each_value_once_after_a_task_reruns(client: Client):
    streams = await connect_with(client, MemoryStreams())
    workflow_id = new_workflow_id()
    async with new_worker(
        streams, FailsAfterAPublish, activities=[tick], workflow_runner=UNSANDBOXED
    ) as worker:
        await streams.execute_workflow(
            FailsAfterAPublish.run, id=workflow_id, task_queue=worker.task_queue
        )
    shown: list[str | None] = []
    read = get_stream_handle(streams, workflow_id).read(topic=STEPS)
    async for record, withdrawn in track_supersession(read):
        for old in withdrawn:
            shown.remove(old.value)
        if record.kind is RecordKind.DATA and not record.stale:
            shown.append(record.value)
    assert shown == ["before", "a", "b"]


def test_a_rerun_withdraws_the_links_of_its_earlier_attempt():
    tracker: SupersessionTracker[str] = SupersessionTracker()
    link: StreamRecord[str] = StreamRecord(
        kind=RecordKind.LINK,
        cursor=Cursor("l1"),
        topic="t",
        producer_id="tool@run",
        attempt=1,
        link=StreamLink(LinkEvent.OPEN, StreamRef.for_workflow("child"), "sub"),
    )
    assert tracker.observe(link) == []
    assert tracker.observe(data("tool@run", 1, "a")) == []
    assert tracker.observe(superseded("tool@run", 1, 2)) == [
        link,
        data("tool@run", 1, "a"),
    ]
