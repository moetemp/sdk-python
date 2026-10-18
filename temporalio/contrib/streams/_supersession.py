"""Applying ``SUPERSEDED`` and the other reader rules for a reader.

A ``SUPERSEDED`` record withdraws only what one producer wrote in its
earlier attempts. Records of other producers stay valid, wherever they sit
in the stream. So a reader that shows records from several producers must
find the withdrawn ones by producer and attempt, not by position. This
module holds the records a read delivered under their producer, and names
the ones each ``SUPERSEDED`` withdraws.

It also applies the rules the stream runtime's reader applies on every store.
It drops a second copy of a record, which a store without producer state can
hold, and everything after the first ``CLOSE``. The runtime already dropped
them from its own reads, so on those the rules change nothing. They matter for
records that come from somewhere else, for example a copy of a read kept by the
application.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import AsyncGenerator, AsyncIterable
from typing import Generic, TypeVar

from temporalio.contrib.streams._record import RecordKind, StreamRecord, Supersession

__all__ = ["SupersessionTracker", "track_supersession"]

T = TypeVar("T")

# The bounds of a read's state in the stream runtime, which forgets the producer it
# saw least recently past either one. The read reports no SUPERSEDED for a producer
# it forgot, so holding that producer's records would only use memory. The runtime
# counts each producer as its id plus about 20 bytes.
_MAX_PRODUCERS = 1_000
_MAX_STATE_BYTES = 16 << 10
_PRODUCER_OVERHEAD = 20
_MAX_RECORDS_PER_PRODUCER = 10_000
# How a Workflow producer id starts, ``@<run id>``.
WORKFLOW_PRODUCER_PREFIX = "@"
# The low bits of a Workflow producer's attempt that count the retries of one
# Workflow Task. The bits above hold the task's prefix.
_RETRY_BITS = 16


class SupersessionTracker(Generic[T]):
    """Tells which earlier records each ``SUPERSEDED`` record withdraws.

    Feed it every record of one read, in order, with :meth:`observe`. It
    holds each record that can still be withdrawn, under its producer,
    until it is withdrawn or :meth:`forget` drops its producer. It keeps the
    producers it saw most recently, at most ``max_producers`` of them and no
    more than fit in 16 KiB at their id's length plus 20 bytes each, and drops
    the producer it saw least recently past that. The read's own state in the
    stream runtime forgets producers by the same rule, so a producer the
    tracker dropped gets no ``SUPERSEDED`` either. For producer ids of Local
    Activities, that is about 200 producers. A stream that many Local
    Activities write to has a producer per call, so this bound is what keeps a
    read's memory from growing with the stream. For a Workflow's producer, it
    holds only the records of the newest Workflow Task prefix, since a rerun
    can't withdraw what a completed task wrote.

    It drops a record as the stream runtime's reader does (see :meth:`drops`).
    Such a record withdraws nothing and is not held.

    A record with an empty ``producer_id`` or no attempt can't be
    withdrawn, so it is not held. Of one producer, it holds the newest
    ``max_records_per_producer`` records and drops the oldest past that, so
    a long attempt, for example an Activity that streams many tokens, can't
    grow it without bound. A ``SUPERSEDED`` then withdraws only the records
    still held.
    """

    def __init__(
        self,
        max_producers: int = _MAX_PRODUCERS,
        max_records_per_producer: int = _MAX_RECORDS_PER_PRODUCER,
    ) -> None:
        """Start with no records held.

        Raises:
            ValueError: ``max_producers`` or ``max_records_per_producer`` is
                below 1.
        """
        if max_producers < 1:
            raise ValueError(f"max_producers must be at least 1, got {max_producers}")
        if max_records_per_producer < 1:
            raise ValueError(
                "max_records_per_producer must be at least 1, got "
                f"{max_records_per_producer}"
            )
        self._max_producers = max_producers
        self._max_records = max_records_per_producer
        self._held: OrderedDict[str, list[tuple[int, StreamRecord[T]]]] = OrderedDict()
        self._bytes = 0
        self._count = 0
        # Per producer, the newest attempt seen and its highest sequence.
        self._marks: OrderedDict[str, tuple[int, int]] = OrderedDict()
        self._mark_bytes = 0
        self._closed = False
        # The read's topic, from its first record, so a CLOSE of another topic
        # is told apart.
        self._topic: str | None = None

    @property
    def closed(self) -> bool:
        """Whether the tracker saw a ``CLOSE``, so the stream ended."""
        return self._closed

    def drops(self, record: StreamRecord[T]) -> bool:
        """Whether the reader rules drop ``record``, so a reader doesn't show it.

        ``CLOSE`` and ``TOPIC`` records are dropped, and so is every record
        after the first ``CLOSE`` of the topic or of the run chain. So is a
        copy of a record already seen, with the same producer and attempt
        and a sequence at or below the highest seen for that attempt. A
        store without producer state can hold such a copy after a retry of
        an append whose answer was lost. A record with sequence 0 is never
        a copy. Neither is a ``stale`` one, which an older attempt wrote.
        Pass a dropped record to :meth:`observe` too, since a ``CLOSE``
        ends the stream there.
        """
        if self._closed or record.kind in (RecordKind.CLOSE, RecordKind.TOPIC):
            return True
        if record.kind is RecordKind.SUPERSEDED or record.stale:
            return False
        mark = self._marks.get(record.producer_id)
        return (
            mark is not None
            and record.attempt == mark[0]
            and 0 < record.sequence <= mark[1]
        )

    def observe(self, record: StreamRecord[T]) -> list[StreamRecord[T]]:
        """Take the next record of the read, and return the records it withdraws.

        For a :attr:`RecordKind.SUPERSEDED` record, these are the held
        records of its producer with an attempt below the new one, in stream
        order. For a Workflow's producer, ``@<run id>``, it is only those of
        the Workflow Task of the attempt it names as previous, since the
        other tasks completed. That task is the new attempt's own on a rerun,
        or an earlier one whose rerun wrote nothing to the topic. For a
        record marked ``stale``, it is the record itself, since an older
        attempt wrote it after the read delivered a newer one. For a record
        that :meth:`drops`, and for any other record, nothing.
        """
        if record.kind is RecordKind.CLOSE and not self._closed:
            if not record.topic or self._topic in (None, record.topic):
                self._closed = True
            return []
        if self.drops(record):
            return []
        if self._topic is None:
            self._topic = record.topic
        if record.kind is not RecordKind.SUPERSEDED:
            self._mark(record)
        if record.kind is RecordKind.SUPERSEDED:
            supersession = record.supersession
            if supersession is None:
                return []
            held = self._held.get(supersession.producer_id, [])
            withdrawn = [r for _, r in held if _withdraws(supersession, r.attempt)]
            kept = [(i, r) for i, r in held if not _withdraws(supersession, r.attempt)]
            if kept:
                self._held[supersession.producer_id] = kept
            else:
                self.forget(supersession.producer_id)
            return withdrawn
        if record.stale:
            # The stream runtime counts a stale record as a sight of its producer.
            if record.producer_id in self._held:
                self._held.move_to_end(record.producer_id)
            return [record]
        if record.producer_id and record.attempt > 0:
            self._count += 1
            if record.producer_id not in self._held:
                self._held[record.producer_id] = []
                self._bytes += _size(record.producer_id)
            elif record.producer_id.startswith(WORKFLOW_PRODUCER_PREFIX):
                task = record.attempt >> _RETRY_BITS
                self._held[record.producer_id] = [
                    (i, r)
                    for i, r in self._held[record.producer_id]
                    if r.attempt >> _RETRY_BITS >= task
                ]
            held = self._held[record.producer_id]
            held.append((self._count, record))
            if len(held) > self._max_records:
                del held[: len(held) - self._max_records]
            self._held.move_to_end(record.producer_id)
            while len(self._held) > 1 and (
                len(self._held) > self._max_producers or self._bytes > _MAX_STATE_BYTES
            ):
                self.forget(next(iter(self._held)))
        return []

    def _mark(self, record: StreamRecord[T]) -> None:
        if not record.producer_id:
            return
        mark = self._marks.get(record.producer_id)
        if mark is None:
            self._mark_bytes += _size(record.producer_id)
            self._marks[record.producer_id] = (record.attempt, record.sequence)
        elif record.attempt > mark[0]:
            self._marks[record.producer_id] = (record.attempt, record.sequence)
        elif record.attempt == mark[0] and record.sequence > mark[1]:
            self._marks[record.producer_id] = (mark[0], record.sequence)
        self._marks.move_to_end(record.producer_id)
        while len(self._marks) > 1 and (
            len(self._marks) > self._max_producers
            or self._mark_bytes > _MAX_STATE_BYTES
        ):
            forgotten, _ = self._marks.popitem(last=False)
            self._mark_bytes -= _size(forgotten)

    def standing(self) -> list[StreamRecord[T]]:
        """The held records no ``SUPERSEDED`` withdrew yet, in stream order."""
        held = [entry for entries in self._held.values() for entry in entries]
        return [record for _, record in sorted(held, key=lambda entry: entry[0])]

    def forget(self, producer_id: str) -> None:
        """Drop the held records of ``producer_id``.

        Call it once that producer can't start another attempt, for example
        once the Activity behind it completed. A later ``SUPERSEDED`` for it
        then withdraws nothing.
        """
        if self._held.pop(producer_id, None) is not None:
            self._bytes -= _size(producer_id)


def _withdraws(supersession: Supersession, held: int) -> bool:
    if supersession.producer_id.startswith(WORKFLOW_PRODUCER_PREFIX):
        # A Workflow's attempt is its Workflow Task prefix, then the retry of that
        # task. The runtime withdraws only the task of the attempt it names, a
        # rerun's or one whose rerun wrote nothing, since the others completed.
        return (
            held >> _RETRY_BITS == supersession.previous_attempt >> _RETRY_BITS
            and held < supersession.attempt
        )
    return held < supersession.attempt


def _size(producer_id: str) -> int:
    return len(producer_id.encode()) + _PRODUCER_OVERHEAD


async def track_supersession(
    records: AsyncIterable[StreamRecord[T]],
    *,
    max_producers: int = _MAX_PRODUCERS,
    max_records_per_producer: int = _MAX_RECORDS_PER_PRODUCER,
) -> AsyncGenerator[tuple[StreamRecord[T], list[StreamRecord[T]]], None]:
    """Yield each record of ``records`` with the earlier records it withdraws.

    It skips the records that :meth:`SupersessionTracker.drops`, and stops
    at the first ``CLOSE``. A wrapper over :class:`SupersessionTracker`
    for a read::

        async for record, withdrawn in track_supersession(handle.read()):
            for old in withdrawn:
                undo(old)
            if record.kind is RecordKind.DATA and not record.stale:
                show(record)

    ``withdrawn`` is empty unless ``record`` is ``SUPERSEDED`` or stale.
    Closing this generator closes ``records`` too, so the read stops.
    ``max_producers`` and ``max_records_per_producer`` bound the tracker as
    in :class:`SupersessionTracker`.
    """
    tracker: SupersessionTracker[T] = SupersessionTracker(
        max_producers, max_records_per_producer
    )
    try:
        async for record in records:
            if tracker.drops(record):
                tracker.observe(record)
                if tracker.closed:
                    return
                continue
            yield record, tracker.observe(record)
    finally:
        close = getattr(records, "aclose", None)
        if close is not None:
            await close()
