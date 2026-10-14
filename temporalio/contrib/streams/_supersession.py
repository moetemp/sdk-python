"""Applying ``SUPERSEDED`` for a reader.

A ``SUPERSEDED`` record withdraws only what one producer wrote in its
earlier attempts. Records of other producers stay valid, wherever they sit
in the stream. So a reader that shows records from several producers must
find the withdrawn ones by producer and attempt, not by position. This
module holds the records a read delivered under their producer, and names
the ones each ``SUPERSEDED`` withdraws.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterable
from typing import Generic, TypeVar

from temporalio.contrib.streams._record import RecordKind, StreamRecord

__all__ = ["SupersessionTracker", "track_supersession"]

T = TypeVar("T")


class SupersessionTracker(Generic[T]):
    """Tells which earlier records each ``SUPERSEDED`` record withdraws.

    Feed it every record of one read, in order, with :meth:`observe`. It
    holds each record that can still be withdrawn, under its producer, so
    it keeps every such record of the read in memory until it is withdrawn
    or :meth:`forget` drops its producer.

    A record with an empty ``producer_id`` or no attempt can't be
    withdrawn, so it is not held.
    """

    def __init__(self) -> None:
        """Start with no records held."""
        self._held: dict[str, list[tuple[int, StreamRecord[T]]]] = {}
        self._count = 0

    def observe(self, record: StreamRecord[T]) -> list[StreamRecord[T]]:
        """Take the next record of the read, and return the records it withdraws.

        For a :attr:`RecordKind.SUPERSEDED` record, these are the held
        records of its producer with an attempt below the new one, in stream
        order. For a record marked ``stale``, it is the record itself, since
        an older attempt wrote it after the read delivered a newer one. For
        any other record, nothing.
        """
        if record.kind is RecordKind.SUPERSEDED:
            supersession = record.supersession
            if supersession is None:
                return []
            held = self._held.get(supersession.producer_id, [])
            withdrawn = [r for _, r in held if r.attempt < supersession.attempt]
            kept = [(i, r) for i, r in held if r.attempt >= supersession.attempt]
            if kept:
                self._held[supersession.producer_id] = kept
            else:
                self._held.pop(supersession.producer_id, None)
            return withdrawn
        if record.stale:
            return [record]
        if record.producer_id and record.attempt > 0:
            self._count += 1
            self._held.setdefault(record.producer_id, []).append((self._count, record))
        return []

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
        self._held.pop(producer_id, None)


async def track_supersession(
    records: AsyncIterable[StreamRecord[T]],
) -> AsyncGenerator[tuple[StreamRecord[T], list[StreamRecord[T]]], None]:
    """Yield each record of ``records`` with the earlier records it withdraws.

    A wrapper over :class:`SupersessionTracker` for a read::

        async for record, withdrawn in track_supersession(handle.read()):
            for old in withdrawn:
                undo(old)
            if record.kind is RecordKind.DATA and not record.stale:
                show(record)

    ``withdrawn`` is empty unless ``record`` is ``SUPERSEDED`` or stale.
    Closing this generator closes ``records`` too, so the read stops.
    """
    tracker: SupersessionTracker[T] = SupersessionTracker()
    try:
        async for record in records:
            yield record, tracker.observe(record)
    finally:
        close = getattr(records, "aclose", None)
        if close is not None:
            await close()
