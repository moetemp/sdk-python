"""The value types the stream contract is expressed in.

Nothing here touches Temporal or a store, so every store shares it
unchanged.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Generic, TypeVar

__all__ = [
    "Cursor",
    "RecordKind",
    "StreamRecord",
    "Supersession",
]

T = TypeVar("T")


@enum.unique
class RecordKind(enum.IntEnum):
    """What a record is.

    The stored kinds match ``temporal.sdk.streams.v1.StreamRecordKind``
    value for value, so a kind crosses the wire as the integer the proto
    holds. :attr:`SUPERSEDED` is never stored.
    """

    UNSPECIFIED = 0
    """The proto's zero value.

    A stored record whose writer set no kind is read as :attr:`DATA`, so a
    reader never sees this kind on a record.
    """

    DATA = 1
    """Carries a value published by a Workflow or a producer."""

    FINISH = 2
    """The producer in ``producer_id`` will write nothing more on this topic.

    An empty ``producer_id`` names the owning Workflow. A Workflow's
    ``FINISH`` covers one run: after Continue-as-New the next run can publish
    on the topic again, under the same empty ``producer_id``. It does not end
    a read and says nothing about the producer's outcome: an Activity can
    still fail after it wrote ``FINISH``.
    """

    SUPERSEDED = 3
    """A later attempt of the producer in ``producer_id`` started writing.

    It withdraws only what that producer wrote on this topic in its earlier
    attempts. Records of other producers stay valid, and so does the rest of
    the stream. Match the earlier records on ``producer_id`` and an
    ``attempt`` below :attr:`Supersession.attempt`.

    Core's reader synthesizes it from the records it observed. No store
    holds it, so every store reports a retry the same way. Its cursor is the
    position before the new attempt's first record. A reader that resumes
    there gets this record again, then the new attempt, since dropping the
    earlier attempt's records twice is harmless and missing it is not.
    """

    # 4 is kept for a kind a later release stores.

    CLOSE = 5
    """The stream's topic is closed, and the read ends here.

    A producer writes it when it closes the topic, or when it learns that the
    owner ended. The owner's Core writes it when the run chain ends through
    a Workflow Task. Only the first one counts. Readers drop what an append
    that raced the close wrote after it. An empty ``topic`` closes every
    topic of the run chain.
    """

    TOPIC = 6
    """A producer started writing on the topic in ``topic``.

    It sits in the run chain's control log, on a store that can't list a
    chain's topics, so a closer finds every topic to close. A reader of a
    topic never sees it.
    """


@dataclass(frozen=True)
class Cursor:
    """A position in one stream.

    Opaque on purpose. The token names the store that minted it and the
    stream it belongs to, and a token from another store or another stream
    is refused with
    :class:`temporalio.contrib.streams.StreamCursorError`. Do not compare two
    cursors or do arithmetic on one: hand a cursor back to resume strictly
    after the record it names.
    """

    token: str

    def __str__(self) -> str:
        """The store's position token."""
        return self.token


@dataclass(frozen=True)
class Supersession:
    """What a :attr:`RecordKind.SUPERSEDED` record reports.

    ``producer_id`` started ``attempt``, so the records it wrote in
    ``previous_attempt`` and before are withdrawn. No other producer's
    records are.
    """

    producer_id: str
    previous_attempt: int
    attempt: int


@dataclass(frozen=True)
class StreamRecord(Generic[T]):
    """One record as a reader sees it.

    ``value`` is set on a :attr:`RecordKind.DATA` record and
    ``supersession`` on a :attr:`RecordKind.SUPERSEDED` one. Other kinds
    carry neither, so a consumer narrows on ``kind`` first.
    """

    kind: RecordKind
    cursor: Cursor
    topic: str
    producer_id: str = ""
    """Who wrote it, or empty when the owning Workflow wrote it."""
    attempt: int = 0
    """The producer's attempt, or 0 when the producer declared none."""
    sequence: int = 0
    """The producer's position within its attempt, or 0 when it does not number."""
    run_id: str = ""
    """The run that published it, when the owning Workflow did; empty otherwise.

    A stream follows its Workflow's run chain, so this tells a successor run
    or a reset branch apart. An Activity's record leaves it empty, and the
    scheduling run is in its ``producer_id``, as ``<Activity id>@<run id>``.
    """
    value: T | None = None
    """The published value. Set on ``DATA`` only."""
    supersession: Supersession | None = None
    """The attempt change being reported. Set on ``SUPERSEDED`` only."""
    stale: bool = False
    """An older attempt wrote it after this read delivered a newer attempt.

    Such a record is not part of the producer's current answer, for example
    the last writes of an Activity attempt that timed out but kept running.
    """
