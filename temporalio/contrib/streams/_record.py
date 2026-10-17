"""The value types the stream contract is expressed in.

Nothing here touches Temporal or a store, so every store shares it
unchanged.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from temporalio.contrib.streams._ref import StreamRef

__all__ = [
    "Cursor",
    "LinkEvent",
    "RecordKind",
    "StreamLink",
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

    LINK = 4
    """A reference to another stream, in :attr:`StreamRecord.link`.

    A follower reads the linked stream in place, in one order with this
    stream. ``value`` is an optional value the writer put on the link, such
    as a label. A reader that follows nothing sees it as a record of its
    own.
    """

    CLOSE = 5
    """The topic in ``topic`` is closed, or every topic of the run chain when empty.

    A producer writes it when it closes the topic, or when it learns that the
    owner ended. The owner's Worker writes it when the run chain ends
    through a Workflow Task. The stream runtime's read ends at the first one
    and doesn't deliver it, and it drops what an append that raced the close
    wrote after it. So a reader sees one only in records that come from
    somewhere else.
    """

    TOPIC = 6
    """A producer started writing on the topic in ``topic``.

    It sits in the run chain's control log, on a store that can't list a
    chain's topics, so a closer finds every topic to close. A read never
    delivers it.
    """


@enum.unique
class LinkEvent(enum.IntEnum):
    """What a :attr:`RecordKind.LINK` record says about its linked stream.

    The values match ``temporal.sdk.streams.v1.StreamLink.Event``.
    """

    OPEN = 1
    """A follower starts reading the linked stream strictly after ``after``."""

    JOIN = 2
    """The writer waited for the link named ``name`` up to ``until``.

    With no ``until``, it waited for the linked stream's end.
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
class StreamLink:
    """The reference a :attr:`RecordKind.LINK` record holds.

    It is stored as plaintext, outside the payload codec, as Workflow and
    Activity ids are in History. Put secrets in the record's value.
    """

    event: LinkEvent
    ref: StreamRef | None
    """The linked stream. Set on ``OPEN``, ``None`` on ``JOIN``."""
    name: str
    """Names the link within its producer. A ``JOIN`` matches the ``OPEN`` with this name."""
    after: Cursor | None = None
    """On ``OPEN``, a cursor of the linked stream. ``None`` starts at its oldest record."""
    until: Cursor | None = None
    """On ``JOIN``, a cursor of the linked stream. ``None`` is the stream's end."""
    namespace: str = ""
    """The linked stream's namespace. Empty is the namespace of this stream."""
    endpoint: str = ""
    """The Nexus endpoint of the linked stream. Empty is the one that served this stream."""


@dataclass(frozen=True)
class StreamRecord(Generic[T]):
    """One record as a reader sees it.

    ``value`` is set on a :attr:`RecordKind.DATA` record,
    ``supersession`` on a :attr:`RecordKind.SUPERSEDED` one, and ``link`` on
    a :attr:`RecordKind.LINK` one, whose ``value`` is optional. Other kinds
    carry none, so a consumer narrows on ``kind`` first.
    """

    kind: RecordKind
    cursor: Cursor
    topic: str
    producer_id: str = ""
    """Who wrote it, or empty when the owning Workflow wrote it."""
    attempt: int = 0
    """The producer's attempt, or 0 when the producer declared none.

    An Activity writes ``started_ms << 11 | min(temporal_attempt, 2047)``,
    where ``started_ms`` is the Unix time in milliseconds at which the
    server started the attempt. A retry, and an attempt after a reset that
    set the Temporal attempt back to one, start later, so they write a
    higher attempt.
    """
    sequence: int = 0
    """The producer's position within its attempt, or 0 when it does not number."""
    run_id: str = ""
    """The run that published it, when the owning Workflow did; empty otherwise.

    A stream follows its Workflow's run chain, so this tells a successor run
    or a reset branch apart. An Activity's record leaves it empty, and the
    scheduling run is in its ``producer_id``, as ``<Activity id>@<run id>``.
    """
    value: T | None = None
    """The published value. Set on ``DATA``, and on ``LINK`` when the writer gave one."""
    supersession: Supersession | None = None
    """The attempt change being reported. Set on ``SUPERSEDED`` only."""
    link: StreamLink | None = None
    """The reference to another stream. Set on ``LINK`` only."""
    stale: bool = False
    """An older attempt wrote it after this read delivered a newer attempt.

    Such a record is not part of the producer's current answer, for example
    the last writes of an Activity attempt that timed out but kept running.
    """
