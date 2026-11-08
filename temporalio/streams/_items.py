"""The items that a Stream read gives."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeVar

from temporalio.streams._close import StreamCloseStatus
from temporalio.streams._offset import StreamOffset
from temporalio.streams.system import models

T = TypeVar("T")


class End(enum.Enum):
    """An ``offset`` past the newest Record, to read only new Records. Use ``END``."""

    END = "END"


END = End.END


class ItemKind(enum.Enum):
    """The kind of a read item. Code matches on an item's ``kind``, which narrows its type.

    .. warning::
        This API is experimental.
    """

    RECORD = "record"
    """A :py:class:`Record`."""

    CLOSED = "closed"
    """A :py:class:`Closed` item, the last item of a read."""


@dataclass(frozen=True)
class Record(Generic[T]):
    """A data Record of a Stream.

    .. warning::
        This API is experimental.
    """

    kind: Literal[ItemKind.RECORD] = field(default=ItemKind.RECORD, init=False)
    """Always :py:attr:`ItemKind.RECORD`."""

    data: T
    """Value of the Record, decoded with the Stream's payload type. A list is one value."""

    offset: StreamOffset
    """Position of the Record in the Stream. Offsets are ordered and unique, with gaps."""

    producer: models.Producer | None
    """Producer that wrote the Record."""


@dataclass(frozen=True)
class Closed:
    """The close of a Stream, the last item of a read.

    A read of a closed Stream ends after this item. On the memory and Redis backends the status is
    :py:attr:`StreamCloseStatus.StreamCloseStatusClosed` and ``failure`` is ``None``. The owner
    statuses and a close failure come from the native runtime.

    .. warning::
        This API is experimental.
    """

    kind: Literal[ItemKind.CLOSED] = field(default=ItemKind.CLOSED, init=False)
    """Always :py:attr:`ItemKind.CLOSED`."""

    offset: StreamOffset
    """The offset at which the Stream ended."""

    status: StreamCloseStatus
    """Why the Stream closed."""

    failure: BaseException | None = None
    """The owner's failure for ``OWNER_FAILED``, or ``None``."""
