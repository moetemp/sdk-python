"""Opt-in client-side supersession for Stream reads.

Import it explicitly. It is not part of the base ``temporalio.streams`` surface::

    from temporalio.streams.superseded import Superseded, with_superseded

The base read gives ``Record | Closed`` only. :py:func:`with_superseded` wraps a reader and adds
:py:class:`Superseded` items, computed from the ``Producer`` epoch that each Record carries, with
no Store facts. It holds the read's view in memory, so it is not for very large Streams.

Only a failed Activity or Standalone Activity attempt is superseded: a later Record with the same
Activity ``id`` and ``run_id`` and a strictly higher epoch withdraws the earlier attempt's
Records. A client never supersedes, since a newer client epoch only fences. A Workflow write is
not superseded from the epochs alone, because a run's ``Producer`` id is stable across its
Workflow Tasks and the Store keeps no facts to tell a failed task from a completed one.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import enum
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any, Literal

from temporalio.streams._items import Closed, Record
from temporalio.streams._offset import StreamOffset
from temporalio.streams._reader import Reader
from temporalio.streams.system import models


class SupersededKind(enum.Enum):
    """The kind of a :py:class:`Superseded` item."""

    SUPERSEDED = "superseded"


@dataclass(frozen=True)
class Superseded:
    """An item that withdraws the Records of a failed Activity attempt.

    :py:func:`with_superseded` yields it when a later Record has the same Activity ``id`` and
    ``run_id`` and a strictly higher epoch. A live view removes the Records in
    :py:attr:`withdrawn_offsets`. A view that kept Records from an earlier read checks them with
    :py:meth:`withdraws`.

    .. warning::
        This API is experimental.
    """

    kind: Literal[SupersededKind.SUPERSEDED] = field(
        default=SupersededKind.SUPERSEDED, init=False
    )
    """Always :py:attr:`SupersededKind.SUPERSEDED`."""

    offset: StreamOffset
    """The offset of the Record whose higher epoch withdrew the earlier ones."""

    producer: models.Producer | None
    """The Producer whose higher epoch withdraws the earlier Records."""

    withdrawn_offsets: tuple[StreamOffset, ...] = ()
    """The offsets of the Records that this read gave and this item withdraws, in order."""

    def withdraws(self, record: Record[Any] | models.Producer | None) -> bool:
        """Whether this item withdraws a Record, or the Records of a Producer.

        It answers for any earlier Record, such as one that an app showed before a resume, from
        the Producer that each Record carries.
        """
        other = record.producer if isinstance(record, Record) else record
        if other is None or self.producer is None:
            return False
        group = _group(other)
        if group is None or group != _group(self.producer):
            return False
        return _epoch_key(other) < _epoch_key(self.producer)


async def with_superseded(
    reader: Reader[Any],
) -> AsyncGenerator[Record[Any] | Superseded | Closed, None]:
    """Wrap a reader so it also yields :py:class:`Superseded` items.

    It yields each :py:class:`Record` and the :py:class:`Closed` item of the base read, and a
    ``Superseded`` item before the Record whose higher Activity epoch withdraws the earlier
    attempt's Records.
    """
    # The offsets of the Records shown for each Activity Producer group, at its highest epoch so
    # far. A higher epoch supersedes them.
    shown: dict[tuple[str, str, str], tuple[tuple[int, ...], list[StreamOffset]]] = {}
    try:
        async for item in reader:
            if isinstance(item, Record) and item.producer is not None:
                group = _group(item.producer)
                if group is not None:
                    key = _epoch_key(item.producer)
                    seen = shown.get(group)
                    if seen is not None and key > seen[0]:
                        yield Superseded(
                            offset=item.offset,
                            producer=item.producer,
                            withdrawn_offsets=tuple(seen[1]),
                        )
                        shown[group] = (key, [item.offset])
                    elif seen is not None and key == seen[0]:
                        seen[1].append(item.offset)
                    elif seen is None:
                        shown[group] = (key, [item.offset])
            yield item
    finally:
        await reader.aclose()


def _group(producer: models.Producer) -> tuple[str, str, str] | None:
    # Only an Activity or Standalone Activity Producer supersedes. The group is its kind, id and
    # run, so a retry of the same Activity supersedes its earlier attempt.
    match producer.kind:
        case models.ProducerKindActivity(value=activity):
            return ("activity", producer.id, activity.run_id or "")
        case models.ProducerKindStandaloneActivity(value=activity):
            return ("standalone-activity", producer.id, activity.run_id or "")
        case _:
            return None


def _epoch_key(producer: models.Producer) -> tuple[int, ...]:
    # An Activity epoch orders by scheduled time, then attempt. A reset starts the attempt count
    # again, so the time orders the attempts across it.
    match producer.kind:
        case (
            models.ProducerKindActivity(value=activity)
            | models.ProducerKindStandaloneActivity(value=activity)
        ):
            epoch = activity.epoch
            scheduled = epoch.attempt_scheduled_time if epoch else None
            attempt = epoch.attempt if epoch else None
            return (
                _n(scheduled.seconds if scheduled else None),
                _n(scheduled.nanos if scheduled else None),
                _n(attempt),
            )
        case _:
            return (0,)


def _n(value: int | None) -> int:
    return max(value or 0, 0)
