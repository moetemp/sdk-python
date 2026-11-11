"""Opt-in client-side supersession for Stream reads.

Import it explicitly. It is not part of the base ``temporalio.streams`` surface::

    from temporalio.streams.superseded import Superseded, with_superseded

The base read gives ``Record | Closed`` only. :py:func:`with_superseded` wraps a reader and adds
:py:class:`Superseded` items, computed from the ``Producer`` epoch that each Record carries, with
no Store facts. It holds the read's view in memory, so it is not for very large Streams.

One rule covers every kind of Producer. A Producer is one unit of work, and an epoch is one
attempt at it. A Record whose epoch is higher than the highest one seen for its Producer withdraws
that Producer's Records at the lower epochs. The Store fences each kind the same way, so a lower
epoch can't write after a higher one.

- An Activity, a Standalone Activity or a Local Activity runs again only when its earlier attempt
  failed or its result was never recorded. A Local Activity's Producer is one call of a run, so a
  Workflow append whose Workflow Task failed and then retried withdraws the first copy.
- A named client from :py:func:`temporalio.streams.producer` that writes at a higher epoch
  withdraws what its lower epochs wrote, such as before a restart. A client with no name has an
  id of its own and epoch 1, so nothing withdraws its Records.
- A Workflow Producer only closes, so it has no Record to withdraw.

The limits: the first copy is visible until the retry writes, a retry that never writes as that
Producer leaves the earlier Records in place, and both attempts must be in one read. Also, the
helper takes a higher epoch as a new attempt at the same work. So a retry that goes on from where
the failed attempt stopped, such as an Activity that resumes from its heartbeat details, still
withdraws what the failed attempt wrote. Read such a Stream without the helper.

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
    """An item that withdraws the Records of a Producer's lower epochs.

    :py:func:`with_superseded` yields it when a Record has the same Producer as earlier Records and
    a strictly higher epoch, for every kind of Producer. A live view removes the Records in
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
    ``Superseded`` item before a Record whose epoch is higher than the highest one seen for its
    Producer, with the offsets of that Producer's Records at the lower epochs. The rule is the
    same for every kind of Producer. See :py:mod:`temporalio.streams.superseded` for what it
    means for each kind, and for its limits.

    It takes a higher epoch as a new attempt at the same work. So a retry that goes on from where
    the failed attempt stopped, such as an Activity that resumes from its heartbeat details, still
    withdraws what the failed attempt wrote. Read such a Stream without the helper.
    """
    # The offsets of the Records shown for each Producer, at its highest epoch so far. A higher
    # epoch supersedes them.
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
    # The Store fences by kind and Producer id, so the group is the same, with the run where the
    # kind has one. A kind of a newer server that this SDK doesn't know is skipped.
    match producer.kind:
        case models.ProducerKindWorkflow(value=workflow):
            return ("workflow", producer.id, workflow.run_id)
        case models.ProducerKindLocalActivity(value=local):
            return ("local-activity", producer.id, local.run_id)
        case models.ProducerKindWorkflowActivity(value=activity):
            return ("workflow-activity", producer.id, activity.run_id or "")
        case models.ProducerKindStandaloneActivity(value=activity):
            return ("standalone-activity", producer.id, activity.run_id or "")
        case models.ProducerKindClient():
            return ("client", producer.id, "")
        case _:
            return None


def _epoch_key(producer: models.Producer) -> tuple[int, ...]:
    # The crate's epoch order: the Workflow Task's started event id, attempt and started time,
    # then the scheduled time, then the attempt. A field that the kind doesn't have is zero.
    task: models.WorkflowTaskEpoch | None = None
    scheduled: models.Timestamp | None = None
    attempt = 0
    match producer.kind:
        case models.ProducerKindWorkflow(value=workflow):
            task = workflow.epoch
        case models.ProducerKindLocalActivity(value=local):
            task, attempt = local.epoch.task, local.epoch.attempt
        case (
            models.ProducerKindWorkflowActivity(value=activity)
            | models.ProducerKindStandaloneActivity(value=activity)
        ):
            scheduled, attempt = (
                activity.epoch.attempt_scheduled_time,
                activity.epoch.attempt,
            )
        case models.ProducerKindClient(value=client):
            attempt = client.epoch.attempt
        case _:
            pass
    started = task.started_time if task is not None else None
    return (
        _n(task.started_event_id if task is not None else None),
        _n(task.attempt if task is not None else None),
        _n(started.seconds if started is not None else None),
        _n(started.nanos if started is not None else None),
        _n(scheduled.seconds if scheduled is not None else None),
        _n(scheduled.nanos if scheduled is not None else None),
        _n(attempt),
    )


def _n(value: int | None) -> int:
    # A negative field compares as zero, as in the crate.
    return max(value or 0, 0)
