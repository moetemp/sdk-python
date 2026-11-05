"""The items that a Stream read gives."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, TypeVar

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

    SUPERSEDED = "superseded"
    """A :py:class:`Superseded` item."""


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

    offset: int
    """Position of the Record in the Stream. Offsets are ordered and unique, with gaps."""

    producer: models.Producer | None
    """Producer that wrote the Record."""


@dataclass(frozen=True)
class Superseded:
    """An item that withdraws the Records of a failed attempt.

    Every reader gets it at the same point in the Stream. A live view removes the Records in
    :py:attr:`withdrawn_offsets`. A view that keeps Records from an earlier read, such as from
    before a reconnect, checks them with :py:meth:`withdraws`.

    The next write of a run to a Stream withdraws the Records that the run's Local Activities
    wrote there in Workflow Tasks that never completed. A Stream that the run never writes
    again keeps them.

    .. warning::
        This API is experimental.
    """

    kind: Literal[ItemKind.SUPERSEDED] = field(default=ItemKind.SUPERSEDED, init=False)
    """Always :py:attr:`ItemKind.SUPERSEDED`."""

    offset: int
    """Position of the item in the Stream."""

    facts: models.StreamSuperseded
    """The withdrawal facts from the Store. Its ``writer`` gives the Records that it withdraws."""

    withdrawn_offsets: tuple[int, ...] = ()
    """The offsets of the Records that this read gave and this item withdraws, in order.

    A read gives each offset once. It doesn't include Records from before the read's start.
    """

    def withdraws(self, record: Record[Any] | models.Producer) -> bool:
        """Whether this item withdraws an earlier Record, or the Records of a Producer.

        It answers for any earlier Record, such as one that an app showed before a resume,
        from the Producer and epoch that each Record carries. So an app can keep only the
        Producer of each Record that it shows.
        """
        producer = record.producer if isinstance(record, Record) else record
        return withdraws(self.facts, producer)


# The epoch rules match the crate's epoch keys. A field compares in the order of the key, and an
# empty or negative field compares as zero.
def _n(value: int | None) -> int:
    return max(value or 0, 0)


def _time(value: models.Timestamp | None) -> tuple[int, int]:
    if value is None:
        return (0, 0)
    return (_n(value.seconds), _n(value.nanos))


_TaskKey = tuple[int, int, tuple[int, int]]


def _task_key(task: models.WorkflowTaskEpoch | None) -> _TaskKey:
    if task is None:
        return (0, 0, (0, 0))
    return (_n(task.started_event_id), _n(task.attempt), _time(task.started_time))


def _task_of(epoch: models.ProducerEpoch) -> models.WorkflowTaskEpoch | None:
    # A Workflow's own task, or the task that dispatched a Local Activity. The other kinds have
    # none.
    match epoch.kind:
        case models.ProducerEpochKindWorkflow(value=task):
            return task
        case models.ProducerEpochKindLocalActivity(value=local):
            return local.task
        case _:
            return None


def _epoch_key(epoch: models.ProducerEpoch) -> tuple[Any, ...]:
    scheduled: models.Timestamp | None = None
    attempt: int | None = None
    match epoch.kind:
        case models.ProducerEpochKindLocalActivity(value=local):
            attempt = local.attempt
        case (
            models.ProducerEpochKindActivity(value=activity)
            | models.ProducerEpochKindStandaloneActivity(value=activity)
        ):
            scheduled, attempt = activity.attempt_scheduled_time, activity.attempt
        case models.ProducerEpochKindClient(value=client):
            attempt = client.attempt
        case _:
            pass
    return (*_task_key(_task_of(epoch)), _time(scheduled), _n(attempt))


def _kind(epoch: models.ProducerEpoch | None) -> type[Any] | None:
    return type(epoch.kind) if epoch is not None and epoch.kind is not None else None


def is_client(producer: models.Producer) -> bool:
    """Whether the Producer is a client, whose newer epoch withdraws nothing."""
    return _kind(producer.epoch) is models.ProducerEpochKindClient


def producer_key(producer: models.Producer) -> tuple[Any, ...]:
    """The Producer fields that :py:func:`withdraws` reads, so equal keys are withdrawn together."""
    epoch = _epoch_key(producer.epoch) if producer.epoch is not None else None
    return (_kind(producer.epoch), producer.id, producer.run_id or "", epoch)


def _of_run(producer: models.Producer, by: models.Producer) -> bool:
    # The run's Workflow Producer, or one of its Local Activities.
    kind = _kind(producer.epoch)
    if kind is models.ProducerEpochKindWorkflow:
        return producer.id == by.id
    if kind is models.ProducerEpochKindLocalActivity:
        return (producer.run_id or "") == (by.run_id or "")
    return False


def withdraws(facts: models.StreamSuperseded, producer: models.Producer | None) -> bool:
    """Whether a Superseded item withdraws a Record of ``producer``.

    The rule matches the crate's, so every SDK withdraws the same Records.
    """
    writer = facts.writer
    if producer is None or writer is None:
        return False
    if writer.withdraw_run and producer.run_id == writer.withdraw_run:
        return True
    by, old = writer.producer, producer.epoch
    epoch = by.epoch if by is not None else None
    if by is None or epoch is None or old is None:
        return False
    kind = _kind(epoch)
    if kind is None or kind is models.ProducerEpochKindClient:
        # A client's newer epoch only fences. Its earlier Records are real messages, not a
        # failed attempt's.
        return False
    if kind is not models.ProducerEpochKindWorkflow:
        return (
            _kind(old) is kind
            and producer.id == by.id
            and _epoch_key(old) < _epoch_key(epoch)
        )
    if not _of_run(producer, by):
        return False
    task, old_task = _task_of(epoch), _task_of(old)
    started = (old_task.started_event_id if old_task else None) or 0
    if started in (writer.failed_tasks or []):
        return True
    completed = writer.previous_completed_task
    after = (completed.started_event_id if completed else None) or 0
    if after < started < ((task.started_event_id if task else None) or 0):
        return True
    # No task in the range can have completed, so a lower attempt at either end goes too.
    ends = [task] if completed is None else [task, completed]
    return any(
        started == ((end.started_event_id if end else None) or 0)
        and _task_key(old_task) < _task_key(end)
        for end in ends
    )
