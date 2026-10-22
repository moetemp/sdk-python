"""The items that a Stream read gives."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from temporalio.api.streamservice.v1 import ProducerKind
from temporalio.contrib.streams.system import models

T = TypeVar("T")


class End(enum.Enum):
    """The type of :py:data:`END`."""

    END = "END"


END = End.END
"""A ``from_offset`` that starts a read after the newest Record, for new Records only."""


@dataclass(frozen=True)
class Record(Generic[T]):
    """A data Record of a Stream.

    .. warning::
        This API is experimental.
    """

    topic: str
    """Topic of the Record."""

    data: T
    """Value of the Record, decoded with the Topic's type."""

    offset: int
    """Place of the Record in the Stream. Ordered and unique, with gaps."""

    producer: models.Producer | None
    """Producer that wrote the Record."""


@dataclass(frozen=True)
class Superseded:
    """A failed attempt's Records are withdrawn.

    It comes at the same point for every Reader. Use :py:meth:`withdraws` to find the earlier
    Records that it withdraws.

    .. warning::
        This API is experimental.
    """

    offset: int
    """Place of the item in the Stream."""

    supersession: models.Supersession
    """Which earlier Records the item withdraws."""

    def withdraws(self, record: Record[Any]) -> bool:
        """Whether this item withdraws an earlier Record."""
        return withdraws(self.supersession, record.producer)


@dataclass(frozen=True)
class Closed:
    """The last item of a closed Stream.

    .. warning::
        This API is experimental.
    """

    offset: int
    """Place of the close in the Stream."""

    result: Any | None
    """Result that the close carries, decoded with no type. None if it has none."""

    failure: BaseException | None
    """Failure that the close carries. None if it has none."""


# Fields compare in the order of the proto. An empty or negative field compares as zero, as in
# the crate's epoch keys.
def _n(value: int | None) -> int:
    return max(value or 0, 0)


def _time(value: models.Timestamp | None) -> tuple[int, int]:
    if value is None:
        return (0, 0)
    return (_n(value.seconds), _n(value.nanos))


def _task(epoch: models.ProducerEpoch) -> tuple[int, int, tuple[int, int]]:
    return (
        _n(epoch.task_started_event_id),
        _n(epoch.task_attempt),
        _time(epoch.task_started_time),
    )


def _epoch(epoch: models.ProducerEpoch) -> tuple[Any, ...]:
    return (*_task(epoch), _time(epoch.attempt_scheduled_time), _n(epoch.attempt))


def _completed(task: models.WorkflowTaskEpoch) -> tuple[int, int, tuple[int, int]]:
    return (_n(task.started_event_id), _n(task.attempt), _time(task.started_time))


def _of_run(producer: models.Producer, supersession: models.Supersession) -> bool:
    if producer.kind == ProducerKind.PRODUCER_KIND_WORKFLOW:
        return producer.id == supersession.producer_id
    if producer.kind == ProducerKind.PRODUCER_KIND_LOCAL_ACTIVITY:
        return (producer.run_id or "") == (supersession.run_id or "")
    return False


def withdraws(
    supersession: models.Supersession, producer: models.Producer | None
) -> bool:
    """Whether a Superseded item withdraws a Record of ``producer``.

    It is the same rule as the crate's, so every SDK withdraws the same Records.
    """
    if producer is None:
        return False
    if supersession.withdraw_run and producer.run_id == supersession.withdraw_run:
        return True
    if supersession.epoch is None or producer.epoch is None:
        return False
    if supersession.producer_kind != ProducerKind.PRODUCER_KIND_WORKFLOW:
        return (
            (producer.kind or 0) == (supersession.producer_kind or 0)
            and producer.id == supersession.producer_id
            and _epoch(producer.epoch) < _epoch(supersession.epoch)
        )
    if not _of_run(producer, supersession):
        return False
    task = _task(supersession.epoch)
    old = _task(producer.epoch)
    started = producer.epoch.task_started_event_id or 0
    if started in (supersession.failed_tasks or []):
        return True
    completed = supersession.previous_completed_task
    after = (completed.started_event_id or 0) if completed is not None else 0
    if after < started < (supersession.epoch.task_started_event_id or 0):
        return True
    if started == (supersession.epoch.task_started_event_id or 0) and old < task:
        return True
    if completed is None or started != (completed.started_event_id or 0):
        return False
    if (completed.attempt or 0) > 0:
        return old < _completed(completed)
    # Core gives the completed task with attempt 0 when it no longer has the attempt. The
    # attempts of one task start in time order, so the started time still tells the lower ones.
    return old[2] < _time(completed.started_time)
