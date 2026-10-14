"""Publishing to a Workflow's own stream through a built-in Activity.

Workflow code can't do I/O, so each publish call runs one Activity that
appends to the stream of the Workflow that scheduled it. The Activity writes
through :func:`temporalio.contrib.streams.activity_handle`, so it writes as
itself and a retry reaches readers as ``SUPERSEDED``.

The Workflow converts each value with its own payload converter and sends
the payloads as one ``Payloads`` message, which the Activity takes raw. So
the Activity's converter never sees the user's types, and the payload codec
runs once on each record body, when the Activity stores it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import timedelta
from typing import Any, Generic, TypeVar, overload

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.common import RetryPolicy
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._errors import (
    StreamError,
)
from temporalio.contrib.streams._record import Cursor
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._workflow_writer import (
    _MAX_RECORD_BYTES,
    PUBLISH_ACTIVITY,
    _raw_payloads,
)

__all__ = [
    "WorkflowActivityStreamWriter",
    "workflow_activity_writer",
]

T = TypeVar("T")

_DEFAULT_START_TO_CLOSE = timedelta(seconds=30)


class _RunTopics:
    """One run's publish state on its topics, across all its writers."""

    def __init__(self) -> None:
        self.locks: dict[str, asyncio.Lock] = {}
        self.last: dict[str, Cursor] = {}
        self.finished: set[str] = set()

    def lock(self, topic: str) -> asyncio.Lock:
        lock = self.locks.get(topic)
        if lock is None:
            lock = self.locks[topic] = asyncio.Lock()
        return lock


# Held on the run's runtime, so the state goes with the run at eviction and is
# rebuilt in order on replay. A weak map keyed by the runtime would never drop
# it, since a lock that was waited on refers to the runtime's event loop.
_STATE_ATTRIBUTE = "_temporal_stream_activity_writers"


def _run_topics() -> _RunTopics:
    runtime = workflow._Runtime.current()
    topics: _RunTopics | None = getattr(runtime, _STATE_ATTRIBUTE, None)
    if topics is None:
        topics = _RunTopics()
        setattr(runtime, _STATE_ATTRIBUTE, topics)
    return topics


class WorkflowActivityStreamWriter(Generic[T]):
    """Publishes to one topic of the running Workflow's own stream, through an Activity.

    Each call schedules the built-in publish Activity and waits for it. Calls
    on one topic of a run go one at a time, in the order they were made, so
    records land in that order. The Activity runs on a Worker whose client
    carries a stream store, and its append is visible once the store accepts
    it.
    """

    def __init__(
        self,
        topic: str,
        *,
        task_queue: str | None,
        start_to_close_timeout: timedelta | None,
        schedule_to_close_timeout: timedelta | None,
        retry_policy: RetryPolicy | None,
    ) -> None:
        """Prefer :func:`temporalio.contrib.streams.workflow_activity_writer`."""
        self._topic = topic
        self._task_queue = task_queue
        self._start_to_close_timeout = start_to_close_timeout
        self._schedule_to_close_timeout = schedule_to_close_timeout
        self._retry_policy = retry_policy

    @property
    def topic(self) -> str:
        """The name of the topic this writer is bound to."""
        return self._topic

    async def publish(self, value: T) -> Cursor:
        """Append ``value`` to this topic and return its cursor.

        The same as ``publish_batch([value])``.
        """
        return await self.publish_batch([value])

    async def publish_batch(self, values: Sequence[T]) -> Cursor:
        """Append ``values`` as one batch and return the cursor of its last record.

        The values are converted here, with the Workflow's payload converter.
        A :class:`temporalio.common.RawValue` passes through pre-encoded. The
        batch lands whole and in order, in one Activity.

        The Activity writes as itself: its Activity id with this run's id,
        and its Temporal attempt. A retried attempt starts a new sequence,
        and readers see ``SUPERSEDED`` before its first record, so they can
        drop what the failed attempt wrote. An empty batch schedules nothing
        and returns the cursor of this run's last publish on the topic, or
        ``BEGINNING``.

        All values ride in the Activity's input, so a batch is bounded by
        the server's payload size limit.

        Raises:
            ValueError: The topic was already finished in this run.
            temporalio.contrib.streams.StreamError: A value's payload is over
                1.5 MiB. Nothing is scheduled.
            temporalio.exceptions.ActivityError: The Activity failed for good,
                for example because the run chain is closed or its retries
                ran out.
        """
        state = _run_topics()
        async with state.lock(self._topic):
            if self._topic in state.finished:
                raise ValueError(f"topic {self._topic!r} was already finished")
            if not values:
                return state.last.get(self._topic, BEGINNING)
            payloads = workflow.payload_converter().to_payloads(list(values))
            for payload in payloads:
                if payload.ByteSize() > _MAX_RECORD_BYTES:
                    raise StreamError(
                        f"a record of {payload.ByteSize()} bytes on {self._topic!r} "
                        f"is over the per-record limit of {_MAX_RECORD_BYTES} bytes"
                    )
            return await self._run(state, payloads, finish=False)

    async def finish(self) -> Cursor:
        """Write ``FINISH`` on this topic and return its cursor. Idempotent.

        Says this run publishes nothing more on the topic. The record is
        written by the publish Activity, as that Activity's producer. A run
        that follows by Continue-as-New can publish on the topic again. It
        does not say the Workflow succeeded, and it does not end anyone's
        read.

        Raises:
            temporalio.exceptions.ActivityError: The Activity failed for good.
        """
        state = _run_topics()
        async with state.lock(self._topic):
            if self._topic in state.finished:
                return state.last.get(self._topic, BEGINNING)
            cursor = await self._run(state, [], finish=True)
            state.finished.add(self._topic)
            return cursor

    async def _run(
        self, state: _RunTopics, payloads: list[Payload], *, finish: bool
    ) -> Cursor:
        token = await workflow.execute_activity(
            PUBLISH_ACTIVITY,
            args=[self._topic, finish, _raw_payloads(payloads)],
            result_type=str,
            task_queue=self._task_queue,
            start_to_close_timeout=self._start_to_close_timeout,
            schedule_to_close_timeout=self._schedule_to_close_timeout,
            retry_policy=self._retry_policy,
        )
        cursor = Cursor(token)
        state.last[self._topic] = cursor
        return cursor


@overload
def workflow_activity_writer(
    topic: StreamTopic[T],
    *,
    task_queue: str | None = None,
    start_to_close_timeout: timedelta | None = _DEFAULT_START_TO_CLOSE,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
) -> WorkflowActivityStreamWriter[T]: ...


@overload
def workflow_activity_writer(
    topic: str | None = None,
    *,
    task_queue: str | None = None,
    start_to_close_timeout: timedelta | None = _DEFAULT_START_TO_CLOSE,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
) -> WorkflowActivityStreamWriter[Any]: ...


def workflow_activity_writer(
    topic: str | StreamTopic[Any] | None = None,
    *,
    task_queue: str | None = None,
    start_to_close_timeout: timedelta | None = _DEFAULT_START_TO_CLOSE,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
) -> WorkflowActivityStreamWriter[Any]:
    """A writer on ``topic`` of the running Workflow's own stream, through an Activity.

    Call it from Workflow code. Every call returns a new writer, and all
    writers in a run share the order of calls on a topic and which topics
    were finished.

    A publish adds the Activity's events to History, as any Activity does,
    and its records are visible once the Activity's append is accepted. The
    Activity is scheduled only after the Workflow Task that scheduled it is
    accepted, so a record never shows before that task.

    Args:
        topic: A :func:`temporalio.contrib.streams.topic` definition, whose
            value type the writer's ``publish`` takes, or a plain string.
            Without one, the writer is on
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        task_queue: Where the publish Activity runs. Without one it is the
            Workflow's task queue. Its Workers need a stream store on their
            client.
        start_to_close_timeout: How long one attempt of the publish Activity
            may run. 30 seconds by default.
        schedule_to_close_timeout: How long a publish may take, retries
            included. Without one, the publish retries until it succeeds.
        retry_policy: How the publish Activity retries. Without one, it is
            Temporal's default for Activities.

    Raises:
        ValueError: ``topic`` is empty.
    """
    name, _ = resolve_topic(topic)
    return WorkflowActivityStreamWriter(
        name,
        task_queue=task_queue,
        start_to_close_timeout=start_to_close_timeout,
        schedule_to_close_timeout=schedule_to_close_timeout,
        retry_policy=retry_policy,
    )
