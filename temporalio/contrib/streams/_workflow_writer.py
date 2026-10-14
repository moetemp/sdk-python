"""Publishing to a Workflow's own stream through Local Activities.

Workflow code can't do I/O. :func:`workflow_writer` buffers each value in
the Workflow, and a background coroutine of the run sends what a topic has
buffered through one Local Activity, one flush at a time per topic. So the
values published while a flush runs land together as the next batch, and
batches land in the order they were published. A Local Activity costs an
Action only for the Workflow Task that carries its marker, however many
markers that task carries, so batching keeps the cost per task.

The Local Activity is the built-in publish Activity. It writes through
:func:`temporalio.contrib.streams.activity_handle` as a Local Activity
producer (see ``_local_activity``), so each flush is a producer of its own,
and a flush that runs again after a failed Workflow Task writes a higher
attempt. Readers see ``SUPERSEDED`` for that flush, then its batch again.

The Workflow converts each value with its own payload converter and passes
the payloads raw, so the codec runs once on each record body, when the Local
Activity stores it. Replay takes each flush's result from its marker and
does no store I/O.

The store plugin's Workflow interceptor drains the buffers when the run
returns, fails, is cancelled or continues as new, so records published
before the run ends land first.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any, Generic, TypeVar, overload

import temporalio.activity
from temporalio import workflow
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.common import RawValue, RetryPolicy
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._handles import StreamProducer, activity_handle
from temporalio.contrib.streams._local_activity import _WorkflowInbound
from temporalio.contrib.streams._record import Cursor
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.exceptions import ApplicationError

__all__ = [
    "PUBLISH_ACTIVITY",
    "WorkflowStreamWriter",
    "publish_activity",
    "run_then_drain",
    "workflow_writer",
]

T = TypeVar("T")

logger = logging.getLogger(__name__)

PUBLISH_ACTIVITY = "__temporal_streams_publish"
"""The name the built-in publish Activity is registered under."""

_DEFAULT_START_TO_CLOSE = timedelta(seconds=10)
# The same per-record limit the stream service keeps, checked in the Workflow
# so an oversized value fails at the call and schedules nothing.
_MAX_RECORD_BYTES = 3 << 19
_PAYLOADS_ENCODING = b"binary/protobuf"
_PAYLOADS_TYPE = Payloads.DESCRIPTOR.full_name.encode()
# Repeats of one batch on the same producer that the store answers with the
# original position, before the Activity gives up and its retry runs.
_OUTCOME_UNKNOWN_REPEATS = 3
# Errors that the same append raises again on every attempt.
_FINAL_ERRORS = (
    ValueError,
    StreamClosedError,
    StreamNotFoundError,
    StreamProducerError,
    StreamUnsupportedError,
)
# Bounds on one flush, so a burst of publishes splits into several Local
# Activities rather than one oversized input.
_MAX_BATCH_RECORDS = 1_000
_MAX_BATCH_BYTES = 2 << 20


class _Topic:
    """One topic's buffer and flush state in one run."""

    def __init__(
        self,
        name: str,
        start_to_close_timeout: timedelta | None,
        schedule_to_close_timeout: timedelta | None,
        retry_policy: RetryPolicy | None,
    ) -> None:
        self.name = name
        self.start_to_close_timeout = start_to_close_timeout
        self.schedule_to_close_timeout = schedule_to_close_timeout
        self.retry_policy = retry_policy
        self.buffer: list[Payload] = []
        # Resolved when everything buffered now has landed. Made when a caller
        # first waits on the buffer, then handed to the flush that empties it.
        self.pending: asyncio.Future[Cursor] | None = None
        self.in_flight: asyncio.Future[Cursor] | None = None
        self.flusher: asyncio.Task[None] | None = None
        self.last = BEGINNING
        self.finishing = False
        self.finished = False
        self.error: BaseException | None = None
        self.error_seen = False

    @property
    def idle(self) -> bool:
        return self.flusher is None or self.flusher.done()

    def raise_error(self) -> None:
        if self.error is not None:
            self.error_seen = True
            raise self.error

    def waiter(self) -> asyncio.Future[Cursor] | None:
        """What a caller waits on to see everything buffered now land."""
        if self.buffer or (self.finishing and not self.finished):
            if self.pending is None:
                self.pending = _future()
            return self.pending
        return self.in_flight

    def start(self) -> None:
        if self.idle:
            self.flusher = asyncio.get_running_loop().create_task(
                self._flush_all(), name=f"stream flush {self.name}"
            )

    async def _flush_all(self) -> None:
        while self.error is None and (
            self.buffer or (self.finishing and not self.finished)
        ):
            batch, finish, done = self._take()
            self.in_flight = done
            try:
                token = await workflow.execute_local_activity(
                    PUBLISH_ACTIVITY,
                    args=[self.name, finish, _raw_payloads(batch)],
                    result_type=str,
                    start_to_close_timeout=self.start_to_close_timeout,
                    schedule_to_close_timeout=self.schedule_to_close_timeout,
                    retry_policy=self.retry_policy,
                    summary=f"stream publish {self.name}",
                )
            except asyncio.CancelledError:
                # The run is being torn down, as at eviction.
                raise
            except BaseException as error:
                self._fail(error, done)
                return
            finally:
                self.in_flight = None
            if token:
                self.last = Cursor(token)
            if finish:
                self.finished = True
            if not done.done():
                done.set_result(self.last)

    def _take(self) -> tuple[list[Payload], bool, asyncio.Future[Cursor]]:
        size, count = 0, 0
        for payload in self.buffer:
            size += payload.ByteSize()
            if count and (count == _MAX_BATCH_RECORDS or size > _MAX_BATCH_BYTES):
                break
            count += 1
        batch, self.buffer = self.buffer[:count], self.buffer[count:]
        if self.buffer:
            # The rest goes in a later flush, which resolves the waiters.
            return batch, False, _future()
        done, self.pending = self.pending or _future(), None
        return batch, self.finishing, done

    def _fail(self, error: BaseException, done: asyncio.Future[Cursor]) -> None:
        self.error = error
        self.buffer = []
        for waiter in (done, self.pending):
            if waiter is not None and not waiter.done():
                waiter.set_exception(error)
        self.pending = None


def _future() -> asyncio.Future[Cursor]:
    future: asyncio.Future[Cursor] = asyncio.get_running_loop().create_future()
    # Marks a failure as retrieved when the only waiter was cancelled.
    future.add_done_callback(lambda f: f.cancelled() or f.exception())
    return future


class _RunWriters:
    """One run's topics, across all its writers."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.topics: dict[str, _Topic] = {}


# Held on the run's runtime, so the state goes with the run at eviction and is
# rebuilt in order on replay. A weak map keyed by the runtime would never drop
# it, since the state's tasks and futures refer to the runtime's event loop.
_STATE_ATTRIBUTE = "_temporal_stream_writers"


def _run_writers(*, create: bool) -> _RunWriters | None:
    runtime = workflow._Runtime.current()
    writers: _RunWriters | None = getattr(runtime, _STATE_ATTRIBUTE, None)
    if writers is None and create:
        if not _has_streams_interceptor(runtime):
            raise StreamUnsupportedError(
                "workflow_writer needs the stream store's Worker interceptor, which "
                "sends what the run buffered before it ends; build the Worker from "
                "a client that carries the store"
            )
        writers = _RunWriters(workflow.info().run_id)
        setattr(runtime, _STATE_ATTRIBUTE, writers)
    return writers


def _has_streams_interceptor(runtime: Any) -> bool:
    inbound = getattr(runtime, "_inbound", None)
    while inbound is not None:
        if isinstance(inbound, _WorkflowInbound):
            return True
        inbound = getattr(inbound, "next", None)
    return False


def _check_writable(action: str) -> None:
    if workflow.unsafe.is_read_only():
        raise workflow.ReadOnlyContextError(
            f"cannot {action} a stream in a query or an update validator"
        )


class WorkflowStreamWriter(Generic[T]):
    """Publishes to one topic of the running Workflow's own stream.

    :meth:`publish` buffers the value in the Workflow and returns at once. A
    background coroutine of the run sends the buffer through one Local
    Activity at a time per topic, so values land in the order they were
    published, and the values published during one flush land together in
    the next.
    """

    def __init__(self, topic: _Topic) -> None:
        """Prefer :func:`temporalio.contrib.streams.workflow_writer`."""
        self._topic = topic

    @property
    def topic(self) -> str:
        """The name of the topic this writer is bound to."""
        return self._topic.name

    def publish(self, value: T) -> None:
        """Buffer ``value`` for this topic.

        The value is converted here, with the Workflow's payload converter. A
        :class:`temporalio.common.RawValue` passes through pre-encoded. The
        record is visible once the Local Activity that carries it has its
        append accepted, which can be while the Workflow Task is still
        running. Call :meth:`flush` to wait for that.

        Raises:
            ValueError: The topic was already finished in this run.
            temporalio.contrib.streams.StreamError: The value's payload is
                over 1.5 MiB.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
            BaseException: The error that failed an earlier flush on this
                topic. The writer publishes nothing more.
        """
        _check_writable("publish to")
        topic = self._topic
        topic.raise_error()
        if topic.finishing:
            raise ValueError(f"topic {topic.name!r} was already finished")
        payload = workflow.payload_converter().to_payloads([value])[0]
        if payload.ByteSize() > _MAX_RECORD_BYTES:
            raise StreamError(
                f"a record of {payload.ByteSize()} bytes on {topic.name!r} is over "
                f"the per-record limit of {_MAX_RECORD_BYTES} bytes"
            )
        topic.buffer.append(payload)
        topic.start()

    async def flush(self) -> Cursor:
        """Wait until every value published so far on this topic has landed.

        Returns the cursor of the last record landed on this topic in this
        run, or ``BEGINNING`` when none has. Cancelling the wait does not
        cancel the flush.

        Raises:
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
            BaseException: The error that failed a flush on this topic, for
                example a :class:`temporalio.exceptions.ActivityError` once
                the Local Activity's retries ran out.
        """
        _check_writable("flush")
        topic = self._topic
        topic.raise_error()
        waiter = topic.waiter()
        if waiter is None:
            return topic.last
        try:
            return await asyncio.shield(waiter)
        except asyncio.CancelledError:
            raise
        except BaseException:
            topic.error_seen = True
            raise

    async def finish(self) -> Cursor:
        """Flush, then write ``FINISH`` on this topic, and return its cursor.

        Idempotent. Says this run publishes nothing more on the topic. The
        record is written by the last flush's Local Activity, as that
        flush's producer. A run that follows by Continue-as-New can publish
        on the topic again. It does not say the Workflow succeeded, and it
        does not end anyone's read.

        Raises:
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
            BaseException: The error that failed a flush on this topic.
        """
        _check_writable("finish")
        topic = self._topic
        topic.raise_error()
        if not topic.finishing:
            topic.finishing = True
            topic.start()
        return await self.flush()


@overload
def workflow_writer(
    topic: StreamTopic[T],
    *,
    start_to_close_timeout: timedelta | None = _DEFAULT_START_TO_CLOSE,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
) -> WorkflowStreamWriter[T]: ...


@overload
def workflow_writer(
    topic: str | None = None,
    *,
    start_to_close_timeout: timedelta | None = _DEFAULT_START_TO_CLOSE,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
) -> WorkflowStreamWriter[Any]: ...


def workflow_writer(
    topic: str | StreamTopic[Any] | None = None,
    *,
    start_to_close_timeout: timedelta | None = _DEFAULT_START_TO_CLOSE,
    schedule_to_close_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
) -> WorkflowStreamWriter[Any]:
    """A writer on ``topic`` of the running Workflow's own stream.

    Call it from Workflow code. All writers on one topic of a run share one
    buffer, one order and one ``FINISH``. The Local Activity options come
    from the first call for the topic in the run.

    Each flush is one Local Activity and adds one marker event to History
    when its Workflow Task completes. Its records are visible as soon as
    its append is accepted, before that task completes. The values ride in
    the Local Activity's input, not in History.

    Args:
        topic: A :func:`temporalio.contrib.streams.topic` definition, whose
            value type the writer's ``publish`` takes, or a plain string.
            Without one, the writer is on
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        start_to_close_timeout: How long one attempt of a flush may run.
            10 seconds by default.
        schedule_to_close_timeout: How long a flush may take, retries
            included. Without one, a flush retries until it succeeds.
        retry_policy: How a flush retries. Without one, it is Temporal's
            default for Activities.

    Raises:
        ValueError: ``topic`` is empty.
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator.
        temporalio.contrib.streams.StreamUnsupportedError: The Worker has no
            stream store interceptor, so it could not send the buffer before
            the run ends.
    """
    _check_writable("open a writer on")
    name, _ = resolve_topic(topic)
    writers = _run_writers(create=True)
    assert writers is not None
    state = writers.topics.get(name)
    if state is None:
        state = writers.topics[name] = _Topic(
            name, start_to_close_timeout, schedule_to_close_timeout, retry_policy
        )
    return WorkflowStreamWriter(state)


async def _drain() -> None:
    writers = _run_writers(create=False)
    if writers is None:
        return
    while True:
        busy = [t for t in writers.topics.values() if not t.idle or t.buffer]
        for topic in writers.topics.values():
            if topic.error is not None and not topic.error_seen:
                topic.raise_error()
        if not busy:
            return
        for topic in busy:
            waiter = topic.waiter()
            if waiter is None and topic.flusher is not None:
                await asyncio.shield(topic.flusher)
            elif waiter is not None:
                try:
                    await asyncio.shield(waiter)
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    pass


async def run_then_drain(run: Any) -> Any:
    """Await the Workflow's run, then send what its writers buffered.

    The buffers drain when the run returns, continues as new, fails the
    Workflow or is cancelled. A failure of the drain fails the Workflow in
    place of a return or a Continue-as-New, and is logged in place of a
    failure. An error that fails only the Workflow Task does not drain,
    since that task runs again.
    """
    try:
        result = await run
    except workflow.ContinueAsNewError:
        await _drain()
        raise
    except BaseException as error:
        if _ends_the_workflow(error):
            try:
                await _drain()
            except Exception:
                logger.warning(
                    "Stream records buffered before the Workflow failed did not land",
                    exc_info=True,
                )
            except BaseException:
                # The run is being evicted. The original error still ends it.
                pass
        raise
    await _drain()
    return result


def _ends_the_workflow(error: BaseException) -> bool:
    if isinstance(error, asyncio.CancelledError):
        # A cancelled run with no cancel request is being evicted.
        return workflow.cancellation_reason() is not None
    return workflow.is_failure_exception(error)


def _raw_payloads(payloads: list[Payload]) -> RawValue:
    # Encoded here rather than by the converter, so a converter without
    # protobuf support still carries the batch.
    return RawValue(
        Payload(
            metadata={"encoding": _PAYLOADS_ENCODING, "messageType": _PAYLOADS_TYPE},
            data=Payloads(payloads=payloads).SerializeToString(),
        )
    )


@temporalio.activity.defn(name=PUBLISH_ACTIVITY)
async def publish_activity(topic: str, finish: bool, values: RawValue) -> str:
    """Append a Workflow's batch to its own stream, as this Activity attempt.

    A flush runs it as a Local Activity, so it writes as a Local Activity
    producer. The store plugin registers it on every Worker built from a
    client that carries a store. Returns the cursor token of the last record
    written.
    """
    if values.payload.metadata.get("encoding") != _PAYLOADS_ENCODING:
        raise ApplicationError(
            "the stream publish input is not a Payloads message", non_retryable=True
        )
    batch = [RawValue(p) for p in Payloads.FromString(values.payload.data).payloads]
    producer = activity_handle().producer(topic=topic)
    try:
        cursor = await _append(producer, batch) if batch else BEGINNING
        if finish:
            cursor = await producer.finish()
    except _FINAL_ERRORS as error:
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error
    return cursor.token


async def _append(producer: StreamProducer[Any], batch: list[RawValue]) -> Cursor:
    for repeat in range(1, _OUTCOME_UNKNOWN_REPEATS):
        try:
            return await producer.append(*batch)
        except StreamOutcomeUnknownError:
            # The producer kept its sequence, so the same batch again is
            # written once.
            await asyncio.sleep(0.1 * 2**repeat)
    return await producer.append(*batch)
