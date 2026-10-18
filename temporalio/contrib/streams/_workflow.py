"""Reading a stream inside Workflow code, through Local Activities.

Workflow code can't do I/O. :func:`workflow_reader` reads in batches, each
one a Local Activity that runs the stream runtime's read and returns the records with
their bodies through the codec. Replay takes each batch from its marker and
does no I/O, so user code sees the same records, every kind included.

The reader sleeps while the stream is idle. Each read names the running
Workflow as the subscriber, under a subscription id that replay gives back.
When a read that waited ``idle_wait`` comes back empty, the stream runtime attaches the
Workflow to the stream's notifier as an idle wake target, once, and asks the
notifier for one wake past ``wake_after``, the counter of the read's
position, all in the read's Local Activity. A read that finds records asks
for nothing. So the reader gets one wake per idle stretch, and none while it
reads. It waits, with no Local Activity and no timer, until a wake for its
subscription carries a higher counter, a recheck, a close, or a request to
attach again. The server records each wake on the next Workflow Task
scheduled event, so a wake adds no event.
"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Generic, TypeVar, overload

import temporalio.activity
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure
from temporalio.bridge.proto.streams import (
    ReadRangeRequest,
    ReadRecord,
    ReadRequest,
    ReadResponse,
    ReadSubscriber,
    SubscribeRequest,
    UnsubscribeRequest,
)
from temporalio.common import RawValue
from temporalio.contrib.streams._cursor import BEGINNING, END
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamRecordError,
    StreamRefusedError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._handles import StreamHandle
from temporalio.contrib.streams._local_activity import (
    require_local_activity,
)
from temporalio.contrib.streams._plugin import (
    StreamStorePlugin,
    call,
    store_for_client,
)
from temporalio.contrib.streams._record import Cursor, StreamRecord
from temporalio.contrib.streams._ref import StreamRef, owner_context
from temporalio.contrib.streams._replay_reads import (
    ReadChain,
    batch_payload,
    is_chained,
    is_positions,
    positions_payload,
)
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wakes import register_wake_context, wake_state
from temporalio.contrib.streams._wire import decode_or_mark, to_record
from temporalio.converter._payload_codec import _apply_to_failure_payloads
from temporalio.exceptions import ApplicationError

__all__ = [
    "READ_ACTIVITY",
    "StreamBatch",
    "StreamPosition",
    "TaskCutoff",
    "WorkflowStreamReader",
    "read_activity",
    "workflow_reader",
]

T = TypeVar("T")

READ_ACTIVITY = "__temporal_streams_read"
"""The name the built-in read Activity is registered under."""

_READ_RUN_ID_KEY = "temporal.io/stream-read-run-id"
_READ_BY_RUN_ID_KEY = "temporal.io/stream-read-by-run-id"
_RESPONSE_ENCODING = b"binary/protobuf"
_RESPONSE_TYPE = ReadResponse.DESCRIPTOR.full_name.encode()
# Errors that the same read raises again on every attempt. The Activity
# fails for good with the error's name as its type.
_FINAL_ERRORS: tuple[type[StreamError], ...] = (
    StreamClosedError,
    StreamCursorError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamRecordError,
    StreamRefusedError,
    StreamUnsupportedError,
)
# How long a reader of an Activity's stream waits for the notifier's close
# after its read ended with the run. The notifier checks the owner every 30
# seconds by default, and debounces its deliveries.
_OWNER_CLOSE_WAIT = timedelta(seconds=60)
# Set once this process warned that a sleeping reader wakes only on the recheck.
_warned_no_notify = False
# Held on the run's runtime: how many readers the run made. Replay makes them
# in the same order, so each reader gets the same number.
_READERS_ATTRIBUTE = "_temporal_stream_readers"


@dataclass(frozen=True)
class StreamPosition:
    """Where a Workflow reader stands, as data the next run can resume from.

    Pass it as ``after=`` to :func:`workflow_reader`, for example in the
    input of a Continue-as-New. It carries the cursor of the last record
    the reader delivered and the stream runtime's read state, which holds the producer
    attempts the reader saw, so the next run still reports a retry as
    ``SUPERSEDED``.
    """

    cursor: str
    state: str = ""
    """The stream runtime's read state, in base64."""


@dataclass(frozen=True)
class TaskCutoff:
    """When a Workflow reader ends the Workflow Task it reads in.

    A reader that finds records on every read never blocks on anything but
    its own Local Activities, so its Workflow Task would complete only at
    the Local Activity heartbeat, and the Activities and timers that user
    code started would wait until then. Before each read, the reader checks
    the commands the task sent so far and the time since the task started.
    Past either bound, it asks Core to complete the task with the read's
    command, and the read runs on into the next task.

    User code never waits on the cutoff, so a cutoff taken from the clock
    changes no command, and replay is safe.
    """

    commands: int = 100
    """The commands one Workflow Task may send before the reader cuts it."""
    time: timedelta = timedelta(seconds=2)
    """How long one Workflow Task may run before the reader cuts it."""


def cut_task(cutoff: TaskCutoff | None) -> None:
    """Ask Core to complete the running Workflow Task once it passed ``cutoff``."""
    if cutoff is None:
        return
    runtime: Any = workflow._Runtime.current()
    commands, seconds = runtime._task_progress()
    if commands >= cutoff.commands or seconds >= cutoff.time.total_seconds():
        runtime._request_task_heartbeat()


@dataclass(frozen=True)
class StreamBatch(Generic[T]):
    """The records one read delivered, in stream order, and the position after them."""

    records: list[StreamRecord[T]]
    position: StreamPosition


@dataclass
class _ReadInput:
    """What the read Activity gets. Plain data, so the default converter carries it."""

    workflow_id: str
    chain_run_id: str
    topic: str
    after: str
    state: str
    wait_ms: int
    max_records: int
    subscription_id: str = ""
    """Set to name the running Workflow as the read's subscriber, so that an empty read
    attaches it and asks for one wake."""
    subscribe: bool = False
    """With ``subscription_id``, attach the running Workflow before the read, also when
    the read finds records."""
    generation: int = 0
    """How many times the reader attached again after a ``resubscribe`` wake."""
    read_run_id: str | None = None
    """The run the read names to the stream runtime, which the first read finds."""
    unsubscribe: bool = False
    """Detach ``subscription_id`` and read nothing."""
    positions_only: bool = False
    """Return the read's range and digest in place of its records."""
    reader_id: str = ""
    """The reader's id in its run, which names its hold on the records."""
    read_seq: int = 0
    """How many reads of the reader returned, this one included."""
    chained: bool = False
    """Set when the previous read's marker holds this read's input state."""
    owner_kind: str = "workflow"
    """The owner kind of the stream, as :attr:`StreamRef.kind`."""
    activity_id: str = ""
    """The owning Activity, for an ``"activity"`` or ``"workflow_activity"`` owner."""
    max_bytes: int = 0
    """The most stored bytes of the records past the first, 0 for no bound."""


@dataclass
class _ReaderState:
    cursor: str
    state: bytes
    number: int
    generation: int = 0
    read_run_id: str | None = None
    # The notifier counter of the last empty read, while the reader sleeps.
    sleep_after: int | None = None
    # Set once a read may have attached the reader, so a close detaches it.
    attached: bool = False
    # Set when the next read attaches before it reads.
    subscribe_now: bool = False
    attached_run_id: str = ""
    done: bool = False
    # Set when the read of an Activity's stream ended because the run ended,
    # with no close.
    owner_ended: bool = False
    # Set once the reader attached to hear how an Activity run ended.
    owner_attached: bool = False
    close_failure: Failure | None = None
    failure_raised: bool = False
    reads: int = 0
    # The run that made the last read, when that read's marker holds its
    # answer state, so the next read can leave its input state out.
    chain_run: str = ""
    # What a positions-only reader read since its first read, which the store
    # holds for it until its run ends.
    held_bytes: int = 0
    first_read_at: datetime | None = None
    # The records of the last read not delivered yet, after one that did not
    # convert, and the cursor past the read.
    pending: list[ReadRecord] = field(default_factory=list)
    pending_end: str = ""
    pending_error: StreamRecordError | None = None


class WorkflowStreamReader(Generic[T]):
    """Reads a stream in Workflow code, one batch per Local Activity.

    Use it as an async iterator of :class:`StreamBatch`, inside
    ``async with``, so that a reader stopped early detaches from the
    stream's notifier::

        async with workflow_reader(ref, topic=EVENTS) as reader:
            async for batch in reader:
                for record in batch.records:
                    ...

    A batch holds every record kind, ``SUPERSEDED`` and ``FINISH`` too, since
    replay must give user code the same records.
    :class:`temporalio.contrib.streams.SupersessionTracker` names the records
    each ``SUPERSEDED`` withdraws. Iteration ends when the stream is closed
    and every record was delivered. When the stream was closed with a
    failure, the iteration raises it after the last batch.
    """

    def __init__(
        self,
        ref: StreamRef,
        topic: str,
        result_type: type | None,
        after: Cursor | StreamPosition,
        idle_wait: timedelta,
        max_records: int,
        start_to_close_timeout: timedelta,
        cutoff: TaskCutoff | None,
        replay_from_store: bool = True,
        *,
        max_bytes: int = 1 << 20,
    ) -> None:
        """Prefer :func:`temporalio.contrib.streams.workflow_reader`."""
        self._cutoff = cutoff
        self._replay_from_store = replay_from_store
        self._max_bytes = max_bytes
        self._ref = ref
        self._topic = topic
        self._result_type = result_type
        self._idle_wait = idle_wait
        self._max_records = max_records
        self._start_to_close_timeout = start_to_close_timeout
        if isinstance(after, StreamPosition):
            cursor, state = after.cursor, base64.b64decode(after.state)
        else:
            cursor, state = after.token, b""
        self._state = _ReaderState(cursor, state, _next_reader())
        register_wake_context(
            self.subscription_id,
            owner_context(workflow.info().namespace, ref.workflow_id, ref.activity_id),
        )

    @property
    def topic(self) -> str:
        """The topic this reader reads."""
        return self._topic

    @property
    def subscription_id(self) -> str:
        """The id the reader's wakes name, unique among this run's readers."""
        return str(self._state.number)

    @property
    def position(self) -> StreamPosition:
        """Where the reader stands: past the last record it delivered."""
        return StreamPosition(
            self._state.cursor, base64.b64encode(self._state.state).decode()
        )

    def continue_as_new_suggested(
        self,
        max_bytes: int = 256 << 20,
        max_age: timedelta = timedelta(days=1),
    ) -> bool:
        """Whether the run should Continue-as-New to free what this reader holds.

        A reader with ``replay_from_store`` holds every record after its
        first read in the store until its run ends, since the run may replay
        its reads. Each hold costs store memory and append time. This is
        ``True`` once the records it read pass ``max_bytes``, or once
        ``max_age`` of Workflow time passed since its first read. The counts
        come from the reader's own reads, so replay gives the same answer. A
        reader that records its batches in History holds nothing, and this
        is ``False``.

        Pass :attr:`position` as ``after`` to the next run's reader.
        """
        state = self._state
        if state.first_read_at is None:
            return False
        return (
            state.held_bytes >= max_bytes
            or workflow.now() - state.first_read_at >= max_age
        )

    def __aiter__(self) -> WorkflowStreamReader[T]:
        """The reader is its own iterator."""
        return self

    async def __aenter__(self) -> WorkflowStreamReader[T]:
        """Start nothing. The first iteration reads."""
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Detach from the stream's notifier, unless the stream ended."""
        await self.close()

    async def close(self) -> None:
        """Detach from the stream's notifier in a Local Activity, so its wakes stop.

        Idempotent. Does nothing once the stream ended or before a read came
        back empty. Iterating after a close attaches again. A reader with
        ``replay_from_store`` keeps its hold on the records it read, since
        the run may replay its reads until it ends.
        """
        state = self._state
        if state.done or not state.attached:
            return
        await self._run(self._input(subscriber=False, unsubscribe=True))
        state.attached = False
        state.sleep_after = None

    async def __anext__(self) -> StreamBatch[T]:
        """Read until records arrive, sleeping on the notifier while the stream is idle.

        Raises:
            StopAsyncIteration: The stream is closed and read to its end.
            BaseException: The failure the stream closed with, once, after
                the last batch.
            temporalio.contrib.streams.StreamRecordError: A record did not
                decode or convert. The reader delivered the records before
                it, and the next iteration goes on past it.
            temporalio.exceptions.FailureError: A read failed for good. It
                is the Local Activity's failure, an
                :class:`temporalio.exceptions.ApplicationError` whose type
                names the stream error, for example ``StreamNotFoundError``
                when the owner does not exist, or ``StreamUnsupportedError``
                on a server without the stream notifier. Uncaught, it fails
                the Workflow, as a failed publish does.
        """
        state = self._state
        if state.pending:
            return self._take_pending()
        while True:
            if state.done:
                await self._wait_for_owner_close()
                raise self._end()
            if (
                state.sleep_after is not None
                and state.attached_run_id != workflow.info().run_id
            ):
                # A reset run replays the attach of the run it was reset from,
                # and the notifier wakes only that run. Its next empty read
                # attaches the reset run.
                state.sleep_after = None
            if state.sleep_after is not None:
                await workflow.wait_condition(self._woke)
                self._take_wake()
            cut_task(self._cutoff)
            response = await self._read()
            records = self._convert(list(response.records), response.cursor)
            state.state = response.state
            if response.done:
                owner_ended = self._ref.kind != "workflow" and not (
                    response.closed_by
                    or response.HasField("close_result")
                    or response.HasField("close_failure")
                )
                if owner_ended and not state.attached and not state.owner_attached:
                    # Only the notifier knows how the Activity run ended, and it
                    # tells only the readers attached to it. The next read
                    # attaches and ends again, after this one's records.
                    state.owner_attached = True
                    state.subscribe_now = True
                    if not response.records:
                        continue
                else:
                    state.done = True
                    state.owner_ended = owner_ended
                    if response.HasField("close_failure"):
                        state.close_failure = response.close_failure
            elif not response.records:
                if self._ref.kind == "workflow_activity" and self._closed():
                    # The notifier closed the stream since the Activity is gone
                    # and no close came. The store keeps it open, and the
                    # Activity can no longer append, so the read is over.
                    state.done = True
                    continue
                if not response.chain_first_run_id:
                    # The owner hasn't started yet, so the stream runtime attached nothing
                    # and no notifier wakes this reader. It reads again later.
                    await workflow.sleep(self._idle_wait)
                    continue
                state.sleep_after = response.wake_after
            if records:
                return StreamBatch(records, self.position)
            if state.pending:
                return self._take_pending()

    def _convert(self, records: list[ReadRecord], end: str) -> list[StreamRecord[T]]:
        """Convert ``records`` in order, up to the first that does not convert.

        A record that does not convert must not stop the reader, so the rest
        wait in the reader's state, and the cursor stays past the last record
        converted.
        """
        state = self._state
        converted: list[StreamRecord[T]] = []
        for index, read in enumerate(records):
            try:
                converted.append(
                    to_record(workflow.payload_converter(), read, self._result_type)
                )
            except StreamRecordError as error:
                state.pending, state.pending_end = records[index:], end
                state.pending_error = error
                if index:
                    state.cursor = records[index - 1].cursor
                return converted
        state.pending = []
        state.cursor = end
        return converted

    def _take_pending(self) -> StreamBatch[T]:
        state = self._state
        error = state.pending_error
        if error is not None:
            state.pending_error = None
            state.cursor = state.pending.pop(0).cursor
            if not state.pending:
                state.cursor = state.pending_end
            raise error
        records = self._convert(state.pending, state.pending_end)
        if not records:
            return self._take_pending()
        return StreamBatch(records, self.position)

    async def _wait_for_owner_close(self) -> None:
        """Wait for the notifier's close after an Activity owner ended with no close.

        The stream runtime ends the read once the owner ended, with no outcome. The
        notifier's owner check closes the stream with the run's failure a
        little later, and wakes the readers attached to it, so a reader that
        never attached attaches first. Only a reader of an Activity's stream
        waits, and at most ``_OWNER_CLOSE_WAIT``, since a Workflow owner's
        normal end has no close and the wait would delay every such reader.
        """
        state = self._state
        if not state.owner_ended or not state.attached:
            return
        state.owner_ended = False

        def settled() -> bool:
            wake = wake_state(self.subscription_id)
            return wake is not None and (wake.closed or wake.resubscribe)

        try:
            await workflow.wait_condition(
                settled, timeout=_OWNER_CLOSE_WAIT.total_seconds()
            )
        except asyncio.TimeoutError:
            pass

    def _woke(self) -> bool:
        wake = wake_state(self.subscription_id)
        sleep_after = self._state.sleep_after
        assert sleep_after is not None
        return wake is not None and (
            wake.counter > sleep_after
            or wake.recheck
            or wake.closed
            or wake.resubscribe
        )

    def _take_wake(self) -> None:
        state = self._state
        wake = wake_state(self.subscription_id)
        assert wake is not None
        state.sleep_after = None
        wake.recheck = False
        if wake.resubscribe and not wake.closed:
            # The notifier dropped this reader. A new generation makes the
            # next attach a new one, since the server answers a repeat of the
            # dropped attach as done.
            wake.resubscribe = False
            state.generation += 1

    def _end(self) -> BaseException:
        state = self._state
        failure = state.close_failure
        if failure is None:
            # The store keeps no outcome for a chain close. The notifier's
            # owner check may still have sent the owner's failure in a wake.
            wake = wake_state(self.subscription_id)
            failure = wake.close_failure if wake is not None else None
        if failure is None or state.failure_raised:
            return StopAsyncIteration()
        state.failure_raised = True
        runtime: Any = workflow._Runtime.current()
        converter = runtime._workflow_context_failure_converter
        return converter.from_failure(failure, workflow.payload_converter())

    def _closed(self) -> bool:
        wake = wake_state(self.subscription_id)
        return wake is not None and wake.closed

    def _input(self, *, subscriber: bool, unsubscribe: bool = False) -> _ReadInput:
        state = self._state
        subscribing = state.subscribe_now and subscriber
        return _ReadInput(
            workflow_id=self._ref.workflow_id,
            owner_kind=self._ref.kind,
            activity_id=self._ref.activity_id or "",
            chain_run_id=self._ref.chain_run_id or "",
            topic=self._topic,
            after=state.cursor,
            state=base64.b64encode(state.state).decode(),
            # The read that attaches only waits for the notifier's answer.
            wait_ms=0 if subscribing else self._idle_wait // timedelta(milliseconds=1),
            max_records=self._max_records,
            max_bytes=self._max_bytes,
            subscription_id=(self.subscription_id if subscriber or unsubscribe else ""),
            subscribe=subscribing,
            generation=state.generation,
            read_run_id=state.read_run_id,
            unsubscribe=unsubscribe,
            positions_only=self._replay_from_store,
            reader_id=self.subscription_id,
        )

    async def _read(self) -> ReadResponse:
        state = self._state
        # A closed stream needs no wake, so the read names no subscriber.
        input = self._input(subscriber=not self._closed())
        # A read counts once it returned, so a failed or canceled read leaves
        # no gap in the chain of answer states that replay follows.
        input.read_seq = state.reads + 1
        input.chained = state.chain_run == workflow.info().run_id
        attached = state.attached
        if input.subscription_id:
            # A read canceled while it runs may have attached already.
            state.attached = True
        response, state.read_run_id, read_by, chained = await self._run(input)
        state.reads = input.read_seq
        state.chain_run = read_by if chained else ""
        if input.positions_only:
            if state.first_read_at is None:
                state.first_read_at = workflow.now()
            state.held_bytes += sum(
                r.stored.ByteSize() for r in response.records if r.HasField("stored")
            )
        state.subscribe_now = False
        state.attached = attached
        if input.subscription_id and (
            input.subscribe or not (response.records or response.done)
        ):
            state.attached = True
            state.attached_run_id = read_by
        return response

    async def _run(self, input: _ReadInput) -> tuple[ReadResponse, str, str, bool]:
        raw = await workflow.execute_local_activity(
            READ_ACTIVITY,
            input,
            result_type=RawValue,
            start_to_close_timeout=self._start_to_close_timeout,
            summary=_read_summary(input),
        )
        payload: Payload = raw.payload
        if is_positions(payload):
            raise StreamUnsupportedError(
                "a stream read with replay_from_store=True (the default) recorded "
                "only its range, and only the stream store plugin on the Worker or "
                "Replayer turns it back into records; add the plugin, or pass "
                "replay_from_store=False to record each batch in History"
            )
        response = ReadResponse.FromString(payload.data)
        return (
            response,
            payload.metadata.get(_READ_RUN_ID_KEY, b"").decode(),
            payload.metadata[_READ_BY_RUN_ID_KEY].decode(),
            is_chained(payload),
        )


def _read_summary(input: _ReadInput) -> str:
    # The summary names what the marker did, since every stream read marker
    # has the same Activity type.
    if input.unsubscribe:
        return f"stream unsubscribe {input.topic}"
    after = {BEGINNING: "the start", END: "the end"}.get(
        Cursor(input.after), input.after
    )
    what = "subscribe and read" if input.subscribe else "read"
    return f"stream {what} {input.topic} after {after}"


def _next_reader() -> int:
    runtime = workflow._Runtime.current()
    number = getattr(runtime, _READERS_ATTRIBUTE, 0) + 1
    setattr(runtime, _READERS_ATTRIBUTE, number)
    return number


@overload
def workflow_reader(
    ref: StreamRef | str,
    *,
    topic: StreamTopic[T],
    after: Cursor | StreamPosition = ...,
    idle_wait: timedelta = ...,
    max_records: int = ...,
    max_bytes: int = ...,
    start_to_close_timeout: timedelta | None = ...,
    cutoff: TaskCutoff | None = ...,
    replay_from_store: bool = ...,
) -> WorkflowStreamReader[T]: ...


@overload
def workflow_reader(
    ref: StreamRef | str,
    *,
    topic: str | None = None,
    result_type: type[T],
    after: Cursor | StreamPosition = ...,
    idle_wait: timedelta = ...,
    max_records: int = ...,
    max_bytes: int = ...,
    start_to_close_timeout: timedelta | None = ...,
    cutoff: TaskCutoff | None = ...,
    replay_from_store: bool = ...,
) -> WorkflowStreamReader[T]: ...


@overload
def workflow_reader(
    ref: StreamRef | str,
    *,
    topic: str | None = None,
    result_type: None = None,
    after: Cursor | StreamPosition = ...,
    idle_wait: timedelta = ...,
    max_records: int = ...,
    max_bytes: int = ...,
    start_to_close_timeout: timedelta | None = ...,
    cutoff: TaskCutoff | None = ...,
    replay_from_store: bool = ...,
) -> WorkflowStreamReader[Any]: ...


def workflow_reader(
    ref: StreamRef | str,
    *,
    topic: str | StreamTopic[Any] | None = None,
    result_type: type | None = None,
    after: Cursor | StreamPosition = BEGINNING,
    idle_wait: timedelta = timedelta(seconds=2),
    max_records: int = 100,
    max_bytes: int = 1 << 20,
    start_to_close_timeout: timedelta | None = None,
    cutoff: TaskCutoff | None = TaskCutoff(),
    replay_from_store: bool = True,
) -> WorkflowStreamReader[Any]:
    """A reader of a stream, for Workflow code.

    Each read is one Local Activity, which adds one marker event when its
    Workflow Task completes. While the stream is idle, the reader waits for
    a wake from the stream's notifier, with no Local Activity and no timer,
    so an idle reader costs nothing until the stream moves. Each empty read
    asks for one wake, and a read that finds records asks for none, so the
    appends that land while the reader works add no Workflow Task. The
    notifier also asks for a read after a quiet period (1 minute, doubling
    to 5 minutes), which finds records whose notification was lost. It needs a
    server with the stream notifier, and producers that notify, which they
    do unless
    :meth:`temporalio.contrib.streams.StreamStorePlugin.notify_on_append`
    turns it off.

    The reader follows the owner's run chain across Continue-as-New, as
    every read does. On the stream of a standalone Activity it reads the
    Activity run, and ends when that run closes the stream or ends. When
    the run ends with no close, the stream's notifier closes it with the
    run's outcome, and the reader raises the run's failure after its last
    batch. On the stream of an Activity that a Workflow scheduled it ends
    when the scheduling run sees the Activity end, and raises the
    Activity's failure after its last batch when the Activity failed, timed
    out or was canceled. To go on in the next run of the reading Workflow, pass
    :attr:`WorkflowStreamReader.position` as ``after``. The notifier wakes
    the run that attached, so after a reset, the new run reads once more
    to attach itself before it sleeps. A reader of an owner that hasn't
    started yet reads again after each ``idle_wait`` on a timer, until the
    owner starts or its start grace ends.

    Args:
        ref: The stream's owner, as a
            :class:`temporalio.contrib.streams.StreamRef` of any owner, or
            a Workflow id in this namespace.
        topic: The topic to read. Without one, the ref's topic.
        result_type: The type records decode to, for a plain string topic.
        after: Where to start: a cursor, ``BEGINNING``, ``END``, or the
            position of an earlier reader.
        idle_wait: How long a read waits in the store for a record before
            it comes back empty and the reader sleeps.
        max_records: The most records one batch holds. Records ride in the
            Local Activity's result, so this bounds the marker's size.
        max_bytes: The most stored bytes of one batch's records past its
            first, 1 MiB by default. The first record always goes, so a
            batch stays below the server's 2 MiB blob limit for records of
            up to 1 MiB.
        start_to_close_timeout: How long one read may run. ``idle_wait``
            plus 10 seconds by default.
        cutoff: When the reader ends the Workflow Task it reads in, so that
            a busy reader doesn't hold back the rest of the Workflow. 100
            commands or 2 seconds by default. ``None`` leaves the task to
            the Local Activity heartbeat.
        replay_from_store: Record each read's range and the stream runtime's digest of
            its records in History, in place of the records. On by default.
            Replay reads the range from the store again, before Workflow
            code sees it, and a store that holds other records fails the
            Workflow Task as nondeterministic. The store holds the records
            the reader read against retention for ``floor_lease`` (the
            retention, 7 days, by default) after the reader's last read or
            replay, and the reader never ends the hold itself. Only a read
            or a replay moves the hold, so a reader that read to the end
            moves it no more. A run must Continue-as-New within
            ``floor_lease`` of its last read. Past that it can't replay its
            reads, and its Workflow Task fails on each retry. The hold keeps
            every record after the reader's first read, with no bound on
            their size, so on a busy topic Continue-as-New when
            :meth:`WorkflowStreamReader.continue_as_new_suggested` says so. Replay
            needs the store, so a ``Replayer`` needs the store plugin and a
            client in its process that connected the same store. A store
            with neither holds nor a replay horizon (see
            :attr:`temporalio.contrib.streams.StreamStorePlugin.capabilities`)
            refuses it, and the first read fails with
            ``StreamUnsupportedError``. ``False`` records each batch in
            History, for a History that replays with no store.

    Raises:
        ValueError: ``result_type`` was passed with a topic definition, or a
            bound is not positive.
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator.

    .. warning::
        This API is experimental.
    """
    if workflow.unsafe.is_read_only():
        raise workflow.ReadOnlyContextError(
            "cannot read a stream in a query or an update validator"
        )
    if isinstance(ref, str):
        ref = StreamRef.for_workflow(ref)
    ref._require_supported()
    if idle_wait <= timedelta(0):
        raise ValueError("idle_wait must be positive")
    if max_records < 1:
        raise ValueError(f"max_records must be at least 1, got {max_records}")
    if max_bytes < 1:
        raise ValueError(f"max_bytes must be at least 1, got {max_bytes}")
    if cutoff is not None and (cutoff.commands < 1 or cutoff.time <= timedelta(0)):
        raise ValueError("a task cutoff needs a positive command count and time")
    name, result_type = resolve_topic(
        topic if topic is not None else ref.topic, result_type
    )
    return WorkflowStreamReader(
        ref,
        name,
        result_type,
        after,
        idle_wait,
        max_records,
        start_to_close_timeout or idle_wait + timedelta(seconds=10),
        cutoff,
        replay_from_store,
        max_bytes=max_bytes,
    )


@temporalio.activity.defn(name=READ_ACTIVITY)
async def read_activity(input: _ReadInput) -> RawValue:
    """Read one batch of a stream for a Workflow reader, which an empty read attaches.

    A Workflow reader runs it as a Local Activity. The store plugin
    registers it on every Worker built from a client that carries a store.
    The records come back with their bodies through the codec, so the
    Workflow converts them with its payload converter.
    """
    try:
        require_local_activity("the stream read Activity")
        return await _read_batch(input)
    except _FINAL_ERRORS as error:
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error


async def _read_batch(input: _ReadInput) -> RawValue:
    info = temporalio.activity.info()
    client = temporalio.activity.client()
    store = store_for_client(client)
    ref = StreamRef(
        input.owner_kind,
        input.workflow_id,
        topic=input.topic,
        chain_run_id=input.chain_run_id or None,
        activity_id=input.activity_id or None,
    )
    handle = StreamHandle(store, client, ref)
    service = await store._service_for(client)
    read_run_id = input.read_run_id
    if read_run_id is None:
        read_run_id = await handle._read_run_id()
    address = handle._address(input.topic, run_id=read_run_id)
    floor_holder = f"{info.workflow_run_id}/{input.reader_id}"
    if input.subscription_id:
        assert info.workflow_id and info.workflow_run_id
        if input.unsubscribe:
            await call(
                service.unsubscribe(
                    UnsubscribeRequest(
                        stream=address,
                        run_id=info.workflow_run_id,
                        subscription_id=input.subscription_id,
                        generation=input.generation,
                    )
                )
            )
            return _result(ReadResponse(cursor=input.after), read_run_id)
        _warn_if_no_notify(store)
        if input.subscribe:
            await call(
                service.subscribe(
                    SubscribeRequest(
                        stream=address,
                        workflow_id=info.workflow_id,
                        run_id=info.workflow_run_id,
                        subscription_id=input.subscription_id,
                        generation=input.generation,
                    )
                )
            )
    request = ReadRequest(
        stream=address,
        after=input.after,
        max_records=input.max_records,
        max_bytes=input.max_bytes,
        state=base64.b64decode(input.state),
    )
    if input.subscription_id:
        assert info.workflow_id and info.workflow_run_id
        request.subscriber.CopyFrom(
            ReadSubscriber(
                workflow_id=info.workflow_id,
                run_id=info.workflow_run_id,
                subscription_id=input.subscription_id,
                generation=input.generation,
            )
        )
    if input.positions_only:
        request.floor_holder = floor_holder
    request.wait.FromMilliseconds(input.wait_ms)
    try:
        response = await call(service.read(request))
    except StreamUnsupportedError as error:
        if not input.positions_only:
            raise
        raise StreamUnsupportedError(
            f"{error}; a stream read with replay_from_store=True (the default) "
            "needs a store with holds or a replay horizon, so pass "
            "replay_from_store=False to record each batch in History instead"
        ) from error
    for record in response.records:
        await decode_or_mark(handle._converter, record)
    if response.HasField("close_failure"):
        await _apply_to_failure_payloads(
            response.close_failure, handle._converter._transform_inbound_payloads
        )
    if not input.positions_only:
        return _result(response, read_run_id)
    range = ReadRangeRequest(
        stream=address,
        chain_first_run_id=response.chain_first_run_id,
        after=response.start_cursor,
        to=response.cursor,
        stored_records=sum(1 for r in response.records if r.HasField("stored")),
        state=request.state,
        records_digest=response.records_digest,
        floor_holder=floor_holder,
        start_from_end=request.after == END.token,
    )
    # The Worker turns the result back into this batch before Workflow code
    # sees it, from here while the run stays in this process.
    store._batches.put(range, response)
    chain = ReadChain(f"{info.workflow_run_id}/{input.reader_id}", input.read_seq)
    if input.chained:
        # The input is the previous read's answer state, which that read's
        # marker holds, so replay takes it from there.
        range.ClearField("state")
    return RawValue(
        positions_payload(
            range,
            response,
            read_run_id,
            info.workflow_run_id or "",
            chain,
            state_elided=input.chained,
        )
    )


def _warn_if_no_notify(store: StreamStorePlugin) -> None:
    global _warned_no_notify
    if _warned_no_notify or store._waits_on_notifier():
        return
    _warned_no_notify = True
    temporalio.activity.logger.warning(
        "A Workflow stream reader attaches to the notifier, but this Worker's "
        "store has notifications off, so a reader that sleeps wakes only on the "
        "notifier's recheck, up to 5 minutes late. Leave notify_on_append on in "
        "every process that appends."
    )


def _result(response: ReadResponse, read_run_id: str) -> RawValue:
    # Encoded here rather than by the converter, so a converter without
    # protobuf support still carries the batch. The run that read goes with
    # it, so a reset run tells that run from its own.
    return RawValue(
        batch_payload(
            response, read_run_id, temporalio.activity.info().workflow_run_id or ""
        )
    )
