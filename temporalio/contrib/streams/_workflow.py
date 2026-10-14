"""Reading a stream inside Workflow code, through Local Activities.

Workflow code can't do I/O. :func:`workflow_reader` reads in batches, each
one a Local Activity that runs Core's read and returns the records with
their bodies through the codec. Replay takes each batch from its marker and
does no I/O, so user code sees the same records, every kind included.

The reader sleeps while the stream is idle. Its first read attaches the
running Workflow to the stream's notifier as a wake target, under a
subscription id that replay gives back. A read that waits ``idle_wait`` and
finds nothing arms the stream and answers ``wake_after``, the counter of
the read's position. The reader then waits, with no Local Activity and no
timer, until a wake for its subscription carries a higher counter, a
recheck, a close, or a request to attach again. The server records each
wake on the next Workflow Task scheduled event, so a wake adds no event.
"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar, overload

import temporalio.activity
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams import (
    ReadRequest,
    ReadResponse,
    SubscribeRequest,
    UnsubscribeRequest,
)
from temporalio.common import RawValue
from temporalio.contrib.streams._body import decode_body
from temporalio.contrib.streams._cursor import BEGINNING
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
from temporalio.contrib.streams._plugin import call, store_for_client
from temporalio.contrib.streams._record import Cursor, StreamRecord
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wakes import wake_state
from temporalio.contrib.streams._wire import to_record
from temporalio.exceptions import ApplicationError

__all__ = [
    "READ_ACTIVITY",
    "StreamBatch",
    "StreamPosition",
    "WorkflowStreamReader",
    "read_activity",
    "workflow_reader",
]

T = TypeVar("T")

READ_ACTIVITY = "__temporal_streams_read"
"""The name the built-in read Activity is registered under."""

_READ_RUN_ID_KEY = "temporal.io/stream-read-run-id"
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
# Held on the run's runtime: how many readers the run made. Replay makes them
# in the same order, so each reader gets the same number.
_READERS_ATTRIBUTE = "_temporal_stream_readers"


@dataclass(frozen=True)
class StreamPosition:
    """Where a Workflow reader stands, as data the next run can resume from.

    Pass it as ``after=`` to :func:`workflow_reader`, for example in the
    input of a Continue-as-New. It carries the cursor of the last record
    the reader delivered and Core's read state, which holds the producer
    attempts the reader saw, so the next run still reports a retry as
    ``SUPERSEDED``.
    """

    cursor: str
    state: str = ""
    """Core's read state, in base64."""


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
    arm: bool
    wait_ms: int
    max_records: int
    subscription_id: str = ""
    """Set to attach the running Workflow to the notifier before the read."""
    generation: int = 0
    """How many times the reader attached again after a ``resubscribe`` wake."""
    read_run_id: str | None = None
    """The run the read names to Core, which the first read finds."""
    unsubscribe: bool = False
    """Detach ``subscription_id`` and read nothing."""


@dataclass
class _ReaderState:
    cursor: str
    state: bytes
    number: int
    generation: int = 0
    read_run_id: str | None = None
    armed_at: int | None = None
    attach: bool = True
    done: bool = False
    close_waited: bool = False
    failure_raised: bool = False


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
    and every record was delivered. When the reader learned that the stream
    closed with a failure, the iteration raises it after the last batch.
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
        close_wait: timedelta = timedelta(seconds=10),
    ) -> None:
        """Prefer :func:`temporalio.contrib.streams.workflow_reader`."""
        self._close_wait = close_wait
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

        Idempotent. Does nothing once the stream ended or before the first
        read attached. Iterating after a close attaches again.
        """
        state = self._state
        if state.done or state.attach:
            return
        await self._run(self._input(arm=False, unsubscribe=True))
        state.attach = True
        state.armed_at = None

    async def __anext__(self) -> StreamBatch[T]:
        """Read until records arrive, sleeping on the notifier while the stream is idle.

        Raises:
            StopAsyncIteration: The stream is closed and read to its end.
            BaseException: The failure the stream closed with, once, after
                the last batch.
            temporalio.exceptions.FailureError: A read failed for good. It
                is the Local Activity's failure, an
                :class:`temporalio.exceptions.ApplicationError` whose type
                names the stream error, for example ``StreamNotFoundError``
                when the owner does not exist, or ``StreamUnsupportedError``
                on a server without the stream notifier. Uncaught, it fails
                the Workflow, as a failed publish does.
        """
        state = self._state
        while True:
            if state.done:
                await self._wait_for_close()
                raise self._end()
            if state.armed_at is not None:
                await workflow.wait_condition(self._woke)
                self._take_wake()
            response = await self._read()
            records = [
                to_record(workflow.payload_converter(), r, self._result_type)
                for r in response.records
            ]
            state.cursor = response.cursor
            state.state = response.state
            if response.done:
                state.done = True
            elif response.armed:
                state.armed_at = response.wake_after
            if records:
                return StreamBatch(records, self.position)

    async def _wait_for_close(self) -> None:
        """Give the close's wake a moment to arrive after the store ended the read.

        The store closes before the notifier, so a busy reader can reach the
        end before the wake that carries the outcome. An owner that ended
        with no close sends no such wake soon, so the wait is bounded.
        """
        state = self._state
        if state.close_waited or state.attach:
            return
        state.close_waited = True

        def closed() -> bool:
            wake = wake_state(self.subscription_id)
            return wake is not None and wake.closed

        if closed():
            return
        try:
            await workflow.wait_condition(
                closed, timeout=self._close_wait.total_seconds()
            )
        except asyncio.TimeoutError:
            pass

    def _woke(self) -> bool:
        wake = wake_state(self.subscription_id)
        armed_at = self._state.armed_at
        assert armed_at is not None
        return wake is not None and (
            wake.counter > armed_at or wake.recheck or wake.closed or wake.resubscribe
        )

    def _take_wake(self) -> None:
        state = self._state
        wake = wake_state(self.subscription_id)
        assert wake is not None
        state.armed_at = None
        wake.recheck = False
        if wake.resubscribe and not wake.closed:
            # The notifier dropped this reader. A new generation makes the
            # next attach a new one, since the server answers a repeat of the
            # dropped attach as done.
            wake.resubscribe = False
            state.generation += 1
            state.attach = True

    def _end(self) -> BaseException:
        wake = wake_state(self.subscription_id)
        state = self._state
        if wake is None or wake.close_failure is None or state.failure_raised:
            return StopAsyncIteration()
        state.failure_raised = True
        runtime: Any = workflow._Runtime.current()
        converter = runtime._workflow_context_failure_converter
        return converter.from_failure(wake.close_failure, workflow.payload_converter())

    def _input(self, *, arm: bool, unsubscribe: bool = False) -> _ReadInput:
        state = self._state
        return _ReadInput(
            workflow_id=self._ref.workflow_id,
            chain_run_id=self._ref.chain_run_id or "",
            topic=self._topic,
            after=state.cursor,
            state=base64.b64encode(state.state).decode(),
            arm=arm,
            wait_ms=self._idle_wait // timedelta(milliseconds=1),
            max_records=self._max_records,
            subscription_id=self.subscription_id
            if (state.attach and arm) or unsubscribe
            else "",
            generation=state.generation,
            read_run_id=state.read_run_id,
            unsubscribe=unsubscribe,
        )

    async def _read(self) -> ReadResponse:
        state = self._state
        wake = wake_state(self.subscription_id)
        # A closed stream needs no wake, so the read neither attaches nor arms.
        arm = wake is None or not wake.closed
        input = self._input(arm=arm)
        response, state.read_run_id = await self._run(input)
        if input.subscription_id:
            state.attach = False
        return response

    async def _run(self, input: _ReadInput) -> tuple[ReadResponse, str]:
        what = "unsubscribe" if input.unsubscribe else "read"
        raw = await workflow.execute_local_activity(
            READ_ACTIVITY,
            input,
            result_type=RawValue,
            start_to_close_timeout=self._start_to_close_timeout,
            summary=f"stream {what} {self._topic}",
        )
        payload: Payload = raw.payload
        response = ReadResponse.FromString(payload.data)
        return response, payload.metadata.get(_READ_RUN_ID_KEY, b"").decode()


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
    start_to_close_timeout: timedelta | None = ...,
    close_wait: timedelta = ...,
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
    start_to_close_timeout: timedelta | None = ...,
    close_wait: timedelta = ...,
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
    start_to_close_timeout: timedelta | None = ...,
    close_wait: timedelta = ...,
) -> WorkflowStreamReader[Any]: ...


def workflow_reader(
    ref: StreamRef | str,
    *,
    topic: str | StreamTopic[Any] | None = None,
    result_type: type | None = None,
    after: Cursor | StreamPosition = BEGINNING,
    idle_wait: timedelta = timedelta(seconds=2),
    max_records: int = 100,
    start_to_close_timeout: timedelta | None = None,
    close_wait: timedelta = timedelta(seconds=10),
) -> WorkflowStreamReader[Any]:
    """A reader of a Workflow's stream, for Workflow code.

    Each read is one Local Activity, which adds one marker event when its
    Workflow Task completes. While the stream is idle, the reader waits for
    a wake from the stream's notifier, with no Local Activity and no timer,
    so an idle reader costs nothing until the stream moves. The notifier
    also asks for a read after a quiet period (1 minute, doubling to 1
    hour), which finds records whose notification was lost. It needs a
    server with the stream notifier, and producers that notify, which
    :meth:`temporalio.contrib.streams.StreamStorePlugin.notify_on_append`
    turns on.

    The reader follows the owner's run chain across Continue-as-New, as
    every read does. To go on in the next run of the reading Workflow, pass
    :attr:`WorkflowStreamReader.position` as ``after``.

    Args:
        ref: The stream's owner, as a
            :class:`temporalio.contrib.streams.StreamRef` or a Workflow id
            in this namespace.
        topic: The topic to read. Without one, the ref's topic.
        result_type: The type records decode to, for a plain string topic.
        after: Where to start: a cursor, ``BEGINNING``, ``END``, or the
            position of an earlier reader.
        idle_wait: How long a read waits in the store for a record before
            it arms the stream and the reader sleeps.
        max_records: The most records one batch holds. Records ride in the
            Local Activity's result, so this bounds the marker's size.
        start_to_close_timeout: How long one read may run. ``idle_wait``
            plus 10 seconds by default.
        close_wait: How long a reader that reached the end waits for the
            close's outcome, which the notifier sends after the store
            closed. 10 seconds by default. A wait that runs out costs a
            timer, and the reader then ends with no outcome.

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
        close_wait,
    )


@temporalio.activity.defn(name=READ_ACTIVITY)
async def read_activity(input: _ReadInput) -> RawValue:
    """Read one batch of a stream for a Workflow reader, attaching it first when asked.

    A Workflow reader runs it as a Local Activity. The store plugin
    registers it on every Worker built from a client that carries a store.
    The records come back with their bodies through the codec, so the
    Workflow converts them with its payload converter.
    """
    try:
        return await _read_batch(input)
    except _FINAL_ERRORS as error:
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error


async def _read_batch(input: _ReadInput) -> RawValue:
    info = temporalio.activity.info()
    client = temporalio.activity.client()
    store = store_for_client(client)
    ref = StreamRef.for_workflow(
        input.workflow_id, topic=input.topic, chain_run_id=input.chain_run_id or None
    )
    handle = StreamHandle(store, client, ref)
    service = await store._service_for(client)
    read_run_id = input.read_run_id
    if read_run_id is None:
        read_run_id = await handle._read_run_id()
    address = handle._address(input.topic, run_id=read_run_id)
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
        state=base64.b64decode(input.state),
        arm=input.arm,
    )
    request.wait.FromMilliseconds(input.wait_ms)
    response = await call(service.read(request))
    for record in response.records:
        if record.HasField("stored") and record.stored.HasField("body"):
            body = await decode_body(handle._converter, record.stored.body)
            record.stored.body.CopyFrom(body)
    return _result(response, read_run_id)


def _result(response: ReadResponse, read_run_id: str) -> RawValue:
    # Encoded here rather than by the converter, so a converter without
    # protobuf support still carries the batch.
    return RawValue(
        Payload(
            metadata={
                "encoding": _RESPONSE_ENCODING,
                "messageType": _RESPONSE_TYPE,
                _READ_RUN_ID_KEY: read_run_id.encode(),
            },
            data=response.SerializeToString(),
        )
    )
