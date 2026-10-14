"""Read the stream a Nexus operation returns, from a Workflow.

A stream-returning operation stays open while its stream runs. Its handler
reports progress whenever the stream moves and completes the operation when
the stream closes; the operation token names the stream. The reader turns
that into batches of typed records: it reads from its cursor through the
stream service on a Nexus endpoint, and waits for progress once a read finds
nothing new. A read that waits its idle wait and finds nothing arms the stream,
so the next append notifies the operation's callers.

Every read is a Nexus operation of the Workflow, so History records what each
read answered, and a replay hands over the same batches at the same points.
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Generic, TypeVar, cast

from temporalio import workflow
from temporalio.contrib.streams._errors import StreamError, StreamRecordError
from temporalio.contrib.streams._record import Cursor, RecordKind, Supersession
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._wire import WireRecord
from temporalio.contrib.streams.nexus._generated import (
    ReadInput,
    ReadOutput,
    RecordWire,
    TemporalStreams,
)
from temporalio.contrib.streams.nexus._operation import stream_ref_from_token

__all__ = [
    "ReadPosition",
    "ReadStaleRecord",
    "ReadSupersession",
    "StreamIncompleteError",
    "StreamReader",
]

T = TypeVar("T")

_BATCH = 100
# Once the operation completes, a read waits this long for a record, so a
# reader waiting for the stream's end does not spin.
_DRAIN_WAIT_MS = 2000
# How long a reader waits for progress before it reads again anyway. A
# producer that dies between its append and its notify sends no progress, so
# only a read finds that record. The wait doubles while nothing arrives.
_FIRST_RECHECK = timedelta(minutes=1)
_MAX_RECHECK = timedelta(hours=1)


class _RecheckDue(Exception):
    pass


class StreamIncompleteError(StreamError):
    """The stream and its operation didn't both end within the drain limit.

    .. warning::
        This API is experimental and may change in future versions.

    Either the operation completed but the stream never said it ended, or the
    stream ended but the operation never completed. The reader gives up
    after its drain limit, because every read is a Nexus operation in the
    caller's History, and a wait with no end holds the caller open. The
    first happens when the owner's close didn't reach the store, or when the
    Workflow id moved on to a new run chain whose topic is still open. The
    second happens when the close never reached the caller, for example when
    the server refused it for good.
    """

    def __init__(self, message: str, *, cursor: Cursor) -> None:
        """The reader stopped at ``cursor``."""
        super().__init__(message)
        self.cursor = cursor
        """Where the reader stopped. A new reader can resume after it."""


@dataclass(frozen=True)
class ReadSupersession:
    """Where a reader saw a later attempt of a producer take over.

    .. warning::
        This API is experimental and may change in future versions.
    """

    index: int
    """How many items the reader had handed over before it. The items from
    there on that the producer wrote belong to the later attempt; the ones
    before it from the same producer may be replaced by them."""

    cursor: Cursor
    """The stream position it reports, just before the later attempt's first
    record."""

    supersession: Supersession
    """The producer and the attempts it names."""


@dataclass(frozen=True)
class ReadStaleRecord:
    """A record from a producer attempt behind one the reader already saw.

    .. warning::
        This API is experimental and may change in future versions.

    An attempt that timed out but kept running can write after a later
    attempt started. Its records are not the current answer, so the reader
    keeps them out of the batches it hands over and reports them here.
    """

    index: int
    """How many items the reader had handed over before it."""

    cursor: Cursor
    """The stream position of the record."""

    producer_id: str
    """The producer that wrote it. Empty when the service couldn't hand the
    record over."""

    attempt: int
    """The attempt that wrote it, lower than one the reader already saw. Zero
    when the service couldn't hand the record over."""

    value: Any
    """The record's item, decoded into the reader's item type. ``None`` when
    the record couldn't be handed over or decoded, since a stale record
    raises nothing."""


@dataclass(frozen=True)
class ReadPosition:
    """Where a reader is, with what the stream service knows of the read.

    .. warning::
        This API is experimental and may change in future versions.

    Pass it to a new reader as ``after=`` to go on where an earlier one
    stopped, such as across Continue-as-New. It holds the cursor and the
    service's read state, so the new reader's superseded and stale marks
    stay exact for every producer. A cursor alone tells the service only the
    cursor record's producer. Both fields are text, so the position passes as
    a Workflow argument.
    """

    cursor: Cursor
    """Past every record the reader handed over."""

    state: str = ""
    """The stream service's read state, opaque and in base64. Empty before
    the first read."""


class StreamReader(Generic[T]):
    """Reads the stream a Nexus operation returns, in a Workflow.

    .. warning::
        This API is experimental and may change in future versions.

    Make one from the operation's handle and call :meth:`next` until it
    returns ``None``::

        reader = StreamReader(handle, item_type=Token, endpoint="chat")
        while (batch := await reader.next()) is not None:
            ...

    A generated client's ``read_<operation>_stream`` method builds one with
    the item type and endpoint the contract and the client know.

    The reader holds one cursor, starts at the oldest record the stream
    retains, and hands over each record once. Only data records are handed
    over; a producer's ``FINISH`` record is not. When a later attempt of a
    producer takes over (a retried Activity, for example), the earlier
    attempt's records already handed over stay handed over, and
    :attr:`supersessions` says where the later attempt began. A record an
    earlier attempt writes after a later one started is not handed over, and
    :attr:`stale_records` reports it. The stream service marks both. Each read
    carries the state the service answered with last, so the marks hold
    across reads and across the service's Workers. A reader built with
    ``after=`` an earlier reader's :attr:`position` keeps them exact too. One
    built with only a cursor tells the service only the cursor record's
    producer, so another producer's stale record can pass as current data. A
    record whose body the Workflow's payload converter cannot decode into
    ``item_type`` raises :class:`temporalio.contrib.streams.StreamRecordError`
    from :meth:`next` once the records before it are handed over; the call
    after that goes on past it.

    Each read goes to the run chain the Workflow id has when the read runs,
    not to the chain the operation token names: a stream reference pins a
    run, not a chain. So if the owner's chain ends and the same Workflow id
    starts a new chain while this reader still drains, a later read can hand
    over the new chain's records. Give each stream's owner a Workflow id of
    its own, or let the reader finish before the id is reused.

    A reader that waits for progress reads again once a minute passes with
    none, then after 2, 4 and so on up to 1 hour while the stream stays
    quiet. A producer that dies between its append and its notify sends no
    progress, so this read is what finds its record. Each such read costs a
    timer and a read in History.

    Every read is a Nexus operation result in the caller's History, about
    three events and up to one read answer (1 MiB of records) each. To read a
    long stream, Continue-as-New before History grows too large: pass
    :attr:`position` to the new run, start the operation again there, and
    build the reader with ``after=position``. The position is at most about
    87 KiB. After a successful completion, the
    reader reads until the stream service says the stream is done, and raises
    :class:`StreamIncompleteError` once its reads have found nothing for
    ``drain_limit``. A stream that ended waits at most ``drain_limit`` for its
    operation to complete, then raises the same error. A read that fails
    because retention dropped the records after the cursor raises with
    ``StreamExpiredError`` as its cause. A new reader with no ``after`` starts
    again at the oldest record still kept.

    Each read before the completion waits up to ``idle_wait`` for a record
    and asks the stream service to arm the stream. A read with records
    answers at once, so a busy stream costs no notification. A read that
    waits the whole ``idle_wait`` and finds nothing arms the stream, and the
    reader waits for progress. The stream's next append then notifies the
    operation's callers once. A shorter ``idle_wait`` wakes the reader on
    shorter gaps, at the cost of a notification and a progress Workflow Task
    for each such gap.

    Bodies reach Workflow code decoded: the stream service decodes each body
    with its Worker's data converter, payload codec included, and the read's
    result travels back as one Nexus operation result, which this Worker's
    codec decodes. So the Workers on both sides need the same payload codec,
    as Workers in one namespace normally have.
    """

    def __init__(
        self,
        handle: workflow.NexusOperationHandle[Any],
        *,
        item_type: type[T],
        endpoint: str,
        after: Cursor | ReadPosition | None = None,
        drain_limit: timedelta = timedelta(seconds=60),
        idle_wait: timedelta = timedelta(seconds=2),
    ) -> None:
        """Read the stream ``handle``'s operation returns.

        Args:
            handle: The started stream-returning operation. Its token names
                the stream.
            item_type: What each record's body decodes into.
            endpoint: The Nexus endpoint that serves the stream service for
                this stream, usually the operation's own.
            after: Resume where an earlier reader stopped. Pass its
                :attr:`position` to keep every producer's marks exact. A
                bare cursor tells the stream service only the cursor record's
                producer, so another producer's stale record can pass as
                current data. ``None`` starts at the oldest record kept.
            drain_limit: How long reads after a successful completion may
                find nothing, or a stream that ended may wait for its
                operation to complete, before the reader raises
                :class:`StreamIncompleteError`.
            idle_wait: How long a read waits for a record before it arms the
                stream and the reader waits for progress. Zero arms on the
                first empty read. Keep it well below the 10 second timeout of
                a Nexus request.

        Raises:
            ValueError: ``drain_limit`` is not positive, or ``idle_wait`` is
                negative.
        """
        if drain_limit <= timedelta(0):
            raise ValueError("drain_limit must be positive")
        if idle_wait < timedelta(0):
            raise ValueError("idle_wait must not be negative")
        self._idle_wait_ms = idle_wait // timedelta(milliseconds=1)
        self._handle = handle
        self._item_type = item_type
        self._endpoint = endpoint
        self._drain_limit = drain_limit
        self._drain_since: datetime | None = None
        self._done_since: datetime | None = None
        self._stream: StreamRef | None = None
        # Core's read state from the last answer, which the next read carries.
        self._state: bytes | None = None
        if isinstance(after, ReadPosition) and after.state:
            self._state = base64.b64decode(after.state)
        self._answer: ReadOutput | None = None
        self._pending: list[RecordWire] = []
        if isinstance(after, ReadPosition):
            after = after.cursor
        self._cursor = after.token if after is not None else ""
        self._counter = 0
        # Whether the last read answered with no record. An answer short of
        # the batch may still leave records behind (the service caps an
        # answer's bytes too), so only an empty one means the tail.
        self._caught_up = False
        # The stream service said no record will follow.
        self._stream_done = False
        self._operation_resolved = False
        self._operation_failed = False
        self._ended = False
        self._handed_over = 0
        self._supersessions: list[ReadSupersession] = []
        self._stale_records: list[ReadStaleRecord] = []
        self._undecodable: StreamRecordError | None = None
        self._recheck = _FIRST_RECHECK

    @property
    def cursor(self) -> Cursor:
        """Where the reader is: past every record it has handed over."""
        return Cursor(self._cursor)

    @property
    def position(self) -> ReadPosition:
        """Where the reader is, with the service's read state, for a new reader."""
        state = base64.b64encode(self._state).decode() if self._state else ""
        return ReadPosition(cursor=self.cursor, state=state)

    @property
    def supersessions(self) -> Sequence[ReadSupersession]:
        """Every producer attempt change this reader has seen, in order."""
        return tuple(self._supersessions)

    @property
    def stale_records(self) -> Sequence[ReadStaleRecord]:
        """Every record from an attempt behind one already seen, in order."""
        return tuple(self._stale_records)

    async def next(self) -> list[T] | None:
        """The next batch of records, or ``None`` once the stream has ended.

        Reads on while reads answer with records, and waits for the
        operation to report progress only once a read answers with none. A
        read like that waited ``idle_wait`` and armed the stream. An empty
        answer that did not arm waits for progress too.
        Never answers with an empty batch. The stream closes in the store
        before the operation completes, so once the operation completes the
        reader reads until the stream service says the stream is done, then
        answers ``None``. An operation fails when its owner closes the stream
        with an error, or ends without closing it after it failed, was
        canceled, was terminated or timed out. The store closes before the
        operation fails, so the reader hands over what reads find until one
        that waited a moment answers with none, which loses no record, then
        raises the failure. A canceled owner raises a canceled operation.

        Raises:
            temporalio.exceptions.NexusOperationError: A read failed, or the
                operation failed. A failed operation raises after the records
                it left are handed over.
            temporalio.contrib.streams.StreamRecordError: A record could not
                be decoded into
                ``item_type``, or was too large to cross. The next call goes
                on past it.
            StreamIncompleteError: The operation completed, but reads found
                nothing for ``drain_limit`` and the stream never ended. Or the
                stream ended, but the operation didn't complete within
                ``drain_limit``.
            ValueError: The operation's token does not name a stream.
        """
        if self._undecodable is not None:
            error, self._undecodable = self._undecodable, None
            raise error
        while not self._ended:
            # After a completion, only the stream's end ends the read.
            reads_on = self._operation_resolved and not self._operation_failed
            if not self._stream_done and (reads_on or not self._caught_up):
                batch = await self._read()
                if batch:
                    self._recheck = _FIRST_RECHECK
                    self._handed_over += len(batch)
                    if reads_on:
                        self._drain_since = workflow.now()
                    return batch
                if self._undecodable is not None:
                    error, self._undecodable = self._undecodable, None
                    raise error
                if (
                    reads_on
                    and self._caught_up
                    and not self._stream_done
                    and self._drain_since is not None
                    and workflow.now() - self._drain_since >= self._drain_limit
                ):
                    raise StreamIncompleteError(
                        f"the operation completed, but the stream didn't end within "
                        f"{self._drain_limit} of empty reads",
                        cursor=self.cursor,
                    )
                # An answer of records that carry no item, such as a FINISH,
                # is not the tail.
                continue
            if self._operation_resolved:
                self._ended = True
                # Raises the operation's failure, if it failed.
                await self._handle
                return None
            try:
                progress = await self._next_progress()
            except _RecheckDue:
                self._caught_up = False
                continue
            self._recheck = _FIRST_RECHECK
            if progress is None:
                # Records may have landed after the last progress.
                self._operation_resolved = True
                self._drain_since = workflow.now()
                try:
                    await self._handle
                except Exception:
                    self._operation_failed = True
            else:
                self._counter = progress.counter
            self._caught_up = False
        return None

    async def _next_progress(self) -> workflow.NexusOperationProgress | None:
        if not self._stream_done:
            try:
                return await asyncio.wait_for(
                    self._handle.progress(after_counter=self._counter),
                    self._recheck.total_seconds(),
                )
            except asyncio.TimeoutError:
                self._recheck = min(self._recheck * 2, _MAX_RECHECK)
                raise _RecheckDue from None
        # No record follows the stream's end, so only the completion is left,
        # and a completion that never comes would hold the caller open.
        if self._done_since is None:
            self._done_since = workflow.now()
        left = self._drain_limit - (workflow.now() - self._done_since)
        try:
            if left <= timedelta(0):
                raise asyncio.TimeoutError
            return await asyncio.wait_for(
                self._handle.progress(after_counter=self._counter),
                left.total_seconds(),
            )
        except asyncio.TimeoutError:
            raise StreamIncompleteError(
                f"the stream ended, but the operation didn't complete within "
                f"{self._drain_limit}",
                cursor=self.cursor,
            ) from None

    async def _read(self) -> list[T]:
        if not self._pending:
            await self._fetch()
        batch: list[T] = []
        while self._pending:
            stored = self._pending.pop(0)
            cursor = Cursor(stored.token)
            self._cursor = stored.token
            if stored.superseded is not None:
                self._supersessions.append(
                    ReadSupersession(
                        index=self._handed_over + len(batch),
                        cursor=cursor,
                        supersession=Supersession(
                            producer_id=stored.superseded.producer_id,
                            previous_attempt=stored.superseded.previous_attempt,
                            attempt=stored.superseded.attempt,
                        ),
                    )
                )
                continue
            if stored.stale:
                # Not the current answer, so one that can't be read is still
                # reported, and raises nothing.
                self._stale_records.append(self._stale(stored, cursor, len(batch)))
                continue
            try:
                record = self._record(stored)
            except Exception as error:
                # The rest of the answer stays pending, so the next call hands
                # it over without reading again.
                self._undecodable = StreamRecordError(
                    f"stream record at {cursor} could not be read as "
                    f"{getattr(self._item_type, '__name__', self._item_type)!s}: "
                    f"{error}",
                    cursor,
                )
                self._caught_up = False
                return batch
            if record is None:
                continue
            batch.append(cast(T, record[2]))
        answer = self._answer
        assert answer is not None
        if answer.next_token:
            self._cursor = answer.next_token
        # Core's state matches the answer's end, so it goes with the cursor.
        self._state = answer.state
        self._caught_up = not answer.records
        self._stream_done = answer.done
        return batch

    async def _fetch(self) -> None:
        if self._stream is None:
            token = self._handle.operation_token
            if not token:
                raise ValueError("the operation has no token, so it names no stream")
            self._stream = stream_ref_from_token(token)
        # A read covers every record the progress seen so far announced.
        latest = self._handle.latest_progress
        if latest is not None:
            self._counter = max(self._counter, latest.counter)
        client = workflow.create_nexus_client(
            service=TemporalStreams, endpoint=self._endpoint
        )
        # After the completion no progress follows, so a drain read never arms.
        arm = not self._operation_resolved
        self._answer = await client.execute_operation(
            TemporalStreams.read,
            ReadInput(
                stream=self._stream,
                after_token=self._cursor or None,
                max_records=_BATCH,
                wait_ms=self._idle_wait_ms if arm else _DRAIN_WAIT_MS,
                state=self._state,
                arm=True if arm else None,
            ),
        )
        self._pending = list(self._answer.records)

    def _stale(
        self, stored: RecordWire, cursor: Cursor, batched: int
    ) -> ReadStaleRecord:
        producer_id, attempt, value = "", 0, None
        if not stored.error:
            try:
                wire = WireRecord.FromString(stored.record)
                producer_id, attempt = wire.producer_id, wire.attempt
                record = self._record(stored)
                value = record[2] if record is not None else None
            except Exception:
                pass
        return ReadStaleRecord(
            index=self._handed_over + batched,
            cursor=cursor,
            producer_id=producer_id,
            attempt=attempt,
            value=value,
        )

    def _record(self, stored: RecordWire) -> tuple[str, int, Any] | None:
        """The producer, attempt and item of a data record, or ``None``."""
        if stored.error:
            # The service left out a record it couldn't hand over.
            raise ValueError(stored.error)
        wire = WireRecord.FromString(stored.record)
        kind = RecordKind(wire.kind) if wire.kind else RecordKind.DATA
        if kind is RecordKind.SUPERSEDED:
            raise ValueError("a SUPERSEDED record is marked by readers, never stored")
        if kind is not RecordKind.DATA:
            return None
        value: Any = None
        if wire.HasField("body"):
            value = workflow.payload_converter().from_payloads(
                [wire.body], [self._item_type]
            )[0]
        return wire.producer_id, wire.attempt, value
