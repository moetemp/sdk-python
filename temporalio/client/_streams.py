"""Streams outside Workflow code.

This module imports the Streams API of :py:mod:`temporalio.streams` on first use, so a client
with no Stream never imports it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator, Mapping
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import Self

import temporalio.workflow

if TYPE_CHECKING:
    import temporalio.api.stream.v1
    from temporalio.streams import (
        Closed,
        End,
        Reader,
        Record,
        StreamOffset,
        StreamRef,
    )
    from temporalio.streams._stream_client import StreamCalls

T = TypeVar("T")


class Stream(Generic[T]):
    """A Stream outside Workflow code, with its calls. ``T`` is the payload type.

    Get one with :py:meth:`temporalio.client.Client.create_stream` or
    :py:meth:`temporalio.client.Client.get_stream`. Its :py:attr:`ref` is the data to pass on,
    such as to a Workflow.

    All Streams of one client and Stream id write as one Producer, and
    :py:func:`temporalio.streams.producer` sets a Producer with a name of its own for the writes
    in a block. A write error that no caller got raises at the next call of the Stream.

    In an Activity, including a Local Activity, a Stream from the Worker's client, such as from
    ``activity.client()``, writes as the Producer of the attempt. So a retry withdraws the
    Records of the failed attempt. It can't close the Stream. The end of the attempt waits
    for the attempt's writes, and a write error that no caller got fails the attempt. A sync
    Activity wraps the Stream in :py:class:`SyncStream`.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self, ref: StreamRef[T], payload_type: Any, calls: StreamCalls
    ) -> None:
        """Create a Stream. Use a client's ``create_stream`` or ``get_stream``."""
        self._ref = ref
        # The type that a read decodes each value with. None decodes with no type hint.
        self._payload_type = payload_type
        self._calls = calls

    @property
    def ref(self) -> StreamRef[T]:
        """The data that identifies the Stream, to pass on."""
        return self._ref

    @property
    def proto_ref(self) -> temporalio.api.stream.v1.StreamRef:
        """The ``StreamRef`` wire message of the Stream, for a field of a proto message."""
        from temporalio.streams import _ref

        return _ref.to_proto(self._ref)

    def append(
        self,
        *values: T,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> asyncio.Future[StreamOffset]:
        """Start the write of one Record for each value, in order, and return its future.

        The write has its place in the Stream's order when the call returns, also when no
        caller awaits the future. The writes of one Stream keep the order of the calls. A cancel
        of the future doesn't stop the write. A list is one value. The next call of the Stream
        raises a write error that no caller got.

        Args:
            values: One value for each Record.
            rpc_metadata: Headers of each call. The Redis and memory backends make no RPC, so
                they ignore it.
            rpc_timeout: The deadline of each call, on every backend. A call past it raises
                ``RPCError`` with ``DEADLINE_EXCEEDED``.

        Returns:
            The future of the write. It gives the offset of the last Record, or raises
            ``StreamError`` when the backend refused the write, such as
            ``StreamNotFoundError`` for a Stream that nobody created.

        Raises:
            StreamClosedError: This object closed the Stream.
            StreamError: The code is a sync Activity, which uses :py:class:`SyncStream`.
        """
        return self._calls.append(values, rpc_metadata, rpc_timeout)

    def read(
        self,
        *,
        offset: StreamOffset | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Reader[T]:
        """Read the Stream's items in order.

        The read ends when the Stream is closed and the read gave its last item, or at the
        newest Record when ``wait_new_records`` is False. To stop after a quiet period, use your
        own timer, and resume later with ``offset=`` set to the read's ``next_offset``. A
        ``Superseded`` item withdraws earlier Records of a failed attempt.

        Args:
            offset: Start at this offset, inclusive, or at ``streams.END`` for new Records
                only. Defaults to the first Record.
            last: Start with the newest ``last`` Records, oldest first, such as for the first
                view of a chat. Can't be used with ``offset``. A Record that a later
                ``Superseded`` item withdraws doesn't count. At most 1000.
            wait_new_records: Wait for new Records at the end of the Stream.
            create_wait: How long to wait for a Stream that doesn't exist yet, such as one
                that a Workflow creates after it starts. Defaults to no wait.
            rpc_metadata: Headers of each call of the read. The Redis and memory backends
                ignore it.
            rpc_timeout: The deadline of each call of the read, on every backend, and not of the
                whole read. A call that waits for new Records returns before the deadline.

        Raises:
            StreamError: The code is a sync Activity, which uses :py:class:`SyncStream`.
        """
        from temporalio.streams._access import read_spec
        from temporalio.streams._items import End

        wire_offset: int | End | None
        if offset is None or isinstance(offset, End):
            wire_offset = offset
        else:
            wire_offset = offset._wire
        read = read_spec(
            self._payload_type,
            offset=wire_offset,
            last=last,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        return self._calls.read(read, rpc_metadata, rpc_timeout)

    async def get_offset(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> StreamOffset:
        """The next offset of the Stream, past the newest Record, or the beginning if it's empty.

        Args:
            rpc_metadata: Headers of the call. The Redis and memory backends ignore it.
            rpc_timeout: The deadline of the call, on every backend.
        """
        return await self._calls.get_offset(rpc_metadata, rpc_timeout)

    async def close(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Write the Records that this object appended, then close the Stream.

        A close carries no value, so code with a last value appends it first. Readers get the
        Records before the close, then their read ends. Later writes fail with
        ``StreamClosedError``.

        Args:
            rpc_metadata: Headers of each call. The Redis and memory backends ignore it.
            rpc_timeout: The deadline of each call, on every backend.

        Raises:
            StreamError: The code can't close the Stream. Only the run chain closes a Stream
                bound to a Workflow, and an Activity closes no Stream. Both raise with no call.
        """
        await self._calls.close(rpc_metadata, rpc_timeout)

    async def __aenter__(self) -> Self:
        """Use the Stream in ``async with``, which closes it at the end.

        Raises:
            StreamError: The code can't close the Stream. See :py:meth:`close`.
        """
        await self._calls.enter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream, also when the block raised any exception or was canceled.

        If this object already closed the Stream, this does nothing.
        """
        await self._calls.exit(exc)

    def __repr__(self) -> str:
        """Show the ref."""
        return f"{type(self).__name__}({self._ref!r})"


class SyncReader(Generic[T]):
    """The items of one read of a Stream in a sync Activity, in order. Iterate it with ``for``.

    Each iteration blocks the Activity's thread. See :py:class:`temporalio.streams.Reader`.

    .. warning::
        This API is experimental.
    """

    def __init__(self, reader: Reader[T]) -> None:
        """Create a read. Use a ``SyncStream``'s ``read`` instead."""
        from temporalio.streams import _activity_stream

        self._reader = reader
        self._steps: Generator[Record[T] | Closed, None, None] = (
            _activity_stream.sync_items(reader)
        )

    @property
    def next_offset(self) -> StreamOffset | None:
        """The offset to resume from. See :py:attr:`temporalio.streams.Reader.next_offset`."""
        return self._reader.next_offset

    def __iter__(self) -> SyncReader[T]:
        """The read is its own iterator."""
        return self

    def __next__(self) -> Record[T] | Closed:
        """The next item."""
        return next(self._steps)

    def close(self) -> None:
        """End the read early. It makes no more calls."""
        self._steps.close()


class SyncStream(Generic[T]):
    """A Stream in a sync Activity. Its calls block the Activity's thread.

    It wraps a :py:class:`Stream` of the Worker's client, such as for a ref of the Activity's
    input. A sync Activity has no ``activity.client()``, so it keeps the Worker's client, such as
    in an Activity class. Each call runs the Stream's call of the same name on the Worker's event
    loop, and blocks until it ends. An append returns the offset, not a future.

    .. warning::
        This API is experimental.
    """

    def __init__(self, stream: Stream[T]) -> None:
        """Wrap ``stream`` for a sync Activity."""
        self._stream = stream

    def append(self, *values: T) -> StreamOffset:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value.

        Raises:
            StreamNotFoundError: Nobody created the Stream.
        """
        from temporalio.streams import _activity_stream

        async def append() -> StreamOffset:
            return await self._stream.append(*values)

        return _activity_stream.run_blocking(append())

    def read(
        self,
        *,
        offset: StreamOffset | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
    ) -> SyncReader[T]:
        """Read the Stream's items in order. See :py:meth:`Stream.read`."""
        from temporalio.streams import _activity_stream

        async def read() -> Reader[T]:
            return self._stream.read(
                offset=offset,
                last=last,
                wait_new_records=wait_new_records,
                create_wait=create_wait,
            )

        return SyncReader(_activity_stream.run_blocking(read()))

    def get_offset(self) -> StreamOffset:
        """The next offset of the Stream, past the newest Record, or the beginning if empty."""
        from temporalio.streams import _activity_stream

        return _activity_stream.run_blocking(self._stream.get_offset())

    def close(self) -> None:
        """Close the Stream. See :py:meth:`Stream.close`.

        Raises:
            StreamError: The Stream writes as the Activity attempt, which can't close it.
        """
        from temporalio.streams import _activity_stream

        _activity_stream.run_blocking(self._stream.close())


# Records in one batch of the crate's writer. A full batch is written at once.
_BATCH = 1000


class BufferedAppender(Generic[T]):
    """Batches the appends of one Stream.

    Use it in an Activity or a client that writes often. :py:meth:`append` returns at once.
    The appender writes the buffered values as one batch at most ``max_delay`` after the first
    of them, or at once when they fill a batch. One write is in flight at a time, and values
    appended during it wait for the next one. Use it in ``async with``, which writes the
    remaining values at the end of the block. For a Stream that writes as an Activity attempt,
    the end of the attempt also writes the remaining values.

    ``await flush()`` is the backpressure point. A caller that appends faster than the Store
    accepts Records must await it from time to time, so the buffer doesn't grow without bound.

    After a batch fails, the appender writes nothing more, so a reader sees no gap. Each later
    call raises the error.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        stream: Stream[T],
        *,
        max_delay: timedelta = timedelta(milliseconds=100),
    ) -> None:
        """Create an appender.

        Args:
            stream: The Stream to append to.
            max_delay: The longest time that a value waits in the buffer.

        Raises:
            StreamError: The code is Workflow code. There, the appends that wait behind a write
                to the same Stream go as one Local Activity.
        """
        from temporalio.streams import _activity_stream, _errors

        if temporalio.workflow.in_workflow():
            raise _errors.unsupported(
                "Workflow code can't use a BufferedAppender. Its appends that wait behind "
                "a write already go as one Local Activity."
            )
        self._stream = stream
        self._max_delay = max_delay
        self._buffer: list[T] = []
        # When the oldest buffered value arrived, so the next batch is at most max_delay later.
        self._first_at = 0.0
        self._timer: asyncio.TimerHandle | None = None
        self._writing: asyncio.Task[None] | None = None
        self._appended = 0
        # Count of the values that a write took, written or not.
        self._settled = 0
        self._error: BaseException | None = None
        self._last_offset: StreamOffset | None = None
        self._unwritten = 0
        self._reported = False
        attempt = _activity_stream.attempt_of(stream)
        if attempt is not None:
            attempt.appenders.append(self)

    def append(self, *values: T) -> None:
        """Add one Record for each value to the buffer, and return at once.

        A list is one value.

        Raises:
            StreamError: A batch failed to write, so the appender takes no more values.
        """
        self._raise_error()
        if not values:
            raise TypeError("append takes one or more values")
        loop = asyncio.get_running_loop()
        if not self._buffer:
            self._first_at = loop.time()
        self._buffer.extend(values)
        self._appended += len(values)
        self._schedule()

    async def flush(self) -> StreamOffset | None:
        """Write every value appended before the call, and return the last written offset.

        Returns None if the appender hasn't written anything.
        """
        target = self._appended
        while self._settled < target:
            if self._writing is None:
                self._start()
            assert self._writing is not None
            await asyncio.shield(self._writing)
        self._raise_error()
        return self._last_offset

    def _raise_error(self) -> None:
        if self._error is not None:
            self._reported = True
            raise self._error

    async def _flush_at_attempt_end(self) -> None:
        # Fail the attempt for unwritten values, unless the Activity already got the error and
        # continued.
        reported = self._reported
        try:
            await self.flush()
        except Exception:
            if not reported:
                raise

    def _schedule(self) -> None:
        if self._writing is not None or not self._buffer:
            return
        if len(self._buffer) >= _BATCH:
            self._start()
        elif self._timer is None:
            self._timer = asyncio.get_running_loop().call_at(
                self._first_at + self._max_delay.total_seconds(), self._start
            )

    def _start(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._writing is not None or not self._buffer:
            return
        batch, self._buffer = self._buffer, []
        self._writing = asyncio.get_running_loop().create_task(self._write(batch))

    async def _write(self, batch: list[T]) -> None:
        try:
            if self._error is None:
                self._last_offset = await self._send(batch)
            else:
                # A failed batch fails the later ones too, so a reader sees no gap.
                self._unwritten += len(batch)
        except Exception as err:
            self._unwritten += len(batch)
            self._error = err
        finally:
            self._settled += len(batch)
            self._writing = None
            # Schedule the values appended during the write, for when their delay ends or they
            # fill a batch.
            self._schedule()

    async def _send(self, batch: list[T]) -> StreamOffset:
        return await self._stream.append(*batch)

    @property
    def unwritten(self) -> int:
        """How many appended values a failed batch left unwritten."""
        return self._unwritten

    async def __aenter__(self) -> BufferedAppender[T]:
        """Use the appender in ``async with``."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Write the buffered values. A write error does not hide an error of the block."""
        try:
            await self.flush()
        except Exception:
            if exc is None:
                raise
