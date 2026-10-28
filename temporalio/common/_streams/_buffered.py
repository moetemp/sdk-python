"""A publisher that batches the Records of one Stream."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import TracebackType
from typing import Generic, TypeVar

from temporalio.common._streams import _activity
from temporalio.common._streams._activity_stream import ActivityStream
from temporalio.common._streams._stream_client import ClientStream

T = TypeVar("T")

# Records in one batch of the crate's writer. A full batch is written at once.
_BATCH = 1000


class BufferedPublisher(Generic[T]):
    """Batches the publishes of one Stream.

    Use it in an Activity or a client that writes often. :py:meth:`publish` returns at once.
    The publisher writes the buffered values as one batch at most ``max_delay`` after the first
    of them, or at once when they fill a batch. One write is in flight at a time, and values
    published during it wait for the next one. Use it in ``async with``, which writes the
    remaining values at the end of the block. In an Activity, the end of the attempt also
    writes the remaining values.

    ``await flush()`` is the backpressure point. A caller that publishes faster than the Store
    accepts Records must await it from time to time, so the buffer doesn't grow without bound.

    After a batch fails, the publisher writes nothing more, so a reader sees no gap. Each later
    call raises the error.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        target: ActivityStream[T] | ClientStream[T],
        *,
        max_delay: timedelta = timedelta(milliseconds=100),
    ) -> None:
        """Create a publisher.

        Args:
            target: An ``ActivityStream`` or a ``ClientStream``.
            max_delay: The longest time that a value waits in the buffer.
        """
        self._stream = target
        self._max_delay = max_delay
        self._buffer: list[T] = []
        # When the oldest buffered value arrived, so the next batch is at most max_delay later.
        self._first_at = 0.0
        self._timer: asyncio.TimerHandle | None = None
        self._writing: asyncio.Task[None] | None = None
        self._published = 0
        # Count of the values that a write took, written or not.
        self._settled = 0
        self._error: BaseException | None = None
        self._last_offset = 0
        self._unwritten = 0
        self._reported = False
        streams = _activity.current.get()
        if streams is not None:
            streams.publishers.append(self)

    def publish(self, *values: T) -> None:
        """Add one Record for each value to the buffer, and return at once.

        A list is one value.

        Raises:
            StreamError: A batch failed to write, so the publisher takes no more values.
        """
        self._raise_error()
        if not values:
            raise TypeError("publish takes one or more values")
        loop = asyncio.get_running_loop()
        if not self._buffer:
            self._first_at = loop.time()
        self._buffer.extend(values)
        self._published += len(values)
        self._schedule()

    async def flush(self) -> int:
        """Write every value published before the call, and return the last written offset.

        Returns 0 if the publisher hasn't written anything.
        """
        target = self._published
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
            # Schedule the values published during the write, for when their delay ends or they
            # fill a batch.
            self._schedule()

    async def _send(self, batch: list[T]) -> int:
        return await self._stream._access().publish(batch)

    @property
    def unwritten(self) -> int:
        """How many published values a failed batch left unwritten."""
        return self._unwritten

    async def __aenter__(self) -> BufferedPublisher[T]:
        """Use the publisher in ``async with``."""
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
