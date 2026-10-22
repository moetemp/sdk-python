"""A publisher that batches the Records of one Topic."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import TracebackType
from typing import Any, Generic, TypeVar

from temporalio.contrib.streams._activity_stream import ActivityTopic
from temporalio.contrib.streams._stream_client import ClientTopic

T = TypeVar("T")


class BufferedPublisher(Generic[T]):
    """Batches the publishes of one Topic, for an Activity or a client that writes often.

    :py:meth:`publish` returns at once. The publisher writes the buffered values as one batch
    at most ``max_delay`` after the first of them. Use it in ``async with``, which writes the
    rest at the end of the block.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        topic: ActivityTopic[T] | ClientTopic[T],
        *,
        max_delay: timedelta = timedelta(milliseconds=100),
    ) -> None:
        """Create a publisher.

        Args:
            topic: A Topic of an ``ActivityStream`` or a ``ClientStream``.
            max_delay: How long a value waits in the buffer at most.
        """
        self._topic = topic
        self._max_delay = max_delay
        self._buffer: list[T] = []
        self._timer: asyncio.TimerHandle | None = None
        self._writing: asyncio.Task[None] | None = None
        self._error: BaseException | None = None
        self._last_offset = 0

    def publish(self, *values: T) -> None:
        """Add one Record for each value to the buffer, and return at once.

        Raises:
            StreamError: An earlier batch failed to write.
        """
        self._raise_error()
        if not values:
            raise TypeError("publish takes one or more values")
        self._buffer.extend(values)
        if self._timer is None:
            self._timer = asyncio.get_running_loop().call_later(
                self._max_delay.total_seconds(), self._write_buffer
            )

    async def flush(self) -> int:
        """Write the buffered values, and return the offset of the last Record written.

        Returns 0 when the publisher wrote nothing yet.
        """
        self._write_buffer()
        if self._writing is not None:
            await asyncio.shield(self._writing)
        self._raise_error()
        return self._last_offset

    def _raise_error(self) -> None:
        error, self._error = self._error, None
        if error is not None:
            raise error

    def _write_buffer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        batch, self._buffer = self._buffer, []
        if not batch:
            return
        # Each batch waits for the one before it, so the Records keep the publish order.
        self._writing = asyncio.get_running_loop().create_task(
            self._write(self._writing, batch)
        )

    async def _write(
        self, previous: asyncio.Task[None] | None, batch: list[Any]
    ) -> None:
        if previous is not None:
            await previous
        if self._error is not None:
            # A failed batch fails the later ones too, so a reader sees no gap.
            return
        try:
            self._last_offset = await self._topic.publish(*batch)
        except Exception as err:
            self._error = err

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
