"""A publisher that batches the Records of one Topic, or of every Topic of a Stream."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import TracebackType
from typing import Any, Generic, TypeVar, overload

from temporalio.common._streams import _activity
from temporalio.common._streams._activity_stream import ActivityStream, ActivityTopic
from temporalio.common._streams._defn import TopicDefinition
from temporalio.common._streams._handle import TopicBase
from temporalio.common._streams._stream_client import ClientStream, ClientTopic

T = TypeVar("T")
U = TypeVar("U")

# Records in one batch of the crate's writer. A full batch is written at once.
_BATCH = 1000


class _AnyTopic:
    # The type argument of a publisher over a whole Stream, whose publish takes the Topic.
    pass


class BufferedPublisher(Generic[T]):
    """Batches the publishes of one Topic, or of every Topic of one Stream.

    Use it in an Activity or a client that writes often. :py:meth:`publish` returns at once.
    The publisher writes the buffered values as one batch at most ``max_delay`` after the first
    of them, or at once when they fill a batch. One write is in flight at a time, and values
    published during it wait for the next one. Use it in ``async with``, which writes the
    remaining values at the end of the block. In an Activity, the end of the attempt also
    writes the remaining values.

    ``await flush()`` is the backpressure point. A caller that publishes faster than the Store
    accepts Records must await it from time to time, so the buffer doesn't grow without bound.

    A publisher over a Stream takes the Topic as the first argument of
    ``publish(topic, *values)``. All the Topics share one buffer, so the Records keep the
    publish order across Topics.

    After a batch fails, the publisher writes nothing more, so a reader sees no gap. Each later
    call raises the error.

    .. warning::
        This API is experimental.
    """

    @overload
    def __init__(
        self: BufferedPublisher[_AnyTopic],
        target: ActivityStream[Any] | ClientStream[Any],
        *,
        max_delay: timedelta = timedelta(milliseconds=100),
    ) -> None: ...

    @overload
    def __init__(
        self,
        target: ActivityTopic[T] | ClientTopic[T],
        *,
        max_delay: timedelta = timedelta(milliseconds=100),
    ) -> None: ...

    def __init__(
        self,
        target: Any,
        *,
        max_delay: timedelta = timedelta(milliseconds=100),
    ) -> None:
        """Create a publisher.

        Args:
            target: A Topic of an ``ActivityStream`` or a ``ClientStream``, or the Stream
                itself for a publisher of all its Topics.
            max_delay: The longest time that a value waits in the buffer.
        """
        self._topic: ActivityTopic[Any] | ClientTopic[Any] | None = None
        self._stream: ActivityStream[Any] | ClientStream[Any] | None = None
        if isinstance(target, (ActivityTopic, ClientTopic)):
            self._topic = target
        else:
            self._stream = target
        self._max_delay = max_delay
        self._buffer: list[tuple[str, Any]] = []
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

    @overload
    def publish(
        self: BufferedPublisher[_AnyTopic],
        topic: TopicDefinition[U] | ActivityTopic[U] | ClientTopic[U],
        /,
        *values: U,
    ) -> None: ...

    @overload
    def publish(
        self: BufferedPublisher[_AnyTopic], topic: str, /, *values: Any
    ) -> None: ...

    @overload
    def publish(self, *values: T) -> None: ...

    def publish(self, *args: Any) -> None:
        """Add one Record for each value to the buffer, and return at once.

        A publisher over a Stream takes the Topic first. The Topic can be a Topic of the
        definition, such as ``Events.tokens``, a Topic name, or a Topic of the Stream.

        Raises:
            StreamError: A batch failed to write, so the publisher takes no more values.
        """
        self._raise_error()
        if self._topic is not None:
            topic, values = self._topic.name, args
        elif args:
            topic, values = self._topic_name(args[0]), args[1:]
        else:
            topic, values = "", ()
        if not values:
            raise TypeError("publish takes one or more values")
        loop = asyncio.get_running_loop()
        if not self._buffer:
            self._first_at = loop.time()
        self._buffer.extend((topic, value) for value in values)
        self._published += len(values)
        self._schedule()

    def _topic_name(self, topic: Any) -> str:
        assert self._stream is not None
        if isinstance(topic, TopicBase):
            if topic._stream != self._stream:
                raise ValueError(f"The Topic {topic.name} is of another Stream")
            return topic.name
        name, _ = self._stream._topic_spec(topic, None)
        return name

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

    async def _write(self, batch: list[tuple[str, Any]]) -> None:
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

    async def _send(self, batch: list[tuple[str, Any]]) -> int:
        if self._topic is not None:
            return await self._topic.publish(*(value for _, value in batch))
        assert self._stream is not None
        return await self._stream._access().publish_records(batch)

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
