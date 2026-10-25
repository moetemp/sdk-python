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

# The Records of one batch of the crate's writer. A buffer that holds a batch writes at once.
_BATCH = 1000


class _AnyTopic:
    # The type argument of a publisher over a whole Stream, whose publish takes the Topic.
    pass


class BufferedPublisher(Generic[T]):
    """Batches the publishes of one Topic, or of every Topic of one Stream, for an Activity or
    a client that writes often.

    :py:meth:`publish` returns at once. The publisher writes the buffered values as one batch
    at most ``max_delay`` after the first of them, or at once when they fill a batch. One write
    is in flight at a time, and values published during it wait for the next one. Use it in
    ``async with``, which writes the rest at the end of the block. In an Activity, the end of
    the attempt also writes the rest.

    ``await flush()`` is the backpressure point. A caller that publishes faster than the Store
    takes Records awaits it now and then, so the buffer does not grow without bound.

    Over a Stream, ``publish(topic, *values)`` takes the Topic of each call first, and all the
    Topics share one buffer, so the Records keep the publish order across Topics.

    After a batch fails, the publisher writes nothing more, so a reader sees no gap, and each
    later call raises the error.

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
            max_delay: How long a value waits in the buffer at most.
        """
        self._topic: ActivityTopic[Any] | ClientTopic[Any] | None = None
        self._stream: ActivityStream[Any] | ClientStream[Any] | None = None
        if isinstance(target, (ActivityTopic, ClientTopic)):
            self._topic = target
        else:
            self._stream = target
        self._max_delay = max_delay
        self._buffer: list[tuple[str, Any]] = []
        # When the oldest buffered value came, so the next batch goes at most max_delay later.
        self._first_at = 0.0
        self._timer: asyncio.TimerHandle | None = None
        self._writing: asyncio.Task[None] | None = None
        self._published = 0
        # Values that a write took, written or not.
        self._settled = 0
        self._error: BaseException | None = None
        self._last_offset = 0
        self._unwritten = 0
        self._reported = False
        attempt = _activity.current.get()
        # The attempt's end flushes the publisher before its backend drops the writers.
        stream = self._topic._stream if self._topic is not None else self._stream
        streams = (
            attempt.get(stream.backend)
            if attempt is not None and stream is not None
            else None
        )
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

        A publisher over a Stream takes the Topic first. It is a Topic of the definition, such
        as ``Events.tokens``, a name of the definition, or a Topic of the Stream.

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
        """Write every value published before the call, and return the offset of the last
        Record written.

        Returns 0 when the publisher wrote nothing yet.
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
        # The attempt fails for values that never went out, unless the Activity already saw
        # the error and went on.
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
            # Values published during the write go next, when their delay or a batch is up.
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
