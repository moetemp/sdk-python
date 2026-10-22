"""Stream handles in Activity code."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Coroutine, Iterator, Sequence
from datetime import timedelta
from typing import Any, TypeVar, overload

from temporalio.contrib.streams import _activity, _errors
from temporalio.contrib.streams._access import (
    Access,
    SyncTopicBase,
    _AsyncTopic,
    read_spec,
)
from temporalio.contrib.streams._defn import TopicDefinition
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._reader import Item

E = TypeVar("E")
T = TypeVar("T")
_R = TypeVar("_R")


def _bound(handle: StreamHandle[Any]) -> _activity.ActivityStreams:
    streams = _activity.current.get()
    if streams is None:
        raise _errors.StreamTransportMissingError(
            "No Stream plugin runs this Activity, such as RedisStreamPlugin"
        )
    if handle.backend and handle.backend != streams.backend_name:
        raise _errors.StreamUnsupportedError(
            f"The Stream {handle.id} is of the backend {handle.backend}. The Worker has only "
            f"{streams.backend_name}.",
            reason="stream_unsupported",
        )
    return streams


def _access(handle: StreamHandle[Any]) -> Access:
    streams = _bound(handle)
    stream_id = handle.id

    async def known_id() -> str:
        return stream_id

    return Access(
        backend=streams.backend,
        converter=streams.converter,
        namespace=streams.namespace,
        stream_id=known_id,
        writer=lambda: streams.writer(stream_id),
    )


class ActivityTopic(_AsyncTopic[T]):
    """One Topic of an :py:class:`ActivityStream`. All Topics of a Stream share one order.

    .. warning::
        This API is experimental.
    """


class ActivityStream(StreamHandle[E]):
    """A Stream in an async Activity. The Activity writes, reads and closes it.

    An Activity gets a Stream only as an argument, such as a field of its input. Each publish
    writes before it returns. Use :py:class:`BufferedPublisher` to batch.

    .. warning::
        This API is experimental.
    """

    def _access(self) -> Access:
        return _access(self)

    @overload
    def topic(self, topic: TopicDefinition[T]) -> ActivityTopic[T]: ...

    @overload
    def topic(self, topic: str) -> ActivityTopic[Any]: ...

    @overload
    def topic(self, topic: str, *, type: type[T]) -> ActivityTopic[T]: ...

    def topic(
        self, topic: str | TopicDefinition[Any], *, type: Any = None
    ) -> ActivityTopic[Any]:
        """A Topic of this Stream.

        Args:
            topic: A Topic of the Stream's definition, such as ``Events.turns``, or a name.
                A name of the definition takes its type.
            type: The type of a Topic outside the definition, or of a Stream with no
                definition.
        """
        name, topic_type = self._topic_spec(topic, type)
        return ActivityTopic(self, name, topic_type)

    def subscribe(
        self,
        *topics: str | TopicDefinition[Any] | ActivityTopic[Any],
        from_offset: int | None = None,
        wait_new_records: bool = True,
        prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
    ) -> AsyncGenerator[Item, None]:
        """Read the Stream's items in order.

        The read ends after a ``Closed`` item, or at the newest Record when
        ``wait_new_records`` is False. A ``Superseded`` item withdraws earlier Records of a
        failed attempt, and it passes every Topic filter.

        Args:
            topics: Read these Topics. With no Topics and no prefixes, read every Topic.
            from_offset: Start at this offset, inclusive. Defaults to the first Record.
            wait_new_records: Wait for new Records at the end of the Stream.
            prefixes: Also read the Topics whose names start with one of these.
            create_wait: How long to wait for a Stream that nobody created yet. Defaults to
                no wait.
        """
        names, types = read_spec(self, topics)
        return self._access().read(
            topics=names,
            prefixes=prefixes,
            types=types,
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )

    async def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one."""
        return await self._access().get_offset()

    async def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Write this attempt's Records, then close the Stream.

        Readers get the Records before it, then a ``Closed`` item with ``result`` or
        ``failure``.
        """
        await self._access().close(result, failure)


class SyncActivityTopic(SyncTopicBase[T]):
    """One Topic of a :py:class:`SyncActivityStream`. Its calls block.

    .. warning::
        This API is experimental.
    """

    def publish(self, *values: T) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value.
        """
        stream = self._stream
        assert isinstance(stream, SyncActivityStream)
        return stream._run(stream._access().publish(self._name, values))

    def subscribe(
        self,
        *,
        from_offset: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
    ) -> Iterator[Item]:
        """Read this Topic only. See the Stream's ``subscribe``."""
        stream = self._stream
        assert isinstance(stream, SyncActivityStream)
        return stream.subscribe(
            self,
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )


class SyncActivityStream(StreamHandle[E]):
    """A Stream in a sync Activity. Its calls block the Activity's thread.

    The calls run on the Worker's event loop, so the Activity thread only waits for them.

    .. warning::
        This API is experimental.
    """

    def _access(self) -> Access:
        return _access(self)

    def _run(self, call: Coroutine[Any, Any, _R]) -> _R:
        loop = _bound(self).loop
        try:
            on_loop = asyncio.get_running_loop() is loop
        except RuntimeError:
            on_loop = False
        if on_loop:
            call.close()
            raise RuntimeError(
                "A SyncActivityStream blocks, so use ActivityStream in an async Activity"
            )
        return asyncio.run_coroutine_threadsafe(call, loop).result()

    @overload
    def topic(self, topic: TopicDefinition[T]) -> SyncActivityTopic[T]: ...

    @overload
    def topic(self, topic: str) -> SyncActivityTopic[Any]: ...

    @overload
    def topic(self, topic: str, *, type: type[T]) -> SyncActivityTopic[T]: ...

    def topic(
        self, topic: str | TopicDefinition[Any], *, type: Any = None
    ) -> SyncActivityTopic[Any]:
        """A Topic of this Stream. See :py:meth:`ActivityStream.topic`."""
        name, topic_type = self._topic_spec(topic, type)
        return SyncActivityTopic(self, name, topic_type)

    def subscribe(
        self,
        *topics: str | TopicDefinition[Any] | SyncActivityTopic[Any],
        from_offset: int | None = None,
        wait_new_records: bool = True,
        prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
    ) -> Iterator[Item]:
        """Read the Stream's items in order. See :py:meth:`ActivityStream.subscribe`."""
        names, types = read_spec(self, topics)
        items = self._access().read(
            topics=names,
            prefixes=prefixes,
            types=types,
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        return self._items(items)

    def _items(self, items: AsyncGenerator[Item, None]) -> Iterator[Item]:
        try:
            while True:
                try:
                    item = self._run(_next(items))
                except StopAsyncIteration:
                    return
                yield item
        finally:
            self._run(items.aclose())

    def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one."""
        return self._run(self._access().get_offset())

    def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Write this attempt's Records, then close the Stream."""
        self._run(self._access().close(result, failure))


async def _next(items: AsyncGenerator[Item, None]) -> Item:
    return await items.__anext__()
