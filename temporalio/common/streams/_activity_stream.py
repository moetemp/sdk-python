"""Stream handles in Activity code."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Coroutine, Generator, Sequence
from datetime import timedelta
from typing import Any, Generic, TypeVar, overload

import temporalio.api.streamservice.v1 as stream_pb
from temporalio.common.streams import _activity, _errors
from temporalio.common.streams._access import (
    Access,
    SyncTopicBase,
    _AsyncTopic,
    read_spec,
    subscription,
)
from temporalio.common.streams._defn import TopicDefinition
from temporalio.common.streams._handle import StreamHandle
from temporalio.common.streams._items import Closed, End, Record, Superseded
from temporalio.common.streams._reader import Subscription

E = TypeVar("E")
T = TypeVar("T")
_R = TypeVar("_R")

# How long a blocked sync call waits at a time.
_WAIT_STEP = 0.1


def _bound(handle: StreamHandle[Any]) -> _activity.ActivityStreams:
    streams = _activity.current.get()
    if streams is None:
        raise _errors.StreamConfigError(
            "No Stream backend runs this Activity. Register one on the Worker's client "
            "with stream_backends=."
        )
    if handle.backend and handle.backend != streams.backend_name:
        raise _errors.StreamConfigError(
            f"The Stream {handle.id} is of the backend {handle.backend}. The Worker has only "
            f"{streams.backend_name}."
        )
    if not handle.backend and not streams.default:
        raise _errors.no_default_backend(
            "Worker", [streams.backend_name], "a handle that carries its backend name"
        )
    return streams


def _check_close(handle: StreamHandle[Any]) -> None:
    # A Workflow's Stream closes only from Workflow code, once its task commits, and a client's
    # only from a client. The Store refuses the others too, and this error comes with no call.
    streams = _bound(handle)
    local = streams.producer.kind == stream_pb.ProducerKind.PRODUCER_KIND_LOCAL_ACTIVITY
    if not local or handle.workflow_id:
        raise _errors.StreamNotOwnerError(
            f"Can't close the Stream {handle.id} from this Activity. A Workflow's Stream "
            "closes only from Workflow code, and a client's only from a client.",
            reason="stream_not_owner",
        )


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
        # A handle with only an id names no chain, so the close names this Activity's own, and
        # the Store refuses it for a Workflow's Stream.
        workflow_id=handle.workflow_id or streams.workflow_id,
        first_execution_run_id=handle.first_execution_run_id,
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
        """A Topic of this Stream. See :py:meth:`ClientStream.topic`."""
        name, topic_type = self._topic_spec(topic, type)
        return ActivityTopic(self, name, topic_type)

    def subscribe(
        self,
        *topics: str | TopicDefinition[Any] | ActivityTopic[Any],
        offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        topic_prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
        type: Any = None,
    ) -> Subscription[Any]:
        """Read the Stream's items in order. See :py:meth:`ClientStream.subscribe`."""
        read = read_spec(
            self,
            topics,
            topic_prefixes=topic_prefixes,
            default_type=type,
            offset=offset,
            last=last,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        access = self._access()
        return subscription(lambda position: access.items(read, position), read)

    async def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one.

        It is 0 for a Stream with no Record, and ``offset=0`` reads from the start.
        Offsets are sparse and above zero, so 0 is never a Record's offset.
        """
        return await self._access().get_offset()

    async def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Write this attempt's Records, then close the Stream.

        Readers get the Records before it, then a ``Closed`` item with ``result`` or
        ``failure``. A ``CancelledError`` failure closes it as canceled.

        No Activity closes a Workflow's Stream, also a Local Activity of the run chain that
        created it, and no Activity closes a client's Stream. They get
        :py:class:`StreamNotOwnerError`. A Local Activity writes before its Workflow Task
        commits, so its close would stay when the task fails, and the task's retry could not
        create the Stream again. Workflow code closes a Workflow's Stream.
        """
        _check_close(self)
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


class SyncSubscription(Generic[T]):
    """The items of one read of a Stream in a sync Activity, in order. Use it in ``for``.

    Each step blocks the Activity's thread. See :py:class:`Subscription`.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self, stream: SyncActivityStream[Any], subscription: Subscription[T]
    ) -> None:
        """Create a read. Use a handle's ``subscribe`` instead."""
        self._subscription = subscription
        self._steps = stream._items(subscription)

    @property
    def next_offset(self) -> int | None:
        """The offset to resume from. See :py:attr:`Subscription.next_offset`."""
        return self._subscription.next_offset

    def __iter__(self) -> SyncSubscription[T]:
        """The read is its own iterator."""
        return self

    def __next__(self) -> Record[T] | Superseded | Closed:
        """The next item."""
        return next(self._steps)

    def close(self) -> None:
        """End the read early. It makes no more calls."""
        self._steps.close()


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
        future = asyncio.run_coroutine_threadsafe(call, loop)
        try:
            while True:
                # Short waits let an exception that the Worker raises in this thread, such as
                # a cancel, come in.
                try:
                    return future.result(timeout=_WAIT_STEP)
                except concurrent.futures.TimeoutError:
                    continue
        except BaseException:
            # A cancel of the Activity thread, such as from a heartbeat, stops the call too,
            # so a long poll does not run on in the Worker's loop.
            future.cancel()
            raise

    @overload
    def topic(self, topic: TopicDefinition[T]) -> SyncActivityTopic[T]: ...

    @overload
    def topic(self, topic: str) -> SyncActivityTopic[Any]: ...

    @overload
    def topic(self, topic: str, *, type: type[T]) -> SyncActivityTopic[T]: ...

    def topic(
        self, topic: str | TopicDefinition[Any], *, type: Any = None
    ) -> SyncActivityTopic[Any]:
        """A Topic of this Stream. See :py:meth:`ClientStream.topic`."""
        name, topic_type = self._topic_spec(topic, type)
        return SyncActivityTopic(self, name, topic_type)

    def subscribe(
        self,
        *topics: str | TopicDefinition[Any] | SyncActivityTopic[Any],
        offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        topic_prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
        type: Any = None,
    ) -> SyncSubscription[Any]:
        """Read the Stream's items in order. See :py:meth:`ClientStream.subscribe`."""
        read = read_spec(
            self,
            topics,
            topic_prefixes=topic_prefixes,
            default_type=type,
            offset=offset,
            last=last,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        access = self._access()
        return SyncSubscription(
            self, subscription(lambda position: access.items(read, position), read)
        )

    def _items(
        self, items: Subscription[T]
    ) -> Generator[Record[T] | Superseded | Closed, None, None]:
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
        """The next offset of the Stream, the newest offset plus one.

        It is 0 for a Stream with no Record, and ``offset=0`` reads from the start.
        Offsets are sparse and above zero, so 0 is never a Record's offset.
        """
        return self._run(self._access().get_offset())

    def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Write this attempt's Records, then close the Stream. See
        :py:meth:`ActivityStream.close`.
        """
        _check_close(self)
        self._run(self._access().close(result, failure))


async def _next(items: Subscription[T]) -> Record[T] | Superseded | Closed:
    return await items.__anext__()
