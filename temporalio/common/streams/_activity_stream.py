"""Stream handles in Activity code."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Coroutine, Generator, Sequence
from datetime import timedelta
from types import TracebackType
from typing import Any, Generic, TypeVar, overload

import temporalio.activity
import temporalio.api.streamservice.v1 as stream_pb
from temporalio.common.streams import _activity, _backend, _errors, _ids
from temporalio.common.streams._access import (
    Access,
    SyncTopicBase,
    _AsyncTopic,
    read_spec,
    subscription,
)
from temporalio.common.streams._defn import TopicDefinition, definition_of
from temporalio.common.streams._handle import Creating, StreamHandle, exit_failure
from temporalio.common.streams._items import Closed, End, Record, Superseded
from temporalio.common.streams._reader import Subscription
from temporalio.common.streams.system import models

E = TypeVar("E")
T = TypeVar("T")
_R = TypeVar("_R")

# How long a blocked sync call waits at a time.
_WAIT_STEP = 0.1


def _bound(handle: StreamHandle[Any]) -> _activity.ActivityStreams:
    attempt = _activity.current.get()
    if attempt is None:
        raise _errors.StreamConfigError(
            "No Stream backend runs this Activity. Register one on the Worker's client "
            "with stream_backends=."
        )
    streams = attempt.get(handle.backend)
    if streams is None:
        raise _errors.StreamConfigError(
            f"The Stream {handle.id} is of the backend {handle.backend}. The Worker has "
            f"{', '.join(attempt.backends)}."
        )
    return streams


def _check_close(handle: StreamHandle[Any]) -> None:
    # A Workflow's Stream closes only from its run chain, a Standalone Activity's only from that
    # Activity, and a client's only from a client. So only a Local Activity of the chain or the
    # owning Activity may close here. The Store refuses the others too, and this error comes
    # with no call.
    streams = _bound(handle)
    if handle.activity_id:
        owner = _ids.standalone_activity_producer_id(
            handle.activity_id, handle.activity_run_id or ""
        )
        if (streams.producer.kind, streams.producer.id) == (
            stream_pb.ProducerKind.PRODUCER_KIND_STANDALONE_ACTIVITY,
            owner,
        ):
            return
    local = streams.producer.kind == stream_pb.ProducerKind.PRODUCER_KIND_LOCAL_ACTIVITY
    if (
        not local
        or handle.activity_id
        or (handle.workflow_id and handle.workflow_id != streams.workflow_id)
    ):
        raise _errors.StreamNotOwnerError(
            f"Can't close the Stream {handle.id} from this Activity. A Workflow's Stream "
            "closes only from its run chain, and a client's only from a client.",
            reason="stream_not_owner",
        )


def _access(handle: StreamHandle[Any]) -> Access:
    streams = _bound(handle)

    async def ref() -> models.StreamRef:
        return handle._owner_ref()

    return Access(
        backend=streams.backend,
        converter=streams.converter,
        namespace=streams.namespace,
        ref=ref,
        writer=lambda: streams.writer(handle),
        # A handle with only an id names no chain, so the close names this Activity's own, and
        # the Store refuses it when another chain owns the Stream. A Store that nests a
        # Workflow's Stream finds none by such a handle, and the chain's id alone is no ref.
        workflow_id=handle.workflow_id
        or (None if streams.nests_workflow_streams else streams.workflow_id),
        first_execution_run_id=handle.first_execution_run_id,
    )


class ActivityTopic(_AsyncTopic[T]):
    """One Topic of an :py:class:`ActivityStream`. All Topics of a Stream share one order.

    .. warning::
        This API is experimental.
    """


class ActivityStream(StreamHandle[E]):
    """A Stream in an async Activity. The Activity writes, reads and closes it.

    An Activity gets a Stream as an argument, such as a field of its input, and a Standalone
    Activity creates its own with :py:func:`temporalio.activity.create_stream`. Each publish
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

        Only a Local Activity of the run chain that created the Stream, or the Standalone
        Activity that created it, may close it. Any other Activity gets
        :py:class:`StreamNotOwnerError`, so it can't end the Stream for every reader.
        """
        _check_close(self)
        await self._access().close(result, failure)

    async def __aenter__(self) -> ActivityStream[E]:
        """Use the Stream in ``async with``, which closes it at the end."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream, with the exception as its failure if there is one.

        A cancel closes it with a ``CancelledError`` failure. Another ``BaseException`` leaves
        it open.
        """
        close, failure = exit_failure(exc)
        if close:
            await self.close(failure=failure)


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


def create_stream(
    definition: type[E],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> Creating[ActivityStream[E]]:
    """Create a Stream that this Standalone Activity owns. See
    :py:func:`temporalio.activity.create_stream`.
    """
    info = temporalio.activity.info()
    if info.in_workflow:
        raise _errors.StreamConfigError(
            "Only a Standalone Activity owns Streams. An Activity of a Workflow writes to "
            "the Streams that it receives."
        )
    attempt = _activity.current.get()
    streams = attempt.get(backend) if attempt is not None else None
    if streams is None:
        raise _errors.StreamConfigError(
            f"No Stream backend {backend or 'by default'} runs this Activity"
        )
    if not streams.activity_streams:
        raise _errors.StreamConfigError(
            f"The backend {streams.backend_name} keeps no Stream of a Standalone Activity, "
            "since nothing would close it when the Activity ends. Use the native backend."
        )
    run_id = info.activity_run_id or ""
    stream_id = _ids.scoped_stream_id(
        info.activity_id, run_id, definition_of(definition).name
    )

    async def create() -> ActivityStream[E]:
        owner = models._ProducerTransferTypeConverter().from_transfer_type(
            streams.producer, models.Producer
        )
        request = models.CreateStreamRequest(
            stream_id=stream_id,
            retention=retention,
            owner=owner,
            ref=models.StreamRef(
                id=stream_id, activity_id=info.activity_id, activity_run_id=run_id
            ),
        )
        # The server checks the token against the Activity's current attempt, so a caller
        # can't put a Stream into an Activity it isn't.
        with _backend.with_task_token(streams.task_token):
            response = await streams.backend.service(
                streams.namespace, streams.converter
            ).create_stream(request)
        if response.status == stream_pb.StreamStatus.STREAM_STATUS_CLOSED:
            raise _errors.StreamClosedError(
                f"The Stream {stream_id} was created and closed before",
                reason="stream_closed",
            )
        return ActivityStream(
            id=stream_id,
            activity_id=info.activity_id,
            activity_run_id=run_id,
            backend=streams.backend_name or None,
            definition=definition,
        )

    return Creating(create)
