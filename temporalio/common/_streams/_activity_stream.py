"""Stream handles in Activity code."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Coroutine, Generator
from datetime import timedelta
from typing import Any, Generic, TypeVar

from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.common._streams import _activity, _errors
from temporalio.common._streams._access import Access, read_spec, subscription
from temporalio.common._streams._handle import StreamHandle
from temporalio.common._streams._items import Closed, End, Record, Superseded
from temporalio.common._streams._reader import Subscription

T = TypeVar("T")
_R = TypeVar("_R")

# How long a blocked sync call waits at a time.
_WAIT_STEP = 0.1


def _start(
    loop: asyncio.AbstractEventLoop,
    call: Coroutine[Any, Any, _R],
    future: concurrent.futures.Future[_R],
) -> None:
    # Runs in the Worker's loop. The Activity thread can cancel the future before this runs.
    if future.cancelled():
        call.close()
        return
    task = loop.create_task(call)

    def task_done(task: asyncio.Task[_R]) -> None:
        if task.cancelled():
            future.cancel()
        elif future.set_running_or_notify_cancel():
            error = task.exception()
            if error is None:
                future.set_result(task.result())
            else:
                future.set_exception(error)

    def future_done(future: concurrent.futures.Future[_R]) -> None:
        if future.cancelled() and not loop.is_closed():
            loop.call_soon_threadsafe(task.cancel)

    task.add_done_callback(task_done)
    future.add_done_callback(future_done)


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
            "Worker", [streams.backend_name], "a handle that has its backend name"
        )
    return streams


def _not_owner(handle: StreamHandle[Any]) -> _errors.StreamNotOwnerError:
    return _errors.StreamNotOwnerError(
        f"Can't close the Stream {handle.id} from an Activity. A Workflow's Stream closes "
        "only from Workflow code, and a client's only from a client.",
        reason=StreamErrorReason.STREAM_ERROR_REASON_NOT_OWNER,
    )


def _access(handle: StreamHandle[Any]) -> Access:
    streams = _bound(handle)
    stream_id = handle.id
    return Access(
        backend=streams.backend,
        converter=streams.converter,
        namespace=streams.namespace,
        stream_id=stream_id,
        writer=lambda: streams.writer(stream_id),
    )


class ActivityStream(StreamHandle[T]):
    """A Stream in an async Activity. The Activity can write and read it.

    An Activity gets a Stream only as an argument, such as a field of its input. Each publish
    writes before it returns. Use :py:class:`BufferedPublisher` to batch.

    .. warning::
        This API is experimental.
    """

    def _access(self) -> Access:
        return _access(self)

    async def publish(self, *values: T) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value.

        Raises:
            StreamNotFoundError: Nobody created the Stream.
        """
        return await self._access().publish(values)

    def subscribe(
        self,
        *,
        offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
    ) -> Subscription[T]:
        """Read the Stream's items in order. See :py:meth:`ClientStream.subscribe`."""
        read = read_spec(
            self._payload_type,
            offset=offset,
            last=last,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        access = self._access()
        return subscription(lambda position: access.items(read, position), read)

    async def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one, or 0 if it has no Record.

        Offsets are sparse and above zero, so ``offset=0`` reads from the start.
        """
        return await self._access().get_offset()

    async def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Raise :py:class:`StreamNotOwnerError`, since an Activity can't close a Stream.

        A Workflow's Stream closes only from Workflow code, and a client's Stream only from a
        client. The rule includes a Local Activity of the run chain that created the Stream. A
        Local Activity runs before its Workflow Task commits. If the task then failed, the close
        would stay, and the task's retry couldn't create the Stream again.
        """
        del result, failure
        raise _not_owner(self)


class SyncSubscription(Generic[T]):
    """The items of one read of a Stream in a sync Activity, in order. Iterate it with ``for``.

    Each iteration blocks the Activity's thread. See :py:class:`Subscription`.

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


class SyncActivityStream(StreamHandle[T]):
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
        # The future exists before the call is scheduled, so a cancel raised in this thread at
        # any point after the schedule reaches the except below, which cancels the call.
        future: concurrent.futures.Future[_R] = concurrent.futures.Future()
        try:
            try:
                loop.call_soon_threadsafe(_start, loop, call, future)
            except RuntimeError:
                # The Worker's loop is closed, such as at shutdown, so the call never starts.
                call.close()
                raise
            while True:
                # Short waits let the Worker raise an exception in this thread, such as a cancel.
                try:
                    return future.result(timeout=_WAIT_STEP)
                except concurrent.futures.TimeoutError:
                    continue
        except BaseException:
            # Cancel the call too, so a long poll doesn't keep running in the Worker's loop after
            # the Activity thread is canceled, such as from a heartbeat.
            future.cancel()
            raise

    def publish(self, *values: T) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value.

        Raises:
            StreamNotFoundError: Nobody created the Stream.
        """
        return self._run(self._access().publish(values))

    def subscribe(
        self,
        *,
        offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
    ) -> SyncSubscription[T]:
        """Read the Stream's items in order. See :py:meth:`ClientStream.subscribe`."""
        read = read_spec(
            self._payload_type,
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
        """The next offset of the Stream, the newest offset plus one, or 0 if it has no Record.

        Offsets are sparse and above zero, so ``offset=0`` reads from the start.
        """
        return self._run(self._access().get_offset())

    def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Raise :py:class:`StreamNotOwnerError`. See :py:meth:`ActivityStream.close`."""
        del result, failure
        raise _not_owner(self)


async def _next(items: Subscription[T]) -> Record[T] | Superseded | Closed:
    return await items.__anext__()
