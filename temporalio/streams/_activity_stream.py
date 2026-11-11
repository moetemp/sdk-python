"""Streams in Activity code, which write as the attempt."""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Coroutine, Generator, Mapping
from datetime import timedelta
from typing import Any, TypeVar

from temporalio.client import Stream
from temporalio.streams import _activity, _backend, _errors
from temporalio.streams._access import Access, ReadSpec, reader
from temporalio.streams._items import Closed, Record
from temporalio.streams._offset import StreamOffset
from temporalio.streams._reader import Reader
from temporalio.streams._ref import StreamRef
from temporalio.streams._stream_client import StreamCalls

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


def _not_owner(ref: StreamRef[Any]) -> _errors.StreamError:
    # A Workflow or a client closes a Stream, and the Store refuses an Activity's close. Code in
    # an Activity only writes, so the refusal comes before any call.
    return _errors.not_owner(
        f"Can't close the Stream {ref.id} from an Activity. Only Workflow code or a client "
        "closes a Stream."
    )


def _check_loop() -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        raise _errors.unsupported(
            "A sync Activity can't wait for a Stream call. Wrap the Stream in a SyncStream."
        ) from None


class ActivityCalls(StreamCalls):
    """The calls of one Stream in an Activity attempt. They write as the attempt.

    Each publish starts its write at the call, as the Producer of the attempt. An Activity
    can't close a Stream, and neither can a Local Activity. A Local Activity runs before its
    Workflow Task commits. If the task then failed, the close would stay, and the task's retry
    couldn't write to the Stream again.
    """

    def __init__(self, streams: _activity.ActivityStreams, ref: StreamRef[Any]) -> None:
        """Create the calls of ``ref`` in the attempt of ``streams``."""
        self.streams = streams
        self._ref = ref

    def _access(self) -> Access:
        streams = self.streams
        stream_id = self._ref.id
        return Access(
            backend=streams.backend,
            converter=streams.converter,
            namespace=streams.namespace,
            stream_id=stream_id,
            writer=lambda: streams.writer(stream_id),
        )

    def append(
        self,
        values: tuple[Any, ...],
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> asyncio.Future[StreamOffset]:
        """Start a write. The write's task takes the call options."""
        _check_loop()
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return self._access().append(values)

    def read(
        self,
        read: ReadSpec,
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> Reader[Any]:
        """Start a read."""
        _check_loop()
        access = self._access()
        return reader(
            lambda position: access.items(read, position),
            read,
            _backend.options_of(rpc_metadata, rpc_timeout),
        )

    async def get_offset(
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> StreamOffset:
        """Get the newest offset."""
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return StreamOffset._from_wire(await self._access().get_offset())

    async def close(
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> None:
        """Raise a Stream error, since an Activity can't close a Stream."""
        raise _not_owner(self._ref)

    async def enter(self) -> None:
        """Raise before the block, since the close at its end would raise."""
        raise _not_owner(self._ref)

    async def exit(self, exc: BaseException | None) -> None:
        """Raise, since an Activity can't close a Stream."""
        raise _not_owner(self._ref)


def attempt_of(stream: Stream[Any]) -> _activity.ActivityStreams | None:
    """The Activity attempt that ``stream`` writes as, or None for a client's Stream."""
    calls = stream._calls
    return calls.streams if isinstance(calls, ActivityCalls) else None


def run_blocking(call: Coroutine[Any, Any, _R]) -> _R:
    """Run ``call`` on the Worker's event loop, and block the Activity's thread until it ends.

    Raises:
        StreamConfigError: No Stream backend runs the Activity.
    """
    streams = _activity.current.get()
    if streams is None:
        call.close()
        raise _errors.StreamConfigError(
            "No Stream backend runs this Activity. Register one on the Worker's client "
            "with stream_config=."
        )
    loop = streams.loop
    try:
        on_loop = asyncio.get_running_loop() is loop
    except RuntimeError:
        on_loop = False
    if on_loop:
        call.close()
        raise RuntimeError(
            "A SyncStream blocks, so use the Stream in an async Activity"
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


def sync_items(
    items: Reader[T],
) -> Generator[Record[T] | Closed, None, None]:
    """The items of a read, each of which blocks the Activity's thread."""
    try:
        while True:
            try:
                item = run_blocking(_next(items))
            except StopAsyncIteration:
                return
            yield item
    finally:
        run_blocking(items.aclose())


async def _next(items: Reader[T]) -> Record[T] | Closed:
    return await items.__anext__()
