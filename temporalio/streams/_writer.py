"""The writer of an Activity or a client Producer."""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Callable, Generator, Sequence
from typing import TYPE_CHECKING, Any

import temporalio.api.stream.v1 as stream_v1
from temporalio.streams import _errors

if TYPE_CHECKING:
    from temporalio.streams.writer_system import client as writer_client
    from temporalio.streams.writer_system.client import Writer


class WriteFuture(asyncio.Future[int]):
    """The future of a write. A caller that gets its result or its error sets ``retrieved``.

    A write error that no caller gets still raises, at the end of a Workflow run or an Activity
    attempt, or at the next call of a client's Stream.
    """

    retrieved = False

    def result(self) -> int:
        """The last offset of the write."""
        self.retrieved = True
        return super().result()

    def exception(self) -> BaseException | None:
        """The error of the write, or None."""
        self.retrieved = True
        return super().exception()

    def __await__(self) -> Generator[Any, None, int]:
        """Wait for the write, and return its last offset."""
        # The await of asyncio's future doesn't call result(), so this one does.
        if not self.done():
            self._asyncio_future_blocking = True
            yield self
        return self.result()

    __iter__ = __await__


def unretrieved_error(future: asyncio.Future[int]) -> BaseException | None:
    """The error of a failed write that no caller got, else None."""
    if not isinstance(future, WriteFuture) or future.retrieved or future.cancelled():
        return None
    return asyncio.Future.exception(future)


class RecordWriter:
    """Writes the Records of one Producer to one Stream, through the writer client."""

    def __init__(
        self,
        *,
        writers: writer_client.StreamWriterClientExtension,
        stream_id: str,
        producer: stream_v1.Producer,
        free: Callable[[int], None] | None = None,
        previous_completed_task: stream_v1.WorkflowTaskEpoch | None = None,
        failed_tasks: Sequence[int] = (),
    ) -> None:
        """Create a writer. It opens on the first write.

        ``writers`` encodes the values with the user's converter and codec. ``free`` releases
        the crate's writer if Python frees this one without a close.
        """
        self._writers = writers
        self._stream_id = stream_id
        self._producer = producer
        self._free = free
        self._release: weakref.finalize[Any, RecordWriter] | None = None
        self._previous_completed_task = previous_completed_task
        self._failed_tasks = list(failed_tasks)
        self._writer: Writer | None = None
        self._open_lock = asyncio.Lock()
        # The last write that started. Each write waits for the one before it, so the order is
        # the order of the calls, even before a write's task first runs.
        self._last: asyncio.Task[int] | None = None
        # The writes that failed. The first error that no caller got raises later.
        self._failed: list[WriteFuture] = []
        self._closed = False

    @property
    def producer(self) -> stream_v1.Producer:
        """The Producer that the writer writes as."""
        return self._producer

    async def withdraw_earlier_attempts(self) -> None:
        """Open the writer of a new attempt, which withdraws the Records of earlier attempts.

        A Stream that doesn't exist has nothing to withdraw. A closed or expired Stream accepts
        no write, so the attempt continues, and a later write raises the real error.
        """
        try:
            await self.open()
        except (
            _errors.StreamNotFoundError,
            _errors.StreamClosedError,
            _errors.StreamExpiredError,
        ):
            pass

    async def open(self) -> Writer:
        """Open the crate's writer once.

        The open of a new attempt withdraws the Records of earlier attempts, even if it writes
        nothing.
        """
        from temporalio.streams.writer_system import models

        async with self._open_lock:
            if self._writer is None:
                producer = models._ProducerTransferTypeConverter().from_transfer_type(
                    self._producer, models.Producer
                )
                previous = self._previous_completed_task
                self._writer = await self._writers.open_writer(
                    ref=models.StreamRef(id=self._stream_id),
                    writer=models.StreamWriter(
                        producer=producer,
                        previous_completed_task=models.WorkflowTaskEpoch(
                            started_event_id=previous.started_event_id
                        )
                        if previous is not None
                        else None,
                        failed_tasks=self._failed_tasks,
                    ),
                )
                if self._free is not None:
                    # The crate keeps a writer for each open that nobody closed or freed. So
                    # free it when Python frees this writer.
                    self._release = weakref.finalize(
                        self, self._free, self._writer.writer_id
                    )
            return self._writer

    def write(self, values: Sequence[Any]) -> WriteFuture:
        """Start the write of the values as Records, and return its future.

        Writes start in the order of the calls, also when nobody awaits a future. A cancel of
        the future doesn't stop the write, since the write keeps its place in the order. The
        future gives the last offset.

        Raises:
            StreamClosedError: The writer is closed, since its Stream object closed it.
        """
        if self._closed:
            raise _errors.StreamClosedError(
                f"Can't write to the Stream {self._stream_id}, since this object closed its "
                "writer"
            )
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._write_after(self._last, values))
        self._last = task
        future = WriteFuture(loop=loop)

        def done(task: asyncio.Task[int]) -> None:
            if self._last is task:
                self._last = None
            error = None if task.cancelled() else task.exception()
            if future.cancelled():
                return
            if task.cancelled():
                future.cancel()
            elif error is not None:
                future.set_exception(error)
                self._failed.append(future)
            else:
                future.set_result(task.result())

        task.add_done_callback(done)
        return future

    def unretrieved_error(self) -> BaseException | None:
        """The first write error that no caller got, or None.

        The caller owns the error, so the later errors of the same writes don't raise again.
        """
        failed, self._failed = self._failed, []
        error = next((e for e in map(unretrieved_error, failed) if e is not None), None)
        for future in failed:
            future.retrieved = True
        return error

    async def _write_after(
        self, previous: asyncio.Task[int] | None, values: Sequence[Any]
    ) -> int:
        from temporalio.streams.writer_system import models

        if previous is not None:
            # The earlier write's error belongs to its own caller.
            await asyncio.wait([previous])
        writer = await self.open()
        answer = await writer.write_records(
            [models.AppendRecord(body=value) for value in values]
        )
        return answer.last_offset or 0

    async def close(self) -> None:
        """Wait for the writes that started, then close the writer. The Stream stays open."""
        if self._closed:
            return
        self._closed = True
        if self._last is not None:
            await asyncio.wait([self._last])
        if self._release is not None:
            self._release.detach()
        if self._writer is not None:
            await self._writer.close()
