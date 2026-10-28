"""The writer of an Activity or a client Producer."""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

import temporalio.api.stream.v1 as stream_v1
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.common._streams import _errors

if TYPE_CHECKING:
    from temporalio.common._streams.writer_system import client as writer_client


class StreamWriter:
    """Writes the Records of one Producer to one Stream, through the generated Writer."""

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
        self._release: weakref.finalize[Any, StreamWriter] | None = None
        self._previous_completed_task = previous_completed_task
        self._failed_tasks = list(failed_tasks)
        self._writer: writer_client.Writer | None = None
        self._open_lock = asyncio.Lock()
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

    async def open(self) -> writer_client.Writer:
        """Open the crate's writer once.

        The open of a new attempt withdraws the Records of earlier attempts, even if it writes
        nothing.
        """
        from temporalio.common._streams.writer_system import models

        async with self._open_lock:
            if self._writer is None:
                producer = models._ProducerTransferTypeConverter().from_transfer_type(
                    self._producer, models.Producer
                )
                previous = self._previous_completed_task
                self._writer = await self._writers.open_writer(
                    stream=models.StreamRef(id=self._stream_id),
                    producer=producer,
                    previous_completed_task=models.WorkflowTaskEpoch(
                        started_event_id=previous.started_event_id
                    )
                    if previous is not None
                    else None,
                    failed_tasks=self._failed_tasks,
                )
                if self._free is not None:
                    # The crate keeps a writer for each open that nobody closed or freed. So
                    # free it when Python frees this writer.
                    self._release = weakref.finalize(
                        self, self._free, self._writer.writer_id
                    )
            return self._writer

    async def write(self, values: Sequence[Any]) -> int:
        """Write the values as Records now, in order, and return the last offset."""
        from temporalio.common._streams.writer_system import models

        if self._closed:
            raise _errors.StreamClosedError(
                "The writer is closed",
                reason=StreamErrorReason.STREAM_ERROR_REASON_CLOSED,
            )
        writer = await self.open()
        answer = await writer.write_records(
            [models.AppendRecord(body=value) for value in values]
        )
        return answer.last_offset or 0

    async def close(self) -> None:
        """Close the writer. The Stream stays open."""
        if self._closed:
            return
        self._closed = True
        if self._release is not None:
            self._release.detach()
        if self._writer is not None:
            await self._writer.close()
