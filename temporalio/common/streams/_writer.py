"""The writer of an Activity or a client Producer."""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

import temporalio.api.streamservice.v1 as stream_pb
from temporalio.common.streams import _errors

if TYPE_CHECKING:
    from temporalio.common.streams.writer_system import client as writer_client


class StreamWriter:
    """Writes the Records of one Producer on one Stream, through the generated Writer."""

    def __init__(
        self,
        *,
        writers: writer_client.StreamWriterClient,
        stream_id: Callable[[], Awaitable[str]],
        producer: stream_pb.Producer,
        free: Callable[[int], None] | None = None,
        previous_completed_task: stream_pb.WorkflowTaskEpoch | None = None,
        failed_tasks: Sequence[int] = (),
    ) -> None:
        """Create a writer. It opens on its first write.

        ``writers`` runs the values through the user's converter and codec. ``free`` releases
        the crate's writer when Python drops this one with no close.
        """
        self._writers = writers
        self._resolve_id = stream_id
        self._producer = producer
        self._free = free
        self._release: weakref.finalize[Any, StreamWriter] | None = None
        self._previous_completed_task = previous_completed_task
        self._failed_tasks = list(failed_tasks)
        self._writer: writer_client.Writer | None = None
        self._open_lock = asyncio.Lock()
        self._closed = False

    @property
    def producer(self) -> stream_pb.Producer:
        """The Producer that the writer writes as."""
        return self._producer

    async def withdraw_earlier_attempts(self) -> None:
        """Open the writer of a new attempt, which withdraws the Records of the earlier ones.

        A Stream that does not exist has nothing to withdraw.
        """
        try:
            await self.open()
        except _errors.StreamNotFoundError:
            pass

    async def open(self) -> writer_client.Writer:
        """Open the crate's writer once. The open of a new attempt withdraws the earlier one's
        Records, even when it writes nothing.
        """
        from temporalio.common.streams.writer_system import models

        async with self._open_lock:
            if self._writer is None:
                producer = models._ProducerTransferTypeConverter().from_transfer_type(
                    self._producer, models.Producer
                )
                previous = self._previous_completed_task
                self._writer = await self._writers.open_writer(
                    models.OpenWriterRequest(
                        stream=models.StreamRef(id=await self._resolve_id()),
                        producer=producer,
                        previous_completed_task=models.WorkflowTaskEpoch(
                            started_event_id=previous.started_event_id
                        )
                        if previous is not None
                        else None,
                        failed_tasks=self._failed_tasks,
                    )
                )
                if self._free is not None:
                    # The crate keeps a writer for each open that no one closed or freed, so a
                    # writer that Python drops frees its open.
                    self._release = weakref.finalize(
                        self, self._free, self._writer.writer_id
                    )
            return self._writer

    async def write(self, records: Sequence[tuple[str, Any]]) -> int:
        """Write ``(topic, value)`` pairs as Records now, in order. Returns the last Record's
        offset.
        """
        from temporalio.common.streams.writer_system import models

        if self._closed:
            raise _errors.StreamClosedError(
                "The writer is closed", reason="stream_closed"
            )
        writer = await self.open()
        answer = await writer.write_records(
            [models.AppendRecord(topic=topic, body=value) for topic, value in records]
        )
        return answer.last_offset or 0

    async def close(self) -> None:
        """Drop the writer. The Stream stays open."""
        if self._closed:
            return
        self._closed = True
        if self._release is not None:
            self._release.detach()
        if self._writer is not None:
            await self._writer.close()
