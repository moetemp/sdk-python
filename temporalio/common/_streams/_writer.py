"""The writer of an Activity or a client Producer."""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

import temporalio.api.streamservice.v1 as stream_pb
from temporalio.common._streams import _backend, _errors

if TYPE_CHECKING:
    from temporalio.common._streams.system import models as system_models
    from temporalio.common._streams.writer_system import client as writer_client


class StreamWriter:
    """Writes the Records of one Producer to one Stream, through the generated Writer."""

    def __init__(
        self,
        *,
        writers: writer_client.StreamWriterClient,
        ref: Callable[[], Awaitable[system_models.StreamRef]],
        producer: stream_pb.Producer,
        free: Callable[[int], None] | None = None,
        previous_completed_task: stream_pb.WorkflowTaskEpoch | None = None,
        failed_tasks: Sequence[int] = (),
        task_token: bytes = b"",
    ) -> None:
        """Create a writer. It opens on the first write.

        ``writers`` encodes the values with the user's converter and codec. ``free`` releases
        the crate's writer if Python frees this one without a close.
        """
        self._writers = writers
        self._resolve_ref = ref
        self._producer = producer
        self._free = free
        self._release: weakref.finalize[Any, StreamWriter] | None = None
        self._previous_completed_task = previous_completed_task
        self._task_token = task_token
        self._failed_tasks = list(failed_tasks)
        self._writer: writer_client.Writer | None = None
        self._open_lock = asyncio.Lock()
        self._closed = False

    @property
    def producer(self) -> stream_pb.Producer:
        """The Producer that the writer writes as."""
        return self._producer

    @property
    def task_token(self) -> bytes:
        """The task token of the Activity attempt that writes, or empty."""
        return self._task_token

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
                ref = await self._resolve_ref()
                previous = self._previous_completed_task
                # The crate keeps the token with the writer and sends it with each batch.
                with _backend.with_task_token(self._task_token):
                    self._writer = await self._writers.open_writer(
                        models.OpenWriterRequest(
                            stream=models.StreamRef(
                                id=ref.id,
                                workflow_id=ref.workflow_id,
                                first_execution_run_id=ref.first_execution_run_id,
                                activity_id=ref.activity_id,
                                activity_run_id=ref.activity_run_id,
                            ),
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
                    # The crate keeps a writer for each open that nobody closed or freed. So
                    # free it when Python frees this writer.
                    self._release = weakref.finalize(
                        self, self._free, self._writer.writer_id
                    )
            return self._writer

    async def write(self, records: Sequence[tuple[str, Any]]) -> int:
        """Write ``(topic, value)`` pairs as Records now, in order, and return the last offset."""
        from temporalio.common._streams.writer_system import models

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
        """Close the writer. The Stream stays open."""
        if self._closed:
            return
        self._closed = True
        if self._release is not None:
            self._release.detach()
        if self._writer is not None:
            await self._writer.close()
