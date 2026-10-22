"""The writer of an Activity or a client Producer."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

import temporalio.api.streamservice.v1 as stream_pb
from temporalio.contrib.streams import _errors

if TYPE_CHECKING:
    from temporalio.contrib.streams.system import models as system_models
    from temporalio.contrib.streams.writer_system import client as writer_client


class StreamWriter:
    """Writes the Records of one Producer on one Stream, through the generated Writer."""

    def __init__(
        self,
        *,
        writers: writer_client.StreamWriterClient,
        ref: Callable[[], Awaitable[system_models.StreamRef]],
        producer: stream_pb.Producer,
    ) -> None:
        """Create a writer. It opens on its first write.

        ``writers`` runs the values through the user's converter and codec.
        """
        self._writers = writers
        self._resolve_ref = ref
        self._producer = producer
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
        from temporalio.contrib.streams.writer_system import models

        async with self._open_lock:
            if self._writer is None:
                producer = models._ProducerTransferTypeConverter().from_transfer_type(
                    self._producer, models.Producer
                )
                ref = await self._resolve_ref()
                self._writer = await self._writers.open_writer(
                    models.OpenWriterRequest(
                        stream=models.StreamRef(
                            id=ref.id,
                            workflow_id=ref.workflow_id,
                            first_execution_run_id=ref.first_execution_run_id,
                        ),
                        producer=producer,
                    )
                )
            return self._writer

    async def write(self, topic: str, values: Sequence[Any]) -> int:
        """Write the values as Records now. Returns the last Record's offset."""
        from temporalio.contrib.streams.writer_system import models

        if self._closed:
            raise _errors.StreamClosedError(
                "The writer is closed", reason="stream_closed"
            )
        writer = await self.open()
        answer = await writer.write_records(
            [models.AppendRecord(topic=topic, body=value) for value in values]
        )
        return answer.last_offset or 0

    async def close(self) -> None:
        """Drop the writer. The Stream stays open."""
        if self._closed:
            return
        self._closed = True
        if self._writer is not None:
            await self._writer.close()
