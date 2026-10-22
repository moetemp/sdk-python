"""The writer of an Activity or a client Producer."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
from temporalio.contrib.streams import _errors
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams.proto import v1 as ext_pb


class StreamWriter:
    """Writes the Records of one Producer on one Stream, through the crate's writer."""

    def __init__(
        self,
        *,
        backend: StreamBackend,
        converter: temporalio.converter.DataConverter,
        namespace: str,
        stream_id: Callable[[], Awaitable[str]],
        producer: stream_pb.Producer,
    ) -> None:
        """Create a writer. It opens on its first write."""
        self._backend = backend
        self._converter = converter
        self._namespace = namespace
        self._resolve_id = stream_id
        self._producer = producer
        self._writer_id: int | None = None
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

    async def open(self) -> int:
        """Open the crate's writer once. The open of a new attempt withdraws the earlier one's
        Records, even when it writes nothing.
        """
        async with self._open_lock:
            if self._writer_id is None:
                self._writer_id = await self._backend.open_writer(
                    ext_pb.OpenWriterRequest(
                        namespace=self._namespace,
                        stream=stream_pb.StreamRef(id=await self._resolve_id()),
                        producer=self._producer,
                    )
                )
            return self._writer_id

    async def write(self, topic: str, values: Sequence[Any]) -> int:
        """Write the values as Records now. Returns the last Record's offset."""
        if self._closed:
            raise _errors.StreamClosedError(
                "The writer is closed", reason="stream_closed"
            )
        writer_id = await self.open()
        bodies = await self._converter.encode(list(values))
        return await self._backend.write_records(
            writer_id,
            [stream_pb.AppendRecord(topic=topic, body=body) for body in bodies],
        )

    async def close(self) -> None:
        """Drop the writer. The Stream stays open."""
        if self._closed:
            return
        self._closed = True
        if self._writer_id is not None:
            await self._backend.close_writer(self._writer_id)
