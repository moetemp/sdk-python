"""The Stream state of one Activity attempt."""

from __future__ import annotations

import asyncio
import contextvars
from dataclasses import dataclass, field
from typing import Any

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams._writer import StreamWriter


@dataclass
class ActivityStreams:
    """What a Stream plugin gives the Activity handles for one Activity attempt."""

    backend: StreamBackend
    converter: temporalio.converter.DataConverter
    namespace: str
    producer: stream_pb.Producer
    loop: asyncio.AbstractEventLoop
    backend_name: str = ""
    writers: dict[str, StreamWriter] = field(default_factory=dict)
    # The attempt's BufferedPublishers, which the end of the attempt flushes.
    publishers: list[Any] = field(default_factory=list)

    def writer(self, stream_id: str) -> StreamWriter:
        """The attempt's writer for a Stream, one per Stream."""
        writer = self.writers.get(stream_id)
        if writer is None:

            async def known_id() -> str:
                return stream_id

            writer = StreamWriter(
                writers=self.backend.writers(self.namespace, self.converter),
                stream_id=known_id,
                producer=self.producer,
            )
            self.writers[stream_id] = writer
        return writer

    async def close(self) -> None:
        """Flush the attempt's publishers, then drop every writer of the attempt."""
        errors = []
        for publisher in self.publishers:
            try:
                await publisher._flush_at_attempt_end()
            except Exception as err:
                errors.append(err)
        for writer in self.writers.values():
            try:
                await writer.close()
            except Exception as err:
                errors.append(err)
        if errors:
            raise errors[0]


current: contextvars.ContextVar[ActivityStreams | None] = contextvars.ContextVar(
    "__temporal_activity_streams", default=None
)
