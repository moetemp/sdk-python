"""The Stream state of one Activity attempt."""

from __future__ import annotations

import asyncio
import contextvars
from dataclasses import dataclass, field
from typing import Any

import temporalio.api.stream.v1 as stream_v1
import temporalio.converter
from temporalio.common._streams._backend import BackendCalls
from temporalio.common._streams._writer import StreamWriter


@dataclass
class ActivityStreams:
    """What a Stream backend gives the Activity handles for one Activity attempt."""

    backend: BackendCalls
    converter: temporalio.converter.DataConverter
    namespace: str
    producer: stream_v1.Producer
    loop: asyncio.AbstractEventLoop
    backend_name: str = ""
    # Whether the Worker's backend is the default, which a handle with no backend name uses.
    default: bool = False
    # The attempt's Workflow id, which a close sends so the Store can check the Stream's owner.
    workflow_id: str | None = None
    # A Local Activity's writes use these to withdraw the run's orphaned Records.
    previous_completed_task: stream_v1.WorkflowTaskEpoch | None = None
    failed_tasks: tuple[int, ...] = ()
    writers: dict[str, StreamWriter] = field(default_factory=dict)
    # The attempt's BufferedPublishers, which the end of the attempt flushes.
    publishers: list[Any] = field(default_factory=list)

    def writer(self, stream_id: str) -> StreamWriter:
        """The attempt's writer for a Stream, one per Stream."""
        writer = self.writers.get(stream_id)
        if writer is None:
            writer = StreamWriter(
                writers=self.backend.writers(self.namespace, self.converter),
                stream_id=stream_id,
                producer=self.producer,
                free=self.backend.free_writer,
                previous_completed_task=self.previous_completed_task,
                failed_tasks=self.failed_tasks,
            )
            self.writers[stream_id] = writer
        return writer

    async def close(self) -> None:
        """Flush the attempt's publishers, then close every writer of the attempt."""
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
