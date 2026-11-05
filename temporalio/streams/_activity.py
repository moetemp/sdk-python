"""The Stream state of one Activity attempt."""

from __future__ import annotations

import asyncio
import contextvars
from dataclasses import dataclass, field

import temporalio.api.stream.v1 as stream_v1
import temporalio.converter
from temporalio.streams._backend import BackendCalls
from temporalio.streams._writer import RecordWriter


@dataclass
class ActivityStreams:
    """What a Stream backend gives the Streams of one Activity attempt."""

    backend: BackendCalls
    converter: temporalio.converter.DataConverter
    namespace: str
    producer: stream_v1.Producer
    loop: asyncio.AbstractEventLoop
    # The attempt's Workflow ID, which the close Activity checks against a bound Stream's owner.
    workflow_id: str | None = None
    # A Local Activity's writes use these to withdraw the run's orphaned Records.
    previous_completed_task: stream_v1.WorkflowTaskEpoch | None = None
    failed_tasks: tuple[int, ...] = ()
    writers: dict[str, RecordWriter] = field(default_factory=dict)

    def writer(self, stream_id: str) -> RecordWriter:
        """The attempt's writer for a Stream, one per Stream."""
        writer = self.writers.get(stream_id)
        if writer is None:
            writer = RecordWriter(
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
        """Close every writer of the attempt."""
        errors: list[BaseException] = []
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
