"""The Stream state of one Activity attempt."""

from __future__ import annotations

import asyncio
import contextvars
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import temporalio.api.stream.v1 as stream_v1
import temporalio.converter
from temporalio.streams._backend import BackendCalls
from temporalio.streams._writer import RecordWriter

if TYPE_CHECKING:
    import temporalio.client
    from temporalio.client import BufferedAppender


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
    # The attempt's BufferedAppenders, which the end of the attempt flushes.
    appenders: list[BufferedAppender[Any]] = field(default_factory=list)
    # The Worker's client. Its Streams in the attempt write as the attempt, and other clients'
    # Streams write as those clients.
    client: temporalio.client.Client | None = None

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
        """Flush the attempt's appenders, then close every writer of the attempt.

        A write error that no caller got fails the attempt.
        """
        errors: list[BaseException] = []
        for appender in self.appenders:
            try:
                await appender._flush_at_attempt_end()
            except Exception as err:
                errors.append(err)
        for writer in self.writers.values():
            try:
                await writer.close()
            except Exception as err:
                errors.append(err)
            # As the end of a Workflow run does, the end of the attempt raises a write error that
            # no caller got. A retry then writes the Records again, and the attempt doesn't end
            # with a gap.
            lost = writer.unretrieved_error()
            if lost is not None:
                errors.append(lost)
        if errors:
            raise errors[0]


current: contextvars.ContextVar[ActivityStreams | None] = contextvars.ContextVar(
    "__temporal_activity_streams", default=None
)
