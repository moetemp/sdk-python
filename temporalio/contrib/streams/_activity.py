"""The Stream state of one Activity attempt."""

from __future__ import annotations

import asyncio
import contextvars
from dataclasses import dataclass, field
from typing import Any

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._writer import StreamWriter
from temporalio.contrib.streams.system import models


@dataclass
class ActivityStreams:
    """What a Stream plugin gives the Activity handles for one Activity attempt."""

    backend: StreamBackend
    converter: temporalio.converter.DataConverter
    namespace: str
    producer: stream_pb.Producer
    loop: asyncio.AbstractEventLoop
    backend_name: str = ""
    # The Workflow of the attempt, so a close checks that the Stream is its run chain's.
    workflow_id: str | None = None
    # A Local Activity's run history, which lets its writes withdraw the run's orphaned Records.
    previous_completed_task: stream_pb.WorkflowTaskEpoch | None = None
    failed_tasks: tuple[int, ...] = ()
    # An Activity's task token, which its writes send, so the native server checks the
    # Producer against the Activity's current attempt. Empty for a Local Activity.
    task_token: bytes = b""
    # Whether the backend keeps the Streams of a Standalone Activity.
    activity_streams: bool = False
    writers: dict[str, StreamWriter] = field(default_factory=dict)
    # The attempt's BufferedPublishers, which the end of the attempt flushes.
    publishers: list[Any] = field(default_factory=list)

    def writer(self, handle: StreamHandle[Any]) -> StreamWriter:
        """The attempt's writer for a Stream, one per Stream."""
        writer = self.writers.get(handle.id)
        if writer is None:

            async def ref() -> models.StreamRef:
                return handle._owner_ref()

            writer = StreamWriter(
                writers=self.backend.writers(self.namespace, self.converter),
                ref=ref,
                producer=self.producer,
                free=self.backend.free_writer,
                previous_completed_task=self.previous_completed_task,
                failed_tasks=self.failed_tasks,
                task_token=self.task_token,
            )
            self.writers[handle.id] = writer
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


@dataclass
class AttemptStreams:
    """The Stream state of one Activity attempt, one entry for each backend of the Worker."""

    # The backend that a handle with no backend name uses.
    default: str
    backends: dict[str, ActivityStreams] = field(default_factory=dict)

    def get(self, backend: str | None) -> ActivityStreams | None:
        """The attempt's state for a backend name. None or empty is the default."""
        return self.backends.get(backend or self.default)


current: contextvars.ContextVar[AttemptStreams | None] = contextvars.ContextVar(
    "__temporal_activity_streams", default=None
)
