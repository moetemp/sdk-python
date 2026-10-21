"""Streams in Activity code."""

from __future__ import annotations

import contextvars
from dataclasses import dataclass, field
from datetime import timedelta

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
from temporalio.contrib.streams import _errors
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams._outside import OutsideStream, batch_options
from temporalio.contrib.streams._stream import StreamRef
from temporalio.contrib.streams._writer import BatchOptions, StreamWriter


@dataclass
class ActivityStreams:
    """What a Stream plugin gives the facade for one Activity attempt.

    .. warning::
        This API is experimental.
    """

    backend: StreamBackend
    converter: temporalio.converter.DataConverter
    namespace: str
    producer: stream_pb.Producer
    default_ref: StreamRef | None
    batch: BatchOptions = BatchOptions()
    writers: dict[str, StreamWriter] = field(default_factory=dict)

    def writer(self, stream_id: str, batch: BatchOptions | None = None) -> StreamWriter:
        """The attempt's writer for a Stream, one per Stream."""
        writer = self.writers.get(stream_id)
        if writer is None:
            writer = StreamWriter(
                backend=self.backend,
                converter=self.converter,
                namespace=self.namespace,
                stream_id=stream_id,
                producer=self.producer,
                batch=batch or self.batch,
                # The default Stream has no key until its first write, so its create goes
                # first. A received Stream must exist already.
                create_first=self.default_ref is not None
                and stream_id == self.default_ref.id,
            )
            self.writers[stream_id] = writer
        return writer

    async def close(self) -> None:
        """Flush and drop every writer of the attempt."""
        errors = []
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


def activity_stream(
    ref: StreamRef | None = None,
    *,
    batch_interval: timedelta | None = None,
    max_batch_size: int | None = None,
    max_retry_duration: timedelta | None = None,
) -> OutsideStream:
    """A Stream for this Activity to write to and read.

    Writes are buffered and flushed every ``batch_interval``. The plugin flushes before the
    Activity completes, and a failed flush fails the attempt.

    Args:
        ref: A Stream that the Activity received. With none, the default Stream of the
            Workflow that scheduled the Activity.
        batch_interval: How often the writer flushes. Defaults to 100 ms.
        max_batch_size: At most this many Records in one batch.
        max_retry_duration: How long a flush retries a Store failure.
    """
    streams = current.get()
    if streams is None:
        raise _errors.StreamTransportMissingError(
            "No Stream plugin runs this Activity, such as RedisStreamPlugin"
        )
    if ref is None:
        if streams.default_ref is None:
            raise _errors.StreamError(
                "A Standalone Activity has no default Stream, so pass a StreamRef"
            )
        ref = streams.default_ref
    stream_id = ref.id
    batch = None
    if batch_interval or max_batch_size or max_retry_duration:
        batch = batch_options(batch_interval, max_batch_size, max_retry_duration)
    return OutsideStream(
        backend=streams.backend,
        converter=streams.converter,
        namespace=streams.namespace,
        stream_id=stream_id,
        writer=lambda: streams.writer(stream_id, batch),
    )
