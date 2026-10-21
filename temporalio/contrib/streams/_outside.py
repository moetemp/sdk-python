"""Streams outside Workflow code, for Activities and clients."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any, TypeVar

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
from temporalio.contrib.streams import _reader
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams._reader import Item
from temporalio.contrib.streams._stream import StreamRef, Topic, _Readable
from temporalio.contrib.streams._writer import BatchOptions, StreamWriter

T = TypeVar("T")


class OutsideStream(_Readable):
    """A Stream that an Activity or a client reads and writes.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        *,
        backend: StreamBackend,
        converter: temporalio.converter.DataConverter,
        namespace: str,
        stream_id: str,
        writer: Callable[[], StreamWriter],
        wait_for_create: timedelta | None = None,
        run_ended: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        """Create a Stream handle. Use the facade functions instead."""
        self._backend = backend
        self._converter = converter
        self._namespace = namespace
        self._id = stream_id
        self._writer_factory = writer
        self._writer: StreamWriter | None = None
        self._wait_for_create = wait_for_create
        self._run_ended = run_ended
        # A read of every Topic still decodes each known Topic with its type.
        self._types: dict[str, Any] = {}

    @property
    def ref(self) -> StreamRef:
        """Reference to pass to Workflows, Activities and other clients."""
        return StreamRef(self._id)

    def topic(self, name: str, *, type: type[T] = Any) -> Topic[T]:  # type: ignore[assignment]
        """A Topic of this Stream. ``type`` decodes its values. ``RawValue`` keeps the
        Payload.
        """
        self._types[name] = type
        return Topic(self, name, type)

    def _get_writer(self) -> StreamWriter:
        if self._writer is None:
            self._writer = self._writer_factory()
        return self._writer

    def _publish(self, topic: str, value: Any, force_flush: bool) -> None:
        self._get_writer().publish(topic, value, force_flush)

    async def flush(self) -> None:
        """Wait until the Records published so far are in the Store."""
        if self._writer is not None:
            await self._writer.flush()

    def subscribe(
        self,
        *,
        topics: Sequence[Topic[Any]] | None = None,
        from_offset: int | None = None,
        wait_new_records: bool = True,
        idle_timeout: timedelta | None = None,
    ) -> AsyncIterator[Item]:
        """Read the Stream's items in order.

        The read ends after a ``Closed`` item, after an ``Idle`` item, or at the newest Record
        when ``wait_new_records`` is False. A ``Superseded`` item withdraws earlier Records of
        a failed attempt, and it passes every Topic filter.

        Args:
            topics: Read only these Topics. Defaults to every Topic.
            from_offset: Start at this offset, inclusive. Defaults to the first Record.
            wait_new_records: Wait for new Records at the end of the Stream.
            idle_timeout: End with an ``Idle`` item when no new Record comes for this long.
        """
        return _reader.read(
            self._backend.service(self._namespace),
            self._converter,
            self._id,
            topics=[topic.name for topic in topics] if topics else None,
            types={
                **self._types,
                **{topic.name: topic.type for topic in topics or []},
            },
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            idle_timeout=idle_timeout,
            wait_for_create=self._wait_for_create,
            run_ended=self._run_ended,
        )

    async def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one."""
        response = await self._backend.call_async(
            "DescribeStream",
            stream_pb.DescribeStreamRequest(
                namespace=self._namespace, stream=stream_pb.StreamRef(id=self._id)
            ),
            stream_pb.DescribeStreamResponse,
        )
        # Offsets start above zero, so zero means the Stream has no Record.
        return response.last_offset + 1 if response.last_offset else 0


def batch_options(
    batch_interval: timedelta | None,
    max_batch_size: int | None,
    max_retry_duration: timedelta | None,
) -> BatchOptions:
    """Batch options from the facade keywords."""
    return BatchOptions(
        batch_interval=batch_interval,
        max_batch_size=max_batch_size,
        max_retry_duration=max_retry_duration,
    )
