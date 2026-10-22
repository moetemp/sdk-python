"""The buffered writer of an Activity or a client Producer."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.common
import temporalio.converter
from temporalio.contrib.streams import _convert, _errors
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams.proto import v1 as ext_pb


@dataclass(frozen=True)
class BatchOptions:
    """How a writer batches Records. An unset value takes the crate's default."""

    batch_interval: timedelta | None = None
    max_batch_size: int | None = None
    max_retry_duration: timedelta | None = None
    max_buffered_records: int | None = None


class StreamWriter:
    """Buffers the Records of one Producer on one Stream, through the crate's writer."""

    def __init__(
        self,
        *,
        backend: StreamBackend,
        converter: temporalio.converter.DataConverter,
        namespace: str,
        stream_id: Callable[[], Awaitable[str]],
        producer: stream_pb.Producer,
        batch: BatchOptions,
        create_first: bool,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Create a writer. It opens on its first publish.

        ``loop`` runs the writer's async work, so a sync Activity on another thread can
        publish too.
        """
        self._backend = backend
        self._converter = converter
        self._namespace = namespace
        self._resolve_id = stream_id
        self._stream_id = ""
        self._producer = producer
        self._batch = batch
        self._create_first = create_first
        self._loop = loop
        self._writer_id: int | None = None
        self._open_lock = asyncio.Lock()
        self._pending: asyncio.Task[None] | None = None
        self._closed = False
        backend.writers.add(self)

    @property
    def producer(self) -> stream_pb.Producer:
        """The Producer that the writer writes as."""
        return self._producer

    async def withdraw_earlier_attempts(self) -> None:
        """Open the writer of a new attempt, which withdraws the Records of the earlier ones.

        It creates no Stream. A Stream that does not exist has nothing to withdraw, and the
        first publish creates it.
        """
        try:
            await self.open(create=False)
        except _errors.StreamNotFoundError:
            pass

    async def open(self, *, create: bool = True) -> int:
        """Open the crate's writer once. The open of a new attempt withdraws the earlier one's
        Records, even when it publishes nothing.
        """
        async with self._open_lock:
            if self._writer_id is None:
                if not self._stream_id:
                    self._stream_id = await self._resolve_id()
                if self._create_first and create:
                    await self._backend.call_async(
                        "CreateStream",
                        stream_pb.CreateStreamRequest(
                            namespace=self._namespace, stream_id=self._stream_id
                        ),
                        stream_pb.CreateStreamResponse,
                    )
                request = ext_pb.OpenWriterRequest(
                    namespace=self._namespace,
                    stream=stream_pb.StreamRef(id=self._stream_id),
                    producer=self._producer,
                    max_batch_size=self._batch.max_batch_size or 0,
                    max_buffered_records=self._batch.max_buffered_records or 0,
                )
                if self._batch.batch_interval is not None:
                    request.batch_interval.FromTimedelta(self._batch.batch_interval)
                if self._batch.max_retry_duration is not None:
                    request.max_retry_duration.FromTimedelta(
                        self._batch.max_retry_duration
                    )
                self._writer_id = await self._backend.open_writer(request)
            return self._writer_id

    def publish(self, topic: str, value: Any, force_flush: bool) -> None:
        """Add one Record to the buffer and return at once."""
        try:
            on_loop = asyncio.get_running_loop() is self._loop
        except RuntimeError:
            on_loop = False
        if not on_loop:
            # A sync Activity runs on another thread, so the loop does the publish, and the
            # thread waits only for the hand-off.
            asyncio.run_coroutine_threadsafe(
                self._publish_on_loop(topic, value, force_flush), self._loop
            ).result()
            return
        self._publish(topic, value, force_flush)

    async def _publish_on_loop(self, topic: str, value: Any, force_flush: bool) -> None:
        self._publish(topic, value, force_flush)

    def _publish(self, topic: str, value: Any, force_flush: bool) -> None:
        if self._closed:
            raise _errors.StreamClosedError(
                "The writer is closed", reason="stream_closed"
            )
        body = _convert.encode_now(self._converter, value)
        if body is not None and self._writer_id is not None and self._pending is None:
            self._send(topic, body, force_flush)
            return
        # The open and the codec are async, so they run in order in the background, and a
        # flush raises what they raised.
        previous = self._pending
        self._pending = self._loop.create_task(
            self._publish_later(previous, topic, value, force_flush)
        )

    async def _publish_later(
        self,
        previous: asyncio.Task[None] | None,
        topic: str,
        value: Any,
        force_flush: bool,
    ) -> None:
        if previous is not None:
            await previous
        await self.open()
        self._send(topic, await _convert.encode(self._converter, value), force_flush)

    def _send(
        self, topic: str, body: temporalio.common.RawValue, force_flush: bool
    ) -> None:
        assert self._writer_id is not None
        self._backend.publish(
            self._writer_id,
            [stream_pb.AppendRecord(topic=topic, body=body.payload)],
            force_flush,
        )

    async def write(self, topic: str, values: Sequence[Any]) -> int:
        """Write the values as Records now, after the ones published so far. Returns the last
        Record's offset.
        """
        if self._closed:
            raise _errors.StreamClosedError(
                "The writer is closed", reason="stream_closed"
            )
        await self._settle()
        writer_id = await self.open()
        bodies = await self._converter.encode(list(values))
        return await self._backend.write_records(
            writer_id,
            [stream_pb.AppendRecord(topic=topic, body=body) for body in bodies],
        )

    async def settle(self) -> None:
        """Wait for the publishes that still open the writer or run the codec."""
        await self._settle()

    async def _settle(self) -> None:
        pending, self._pending = self._pending, None
        if pending is not None:
            await pending

    async def flush(self) -> int:
        """Wait until every Record published so far is in the Store. Returns the newest
        offset that the writer wrote, or 0.
        """
        await self._settle()
        if self._writer_id is None:
            return 0
        return await self._backend.flush_writer(self._writer_id)

    async def close(self) -> int:
        """Flush and drop the writer. The Stream stays open."""
        if self._closed:
            return 0
        self._closed = True
        error: BaseException | None = None
        try:
            await self._settle()
        except BaseException as err:
            error = err
        # The crate's writer must go even when a publish failed, or its flusher stays.
        last = 0
        if self._writer_id is not None:
            last = await self._backend.close_writer(self._writer_id)
        if error is not None:
            raise error
        return last
