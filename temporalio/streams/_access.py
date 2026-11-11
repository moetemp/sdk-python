"""Stream calls outside Workflow code, which Activity and client Streams share."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable, Sequence
from dataclasses import dataclass
from typing import Any

import temporalio.converter
from temporalio.streams import _convert, _reader
from temporalio.streams._backend import BackendCalls, CallOptions
from temporalio.streams._items import End
from temporalio.streams._offset import StreamOffset
from temporalio.streams._reader import Item, Position, Reader
from temporalio.streams._writer import RecordWriter
from temporalio.streams.system import models


@dataclass
class Access:
    """The calls of one Stream, bound to a backend."""

    backend: BackendCalls
    converter: temporalio.converter.DataConverter
    namespace: str
    stream_id: str
    writer: Callable[[], RecordWriter]

    def append(self, values: Sequence[Any]) -> asyncio.Future[StreamOffset]:
        """Start the write of the values as Records, in order, and return its future.

        The future gives the offset of the last Record.
        """
        if not values:
            raise TypeError("append takes one or more values")
        return self.writer().write(values)

    async def items(
        self, read: ReadSpec, position: Position
    ) -> AsyncGenerator[Item, None]:
        """Read the Stream's items in order, and move ``position`` past each page."""
        items = _reader.read(
            self.backend.service(self.namespace, _convert.raw(self.converter)),
            self.converter,
            self.stream_id,
            position=position,
            payload_type=read.payload_type,
            from_offset=read.offset,
            wait_new_records=read.wait_new_records,
        )
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def get_offset(self) -> bytes:
        """The offset to resume from, the newest Record's offset, or empty if it has no Record.

        A resume reads after the offset, so an empty offset reads from the start.
        """
        service = self.backend.service(self.namespace)
        response = await service.send_describe_stream(
            models.DescribeStreamRequest(ref=models.StreamRef(id=self.stream_id))
        )
        return response.last_offset or b""

    async def close(self) -> None:
        """Write this Producer's Records, then close the Stream."""
        writer = self.writer()
        await writer.close()
        producer = models._ProducerTransferTypeConverter().from_transfer_type(
            writer.producer, models.Producer
        )
        service = self.backend.service(self.namespace, self.converter)
        # The generated close takes its writer from a source, so the Stream sends the request
        # with its own.
        await service.send_close_stream(
            models.CloseStreamRequest(
                ref=models.StreamRef(id=self.stream_id),
                producer=producer,
            )
        )


@dataclass
class ReadSpec:
    """What one read asks for."""

    payload_type: Any
    offset: bytes | End | None
    wait_new_records: bool


def read_spec(
    payload_type: Any,
    *,
    offset: bytes | End | None,
    wait_new_records: bool,
) -> ReadSpec:
    """The options of a read, and the type that it decodes each value with."""
    return ReadSpec(
        payload_type=payload_type,
        offset=offset,
        wait_new_records=wait_new_records,
    )


def reader(
    items: Callable[[Position], AsyncGenerator[Item, None]],
    read: ReadSpec,
    options: CallOptions | None = None,
) -> Reader[Any]:
    """A read over the items that ``items`` gives."""
    # A read from END has no resume offset until its first page.
    if read.offset is End.END:
        position = Position(None)
    else:
        position = Position(read.offset or b"")
    return Reader(items(position), position, options)
