"""Stream calls outside Workflow code, which Activity and client handles share."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import temporalio.converter
from temporalio.common._streams import _convert, _reader
from temporalio.common._streams._backend import BackendCalls, CallOptions
from temporalio.common._streams._items import End, close_status
from temporalio.common._streams._reader import Item, Position, Subscription
from temporalio.common._streams._writer import StreamWriter
from temporalio.common._streams.system import models


@dataclass
class Access:
    """The calls of one Stream handle, bound to a backend."""

    backend: BackendCalls
    converter: temporalio.converter.DataConverter
    namespace: str
    stream_id: str
    writer: Callable[[], StreamWriter]

    async def publish(self, values: Sequence[Any]) -> int:
        """Write the values as Records now, in order, and return the offset of the last one."""
        if not values:
            raise TypeError("publish takes one or more values")
        return await self.writer().write(values)

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
            last=read.last,
            wait_new_records=read.wait_new_records,
            create_wait=read.create_wait,
        )
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one, or 0 if it has no Record.

        Offsets are sparse and above zero, so ``offset=0`` reads from the start.
        """
        service = self.backend.service(self.namespace)
        response = await (
            await service.describe_stream(stream=models.StreamRef(id=self.stream_id))
        )
        return response.last_offset + 1 if response.last_offset else 0

    async def close(self, result: Any | None, failure: BaseException | None) -> None:
        """Write this Producer's Records, then close the Stream."""
        writer = self.writer()
        await writer.close()
        producer = models._ProducerTransferTypeConverter().from_transfer_type(
            writer.producer, models.Producer
        )
        status = close_status(failure)
        service = self.backend.service(self.namespace, self.converter)
        await (
            await service.close_stream(
                stream=models.StreamRef(id=self.stream_id),
                producer=producer,
                result=result,
                failure=failure,
                status=models.StreamCloseStatus(status) if status is not None else None,
            )
        )


@dataclass
class ReadSpec:
    """What one read asks for."""

    payload_type: Any
    offset: int | End | None
    last: int | None
    wait_new_records: bool
    create_wait: timedelta | None


def read_spec(
    payload_type: Any,
    *,
    offset: int | End | None,
    last: int | None,
    wait_new_records: bool,
    create_wait: timedelta | None,
) -> ReadSpec:
    """The options of a read, and the type that it decodes each value with.

    Raises:
        ValueError: ``last`` is not above zero, or the call also gives ``offset``.
    """
    if last is not None and (last < 1 or offset is not None):
        raise ValueError("last takes a count above zero and no offset")
    return ReadSpec(
        payload_type=payload_type,
        offset=offset,
        last=last,
        wait_new_records=wait_new_records,
        create_wait=create_wait,
    )


def subscription(
    items: Callable[[Position], AsyncGenerator[Item, None]],
    read: ReadSpec,
    options: CallOptions | None = None,
) -> Subscription[Any]:
    """A read over the items that ``items`` gives."""
    # A read from END or of the last Records has no resume offset until its first page.
    if read.last is not None or read.offset is End.END:
        position = Position(None)
    else:
        position = Position(read.offset or 0)
    return Subscription(items(position), position, options)
