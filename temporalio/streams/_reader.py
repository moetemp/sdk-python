"""Reads of a Stream, for Activities and clients."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Generic, TypeVar

import temporalio.common
import temporalio.converter
from temporalio.streams import _backend, _convert, _errors
from temporalio.streams._items import Closed, End, Record
from temporalio.streams._offset import StreamOffset
from temporalio.streams.system import models

T = TypeVar("T")

Item = Record[Any] | Closed

# The service waits at most 60 s per read, so a longer wait is a loop of reads.
_MAX_WAIT = timedelta(seconds=60)

# An empty page that comes back in less than half of its wait is followed by a pause, so a
# Store that ignores the wait isn't asked again at once. The pause doubles up to the last one.
_FIRST_PAUSE = 0.05
_LAST_PAUSE = 2.0


@dataclasses.dataclass
class Position:
    """Where a read resumes from. The read moves it past each page that it finished."""

    last_offset: bytes | None


class Reader(Generic[T]):
    """The items of one read of a Stream, in order. Iterate it with ``async for``.

    It yields :py:class:`Record` items, and a :py:class:`Closed` item last when the Stream is
    closed. Each item's ``kind`` gives its type. A read of a closed Stream ends after the
    ``Closed`` item.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        items: AsyncGenerator[Item, None],
        position: Position,
        options: _backend.CallOptions | None = None,
    ) -> None:
        """Create a read. Use a Stream's ``read`` instead."""
        self._items = items
        self._position = position
        # A client read's rpc_metadata and rpc_timeout, for each call of each step.
        self._options = options

    @property
    def last_offset(self) -> StreamOffset | None:
        """The last offset that the read covered, of its last item or its last page.

        Pass it as ``offset`` to a later read, which goes on after it. After a Record that
        failed to decode, it's that Record's offset. It's None before the first page of a read
        from ``END``.
        """
        wire = self._position.last_offset
        return StreamOffset._from_wire(wire) if wire is not None else None

    def __aiter__(self) -> Reader[T]:
        """The read is its own iterator."""
        return self

    async def __anext__(self) -> Record[T] | Closed:
        """The next item.

        Raises:
            StreamError: A Record's value failed to decode with the Stream's payload type. The
                read ends, and :py:attr:`last_offset` is that Record's offset.
        """
        with _backend.using(self._options):
            item = await self._items.__anext__()
        # A resume reads after the offset, so the next read starts at this item's offset.
        if item.offset is not None:
            self._position.last_offset = max(
                self._position.last_offset or b"", item.offset._wire
            )
        return item

    async def aclose(self) -> None:
        """End the read early, from the task that reads it. It makes no more calls.

        If another task is waiting in the read for new Records, this raises ``RuntimeError``.
        Cancel that task instead.
        """
        with _backend.using(self._options):
            await self._items.aclose()


async def read(
    service: _backend.StreamServiceCalls,
    converter: temporalio.converter.DataConverter,
    stream_id: str,
    *,
    position: Position,
    payload_type: Any,
    from_offset: bytes | End | None,
    wait_new_records: bool,
) -> AsyncGenerator[Item, None]:
    """Yield the items of a Stream in order, until the Stream is closed and drained.

    With ``wait_new_records`` False, stop at the newest Record. A read of a Stream that nobody
    created fails at once with ``STREAM_ERROR_REASON_NOT_FOUND``.
    """
    if from_offset is End.END:
        # A from_offset is exclusive, so the read starts after the newest Record. With no Record
        # it is empty and the read starts at the beginning, which misses no Record that lands
        # before the first read.
        described = await service.send_describe_stream(
            models.DescribeStreamRequest(ref=models.StreamRef(id=stream_id))
        )
        from_offset = described.last_offset
    request = models.ReadRecordsRequest(
        ref=models.StreamRef(id=stream_id), from_offset=from_offset
    )
    pages = _pages(service, request, wait_new_records)
    try:
        async for page in pages:
            for record in page.records or []:
                try:
                    item = await _item(converter, record, payload_type)
                except Exception as err:
                    # Move last_offset to the Record, so a resume doesn't fail on it again.
                    # The error gives the offset, for a read of that Record.
                    offset = record.offset
                    position.last_offset = max(position.last_offset or b"", offset)
                    raise _errors.StreamError(
                        f"Can't decode the Record at the offset {offset.hex()}: {err}"
                    ) from err
                yield item
            if page.last_offset is not None:
                # A resume starts after the page's last offset.
                position.last_offset = max(
                    position.last_offset or b"", page.last_offset
                )
            if page.done:
                yield Closed(
                    offset=StreamOffset._from_wire(page.last_offset)
                    if page.last_offset is not None
                    else None
                )
    finally:
        await pages.aclose()


async def _pages(
    service: _backend.StreamServiceCalls,
    request: models.ReadRecordsRequest,
    wait_new_records: bool,
) -> AsyncGenerator[models.ReadRecordsResponse, None]:
    # A read that waits long-polls each page. A read with no wait reads pages until a page has
    # no Record and doesn't advance.
    while True:
        if wait_new_records:
            page = await _read_until_records(
                service, request, _backend.longest_wait(_MAX_WAIT)
            )
        else:
            page = await _read_page(service, request)
        yield page
        if page.done:
            return
        if not wait_new_records and (
            page.last_offset is None
            # An empty page that didn't advance is at the newest Record.
            or (not page.records and page.last_offset <= (request.from_offset or b""))
        ):
            return
        request = _next_request(request, page)


def _next_request(
    request: models.ReadRecordsRequest, page: models.ReadRecordsResponse[Any]
) -> models.ReadRecordsRequest:
    if page.last_offset is not None:
        # A from_offset is exclusive, so the read starts after the Record at that offset.
        return dataclasses.replace(request, from_offset=page.last_offset)
    return request


async def _read_page(
    service: _backend.StreamServiceCalls,
    request: models.ReadRecordsRequest,
) -> models.ReadRecordsResponse:
    # The bodies stay RawValue, so the read decodes each one with the Stream's payload type.
    return await service.send_read_records(request, temporalio.common.RawValue)


async def _read_until_records(
    service: _backend.StreamServiceCalls,
    request: models.ReadRecordsRequest,
    deadline: timedelta,
) -> models.ReadRecordsResponse:
    # Each read waits for what is left of the deadline, and an empty page moves the next read to
    # after its last offset. At the deadline the last page comes back, so the caller keeps its
    # place.
    loop = asyncio.get_running_loop()
    end = loop.time() + deadline.total_seconds()
    pause = _FIRST_PAUSE
    while True:
        wait = min(max(end - loop.time(), 0.001), _MAX_WAIT.total_seconds())
        request = dataclasses.replace(request, wait=timedelta(seconds=wait))
        asked = loop.time()
        page = await _read_page(service, request)
        if page.records or page.done:
            return page
        request = _next_request(request, page)
        left = end - loop.time()
        if left <= 0:
            return page
        if loop.time() - asked < wait / 2:
            await asyncio.sleep(min(pause, left))
            pause = min(pause * 2, _LAST_PAUSE)
        else:
            pause = _FIRST_PAUSE


async def _item(
    converter: temporalio.converter.DataConverter,
    record: models.StreamRecord[temporalio.common.RawValue],
    payload_type: Any,
) -> Record[Any]:
    value = _convert.decode(converter, record.body, payload_type)
    return Record(
        data=value,
        offset=StreamOffset._from_wire(record.offset),
        producer=record.producer,
    )
