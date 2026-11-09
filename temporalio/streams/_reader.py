"""Reads of a Stream, for Activities and clients."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Generic, TypeVar

import temporalio.common
import temporalio.converter
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.streams import _backend, _convert, _errors
from temporalio.streams._close import StreamCloseStatus
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

    next_offset: int | None


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
    def next_offset(self) -> StreamOffset | None:
        """The offset to resume from, after the last item or the last page that the read got.

        Pass it as ``offset`` to a later read. After a Record that failed to decode, it's past
        that Record. It's None before the first page of a read from ``END`` or of a read with
        ``last``.
        """
        wire = self._position.next_offset
        return StreamOffset._from_wire(wire) if wire is not None else None

    def __aiter__(self) -> Reader[T]:
        """The read is its own iterator."""
        return self

    async def __anext__(self) -> Record[T] | Closed:
        """The next item.

        Raises:
            StreamError: A Record's value failed to decode with the Stream's payload type. The
                read ends, and :py:attr:`next_offset` is past that Record.
        """
        with _backend.using(self._options):
            item = await self._items.__anext__()
        offset = item.offset._wire
        self._position.next_offset = max(self._position.next_offset or 0, offset + 1)
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
    from_offset: int | End | None,
    last: int | None = None,
    wait_new_records: bool,
    create_wait: timedelta | None,
) -> AsyncGenerator[Item, None]:
    """Yield the items of a Stream in order, until the Stream is closed and drained.

    With ``wait_new_records`` False, stop at the newest Record. With ``last``, start with the
    newest ``last`` Records.
    """
    loop = asyncio.get_running_loop()
    create_until = loop.time() + create_wait.total_seconds() if create_wait else None
    pause = _FIRST_PAUSE

    def request() -> models.ReadRecordsRequest:
        create_left = (
            timedelta(seconds=max(create_until - loop.time(), 0))
            if create_until is not None
            else None
        )
        return models.ReadRecordsRequest(
            ref=models.StreamRef(id=stream_id),
            # END applies only to the first page. Later pages start at the last page's next offset.
            from_offset=from_offset if isinstance(from_offset, int) else None,
            start=models.StreamReadStart.StreamReadStartEnd
            if from_offset is End.END
            else models.StreamReadStart.StreamReadStartBeginning
            if from_offset is None and not last
            else None,
            last=last,
            create_wait=min(create_left, _backend.longest_wait(_MAX_WAIT))
            if create_left
            else None,
        )

    while True:
        pages = _pages(service, request(), wait_new_records)
        try:
            async for page in pages:
                for record in page.records or []:
                    try:
                        item = await _item(converter, record, payload_type)
                        if item is None:
                            continue
                    except Exception as err:
                        # Move next_offset past the Record, so a resume doesn't fail on it again.
                        # The error gives the offset, for a read of that Record.
                        offset = record.offset or 0
                        position.next_offset = max(
                            position.next_offset or 0, offset + 1
                        )
                        raise _errors.StreamError(
                            f"Can't decode the Record at the offset {offset}: {err}"
                        ) from err
                    yield item
                if page.next_offset is not None:
                    # A resume starts where the page ended.
                    position.next_offset = max(
                        position.next_offset or 0, page.next_offset
                    )
            return
        except _errors.StreamError as err:
            if not _errors.has_reason(
                err, StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
            ):
                raise
            left = create_until - loop.time() if create_until is not None else 0
            if left <= 0:
                raise
            # A Store that answers at once, with no wait for the create, isn't asked again at
            # once.
            await asyncio.sleep(min(pause, left))
            pause = min(pause * 2, _LAST_PAUSE)
        finally:
            await pages.aclose()


async def _pages(
    service: _backend.StreamServiceCalls,
    request: models.ReadRecordsRequest,
    wait_new_records: bool,
) -> AsyncGenerator[models.ReadRecordsResponse, None]:
    # A read that waits long-polls each page. A read with no wait reads pages until a page has
    # no Record and doesn't advance.
    if request.last:
        # The newest Records come in one page that never waits. A waiting read then continues
        # after them.
        page = await _read_page(service, request)
        yield page
        if page.done or not wait_new_records or page.next_offset is None:
            return
        request = dataclasses.replace(request, last=None, from_offset=page.next_offset)
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
            page.next_offset is None
            # An empty page that didn't advance is at the newest Record.
            or (not page.records and page.next_offset <= (request.from_offset or 0))
        ):
            return
        if page.next_offset is not None:
            request = dataclasses.replace(request, from_offset=page.next_offset)


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
    # its next offset. At the deadline the last page comes back, so the caller keeps its place.
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
        if page.next_offset is not None:
            request = dataclasses.replace(request, from_offset=page.next_offset)
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
) -> Item | None:
    offset = record.offset or 0
    match record.item:
        case None:
            # A newer backend's item kind that this SDK doesn't know is empty, so the read skips
            # it.
            return None
        case models.StreamItemData(value=data):
            value = _convert.decode(converter, data.body, payload_type)
            return Record(
                data=value,
                offset=StreamOffset._from_wire(offset),
                producer=data.producer,
            )
        case models.StreamItemClose(value=close):
            return Closed(
                offset=StreamOffset._from_wire(offset),
                status=StreamCloseStatus(int(close.status))
                if close.status is not None
                else StreamCloseStatus.UNSPECIFIED,
            )
