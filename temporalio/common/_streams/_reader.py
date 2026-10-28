"""Reads of a Stream, for Activities and clients."""

from __future__ import annotations

import array
import asyncio
import dataclasses
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Generic, TypeVar

import temporalio.converter
from temporalio.api.enums.v1 import StreamProducerKind
from temporalio.common._streams import _backend, _convert, _errors
from temporalio.common._streams._items import (
    Closed,
    CloseStatus,
    End,
    Record,
    Superseded,
    producer_key,
    withdraws,
)
from temporalio.common._streams.system import client as service_client
from temporalio.common._streams.system import models

T = TypeVar("T")

Item = Record[Any] | Superseded | Closed

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


class Subscription(Generic[T]):
    """The items of one read of a Stream, in order. Iterate it with ``async for``.

    It yields :py:class:`Record` items, :py:class:`Superseded` items that withdraw earlier
    Records, and a :py:class:`Closed` item last when the Stream is closed.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        items: AsyncGenerator[Item, None],
        position: Position,
        options: _backend.CallOptions | None = None,
    ) -> None:
        """Create a read. Use a handle's ``subscribe`` instead."""
        self._items = items
        self._position = position
        # A client read's rpc_metadata and rpc_timeout, for each call of each step.
        self._options = options
        # The Producer and the offsets of each Record that this read gave, without values, so a
        # Superseded item can list the offsets that it withdraws.
        self._shown: dict[
            tuple[Any, ...], tuple[models.Producer, array.array[int]]
        ] = {}

    @property
    def next_offset(self) -> int | None:
        """The offset to resume from, after the last item or the last page that the read got.

        Pass it as ``offset`` to a later read. After a ``Superseded`` item, it's that item's own
        offset, since the first Record of its batch has the same offset and comes next. A read
        from there gives the ``Superseded`` item again, which withdraws nothing new. After a
        Record that failed to decode, it's past that Record. It's None before the first page of
        a read from ``END`` or of a read with ``last``.
        """
        return self._position.next_offset

    def __aiter__(self) -> Subscription[T]:
        """The read is its own iterator."""
        return self

    async def __anext__(self) -> Record[T] | Superseded | Closed:
        """The next item.

        Raises:
            StreamError: A Record's value failed to decode with the Stream's payload type. The
                read ends, and :py:attr:`next_offset` is past that Record.
        """
        with _backend.using(self._options):
            item = await self._items.__anext__()
        if isinstance(item, Superseded):
            # The Record of the same entry has the same offset and comes next.
            self._position.next_offset = item.offset
        else:
            self._position.next_offset = max(
                self._position.next_offset or 0, item.offset + 1
            )
        if (
            isinstance(item, Record)
            and item.producer is not None
            # Nothing withdraws a client's Records, so a long read of them keeps no offsets.
            and item.producer.kind != StreamProducerKind.STREAM_PRODUCER_KIND_CLIENT
        ):
            key = producer_key(item.producer)
            _, runs = self._shown.setdefault(key, (item.producer, array.array("Q")))
            # A batch has consecutive offsets, so a run of them costs one first and last pair.
            if runs and runs[-1] + 1 == item.offset:
                runs[-1] = item.offset
            else:
                runs.extend((item.offset, item.offset))
        elif isinstance(item, Superseded):
            item = dataclasses.replace(
                item, withdrawn_offsets=self._withdraw(item.supersession)
            )
        return item

    def _withdraw(self, supersession: models.Supersession) -> tuple[int, ...]:
        offsets: list[int] = []
        for key, (producer, runs) in list(self._shown.items()):
            if withdraws(supersession, producer):
                for first, last in zip(runs[::2], runs[1::2]):
                    offsets.extend(range(first, last + 1))
                # A withdrawn Record stays withdrawn, so the read forgets it.
                del self._shown[key]
        return tuple(sorted(offsets))

    async def aclose(self) -> None:
        """End the read early, from the task that reads it. It makes no more calls.

        If another task is waiting in the read for new Records, this raises ``RuntimeError``.
        Cancel that task instead.
        """
        with _backend.using(self._options):
            await self._items.aclose()


async def read(
    service: service_client.StreamServiceClientExtension,
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
    """Yield the items of a Stream in order, until a close.

    With ``wait_new_records`` False, stop at the newest Record. With ``last``, start with the
    newest ``last`` Records.
    """
    loop = asyncio.get_running_loop()
    create_until = loop.time() + create_wait.total_seconds() if create_wait else None

    def request() -> models.ReadRecordsRequest:
        create_left = (
            timedelta(seconds=max(create_until - loop.time(), 0))
            if create_until is not None
            else None
        )
        return models.ReadRecordsRequest(
            stream=models.StreamRef(id=stream_id),
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
                    if isinstance(item, Closed):
                        return
                if page.next_offset is not None:
                    # A resume starts where the page ended.
                    position.next_offset = max(
                        position.next_offset or 0, page.next_offset
                    )
            return
        except _errors.StreamNotFoundError:
            if create_until is None or loop.time() >= create_until:
                raise
        finally:
            await pages.aclose()


async def _pages(
    service: service_client.StreamServiceClientExtension,
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
    service: service_client.StreamServiceClientExtension,
    request: models.ReadRecordsRequest,
) -> models.ReadRecordsResponse:
    return await (await service._read_records(request))


async def _read_until_records(
    service: service_client.StreamServiceClientExtension,
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
    record: models.StreamRecord,
    payload_type: Any,
) -> Item:
    offset = record.offset or 0
    match record.item:
        case models.StreamItemData(value=data):
            value = _convert.decode(converter, data.body, payload_type)
            return Record(data=value, offset=offset, producer=data.producer)
        case models.StreamItemSuperseded(value=supersession):
            return Superseded(offset=offset, supersession=supersession)
        case models.StreamItemClose(value=close):
            result = _convert.decode(converter, close.result, None)
            failure = close.failure
            # A close with no status takes it from its failure.
            status = close.status or (
                CloseStatus.FAILED if failure is not None else CloseStatus.COMPLETED
            )
            return Closed(
                offset=offset,
                result=result,
                failure=_convert.failure(converter, failure)
                if failure is not None
                else None,
                status=CloseStatus(status),
            )
