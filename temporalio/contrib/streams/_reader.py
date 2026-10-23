"""Reads of a Stream, for Activities and clients."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import timedelta
from typing import Any, Generic, TypeVar

import temporalio.converter
from temporalio.api.streamservice.v1 import ReadStart, StreamRecordKind
from temporalio.contrib.streams import _convert, _errors
from temporalio.contrib.streams._items import (
    Closed,
    End,
    Record,
    Superseded,
    producer_key,
    withdraws,
)
from temporalio.contrib.streams.system import client as service_client
from temporalio.contrib.streams.system import models

T = TypeVar("T")

Item = Record[Any] | Superseded | Closed

# The service waits at most 60 s per read, so a longer wait is a loop of reads.
_MAX_WAIT = timedelta(seconds=60)


@dataclasses.dataclass
class Position:
    """Where a read goes on from. The read moves it past each page it finished."""

    next_offset: int | None


class Subscription(Generic[T]):
    """The items of one read of a Stream, in order. Use it in ``async for``.

    It gives :py:class:`Record` items, :py:class:`Superseded` items that withdraw earlier
    Records, and a :py:class:`Closed` item last when the Stream is closed.

    .. warning::
        This API is experimental.
    """

    def __init__(self, items: AsyncGenerator[Item, None], position: Position) -> None:
        """Create a read. Use a handle's ``subscribe`` instead."""
        self._items = items
        self._position = position
        # The Producer and the offsets of each Record this read gave, with no values, so a
        # Superseded item can give the offsets it withdraws.
        self._shown: dict[tuple[Any, ...], tuple[models.Producer, list[int]]] = {}

    @property
    def next_offset(self) -> int | None:
        """The offset to resume from, after the last item given or the last page read.

        Pass it as ``from_offset`` to a later read. It is None only before the first page of a
        read from ``END``.
        """
        return self._position.next_offset

    def __aiter__(self) -> Subscription[T]:
        """The read is its own iterator."""
        return self

    async def __anext__(self) -> Record[T] | Superseded | Closed:
        """The next item."""
        item = await self._items.__anext__()
        self._position.next_offset = max(
            self._position.next_offset or 0, item.offset + 1
        )
        if isinstance(item, Record) and item.producer is not None:
            key = producer_key(item.producer)
            self._shown.setdefault(key, (item.producer, []))[1].append(item.offset)
        elif isinstance(item, Superseded):
            item = dataclasses.replace(
                item, withdrawn_offsets=self._withdraw(item.supersession)
            )
        return item

    def _withdraw(self, supersession: models.Supersession) -> tuple[int, ...]:
        offsets: list[int] = []
        for key, (producer, shown) in list(self._shown.items()):
            if withdraws(supersession, producer):
                offsets.extend(shown)
                # A withdrawn Record stays withdrawn, so the read forgets it.
                del self._shown[key]
        return tuple(sorted(offsets))

    async def aclose(self) -> None:
        """End the read early. It makes no more calls."""
        await self._items.aclose()


async def read(
    service: service_client.StreamServiceClient,
    converter: temporalio.converter.DataConverter,
    stream_id: str,
    *,
    position: Position,
    topics: Sequence[str] | None,
    topic_prefixes: Sequence[str] = (),
    types: Mapping[str, Any],
    default_type: Any = None,
    from_offset: int | End | None,
    wait_new_records: bool,
    create_wait: timedelta | None,
) -> AsyncGenerator[Item, None]:
    """Give the items of a Stream in order, until a close, or the newest Record when
    ``wait_new_records`` is False.
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
            # END is only the first page's start. Each answer's next offset pins later pages.
            from_offset=from_offset if isinstance(from_offset, int) else None,
            start=ReadStart.READ_START_END
            if from_offset is End.END
            else ReadStart.READ_START_BEGINNING
            if from_offset is None
            else None,
            topics=list(topics or []),
            topic_prefixes=list(topic_prefixes),
            create_wait=min(create_left, _MAX_WAIT) if create_left else None,
        )

    while True:
        pages = _pages(service, request(), wait_new_records)
        try:
            async for page in pages:
                for record in page.records or []:
                    item = await _item(converter, record, types, default_type)
                    if item is None:
                        continue
                    yield item
                    if isinstance(item, Closed):
                        return
                if page.next_offset is not None:
                    # A page ends past the Records that a Topic filter left out.
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
    service: service_client.StreamServiceClient,
    request: models.ReadRecordsRequest,
    wait_new_records: bool,
) -> AsyncGenerator[models.ReadRecordsResponse, None]:
    # A read that waits long-polls each page. A read with no wait reads pages until one has no
    # Record.
    while True:
        if wait_new_records:
            page = await service.read_records_until_records(request, deadline=_MAX_WAIT)
        else:
            page = await service.read_records(request)
        yield page
        if page.done:
            return
        if not wait_new_records and (not page.records or page.next_offset is None):
            return
        if page.next_offset is not None:
            request = dataclasses.replace(request, from_offset=page.next_offset)


async def _item(
    converter: temporalio.converter.DataConverter,
    record: models.StreamRecord,
    types: Mapping[str, Any],
    default_type: Any,
) -> Item | None:
    offset = record.offset or 0
    if record.kind == StreamRecordKind.STREAM_RECORD_KIND_DATA:
        topic = record.topic or ""
        type_hint = types.get(topic, default_type)
        data = _convert.decode(converter, record.body, type_hint)
        return Record(topic=topic, data=data, offset=offset, producer=record.producer)
    if record.kind == StreamRecordKind.STREAM_RECORD_KIND_SUPERSEDED:
        if record.supersession is None:
            return None
        return Superseded(offset=offset, supersession=record.supersession)
    if record.kind == StreamRecordKind.STREAM_RECORD_KIND_CLOSE:
        result = _convert.decode(converter, record.close_result, None)
        return Closed(offset=offset, result=result, failure=record.close_failure)
    return None
