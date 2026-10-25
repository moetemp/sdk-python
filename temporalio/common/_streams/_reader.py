"""Reads of a Stream, for Activities and clients."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import timedelta
from typing import Any, Generic, TypeVar

import temporalio.converter
from temporalio.api.streamservice.v1 import ProducerKind, ReadStart, StreamRecordKind
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
        self._shown: dict[tuple[Any, ...], tuple[models.Producer, list[int]]] = {}

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
            StreamError: A Record's value failed to decode with its Topic's type. The read
                ends, and :py:attr:`next_offset` is past that Record.
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
            and item.producer.kind != ProducerKind.PRODUCER_KIND_CLIENT
        ):
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
        """End the read early, from the task that reads it. It makes no more calls.

        If another task is waiting in the read for new Records, this raises ``RuntimeError``.
        Cancel that task instead.
        """
        with _backend.using(self._options):
            await self._items.aclose()


async def read(
    service: service_client.StreamServiceClient,
    converter: temporalio.converter.DataConverter,
    stream: models.StreamRef,
    *,
    position: Position,
    topics: Sequence[str] | None,
    topic_prefixes: Sequence[str] = (),
    types: Mapping[str, Any],
    default_type: Any = None,
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
            stream=stream,
            # END applies only to the first page. Later pages start at the last page's next offset.
            from_offset=from_offset if isinstance(from_offset, int) else None,
            start=ReadStart.READ_START_END
            if from_offset is End.END
            else ReadStart.READ_START_BEGINNING
            if from_offset is None and not last
            else None,
            last=last,
            topics=list(topics or []),
            topic_prefixes=list(topic_prefixes),
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
                        item = await _item(converter, record, types, default_type)
                    except Exception as err:
                        # Move next_offset past the Record, so a resume doesn't fail on it again.
                        # The error gives the offset, for a read of that Record.
                        offset = record.offset or 0
                        position.next_offset = max(
                            position.next_offset or 0, offset + 1
                        )
                        raise _errors.StreamError(
                            f"Can't decode the Record at the offset {offset} of the Topic "
                            f"{record.topic or ''}: {err}"
                        ) from err
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
    # A read that waits long-polls each page. A read with no wait reads pages until a page has
    # no Record and doesn't advance.
    if request.last:
        # The newest Records come in one page that never waits. A waiting read then continues
        # after them.
        page = await service.read_records(request)
        yield page
        if page.done or not wait_new_records or page.next_offset is None:
            return
        request = dataclasses.replace(request, last=None, from_offset=page.next_offset)
    while True:
        if wait_new_records:
            page = await service.read_records_until_records(
                request, deadline=_backend.longest_wait(_MAX_WAIT)
            )
        else:
            page = await service.read_records(request)
        yield page
        if page.done:
            return
        if not wait_new_records and (
            page.next_offset is None
            # A Store scans a limited number of entries per call. So an empty page that advanced
            # stopped in Records that a Topic filter skipped, not at the newest Record.
            or (not page.records and page.next_offset <= (request.from_offset or 0))
        ):
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
        failure = record.close_failure
        # An older close Record has no status, so its failure decides the status.
        status = record.close_status or (
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
    return None
