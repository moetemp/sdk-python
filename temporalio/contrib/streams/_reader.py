"""Reads of a Stream, for Activities and clients."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import timedelta
from typing import Any

import temporalio.converter
from temporalio.api.streamservice.v1 import ReadStart, StreamRecordKind
from temporalio.contrib.streams import _convert, _errors
from temporalio.contrib.streams._items import Closed, End, Record, Superseded
from temporalio.contrib.streams.system import client as service_client
from temporalio.contrib.streams.system import models

Item = Record[Any] | Superseded | Closed

# The service waits at most 60 s per read, so a longer wait is a loop of reads.
_MAX_WAIT = timedelta(seconds=60)


async def read(
    service: service_client.StreamServiceClient,
    converter: temporalio.converter.DataConverter,
    stream: models.StreamRef,
    *,
    topics: Sequence[str] | None,
    topic_prefixes: Sequence[str] = (),
    types: Mapping[str, Any],
    from_offset: int | End | None,
    wait_new_records: bool,
    create_wait: timedelta | None,
) -> AsyncGenerator[Item, None]:
    """Give the items of a Stream in order, until a close, or the newest Record when
    ``wait_new_records`` is False.
    """
    loop = asyncio.get_running_loop()
    create_until = loop.time() + create_wait.total_seconds() if create_wait else None
    offset = from_offset

    def request() -> models.ReadRecordsRequest:
        create_left = (
            timedelta(seconds=max(create_until - loop.time(), 0))
            if create_until is not None
            else None
        )
        return models.ReadRecordsRequest(
            stream=stream,
            # END is only the first page's start. Each answer's next offset pins later pages.
            from_offset=offset if isinstance(offset, int) else None,
            start=ReadStart.READ_START_END
            if offset is End.END
            else ReadStart.READ_START_BEGINNING
            if offset is None
            else None,
            topics=list(topics or []),
            topic_prefixes=list(topic_prefixes),
            create_wait=min(create_left, _MAX_WAIT) if create_left else None,
        )

    while True:
        # The generated iterator pages the long poll. A read with no wait reads pages until
        # one has no Record.
        records = (
            service.iter_read_records(request())
            if wait_new_records
            else _pages_until_empty(service, request())
        )
        try:
            while True:
                try:
                    record = await records.__anext__()
                except StopAsyncIteration:
                    return
                item = await _item(converter, record, types)
                if item is None:
                    continue
                yield item
                if isinstance(item, Closed):
                    return
        except _errors.StreamNotFoundError:
            if create_until is None or loop.time() >= create_until:
                raise
        finally:
            if isinstance(records, AsyncGenerator):
                await records.aclose()


async def _pages_until_empty(
    service: service_client.StreamServiceClient, request: models.ReadRecordsRequest
) -> AsyncGenerator[models.StreamRecord, None]:
    while True:
        answer = await service.read_records(request)
        for record in answer.records or []:
            yield record
        if answer.done or not answer.records or answer.next_offset is None:
            return
        request = dataclasses.replace(request, from_offset=answer.next_offset)


async def _item(
    converter: temporalio.converter.DataConverter,
    record: models.StreamRecord,
    types: Mapping[str, Any],
) -> Item | None:
    offset = record.offset or 0
    if record.kind == StreamRecordKind.STREAM_RECORD_KIND_DATA:
        topic = record.topic or ""
        type_hint = types.get(topic)
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
