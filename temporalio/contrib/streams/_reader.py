"""Reads of a Stream, for Activities and clients."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from datetime import timedelta
from typing import Any

import temporalio.converter
from temporalio.api.streamservice.v1 import ReadStart, StreamRecordKind
from temporalio.contrib.streams import _convert, _errors
from temporalio.contrib.streams._items import Closed, Record, Superseded
from temporalio.contrib.streams.system import client as service_client
from temporalio.contrib.streams.system import models

Item = Record[Any] | Superseded | Closed

# The service waits at most 60 s per read, so a longer wait is a loop of reads.
_MAX_WAIT = timedelta(seconds=60)
# How often a reader that ends with the run checks the run, while it waits for the create
# or for new Records.
_RUN_CHECK = timedelta(seconds=2)
# How long one loop of the generated long poll may run before the facade looks again.
_POLL_DEADLINE = timedelta(minutes=5)


async def read(
    service: service_client.StreamServiceClient,
    converter: temporalio.converter.DataConverter,
    stream_id: str,
    *,
    topics: Sequence[str] | None,
    topic_prefixes: Sequence[str] = (),
    types: Mapping[str, Any],
    default_type: Any = None,
    from_offset: int | None,
    wait_new_records: bool,
    wait_for_create: timedelta | None,
    run_ended: Callable[[], Awaitable[bool]] | None = None,
) -> AsyncGenerator[Item, None]:
    """Give the items of a Stream in order, until a close, or the newest Record when
    ``wait_new_records`` is False.
    """
    loop = asyncio.get_running_loop()
    create_until = (
        loop.time() + wait_for_create.total_seconds() if wait_for_create else None
    )
    offset = from_offset
    names = list(topics or [])
    # A reader that ends with the run waits for the create in short steps, so it sees the
    # run end soon.
    create_step = _RUN_CHECK if run_ended is not None else _MAX_WAIT
    while True:
        create_left = (
            timedelta(seconds=max(create_until - loop.time(), 0))
            if create_until is not None
            else None
        )
        request = models.ReadRecordsRequest(
            stream=models.StreamRef(id=stream_id),
            from_offset=offset,
            start=None if offset is not None else ReadStart.READ_START_BEGINNING,
            topics=names,
            topic_prefixes=list(topic_prefixes),
            create_wait=min(create_left, create_step) if create_left else None,
        )
        try:
            with _convert.raw_bodies(converter.failure_converter):
                if wait_new_records:
                    # A reader that ends with the run looks at the run between short polls.
                    answer = await service.read_records_until_records(
                        request,
                        deadline=_RUN_CHECK
                        if run_ended is not None
                        else _POLL_DEADLINE,
                    )
                else:
                    answer = await service.read_records(request)
        except _errors.StreamNotFoundError:
            if create_until is not None and loop.time() < create_until:
                if run_ended is not None and await run_ended():
                    return
                continue
            raise
        for record in answer.records or []:
            item = await _item(converter, record, types, default_type)
            if item is None:
                continue
            yield item
            if isinstance(item, Closed):
                return
        if answer.next_offset is not None:
            offset = answer.next_offset
        if answer.records:
            continue
        if answer.done or not wait_new_records:
            return
        if run_ended is not None and await run_ended():
            return


async def _item(
    converter: temporalio.converter.DataConverter,
    record: models.StreamRecord,
    types: Mapping[str, Any],
    default_type: Any,
) -> Item | None:
    offset = record.offset or 0
    if record.kind == StreamRecordKind.STREAM_RECORD_KIND_DATA:
        topic = record.topic or ""
        type_hint = types.get(topic) or default_type
        data = await _convert.decode(converter, record.body, type_hint)
        return Record(topic=topic, data=data, offset=offset, producer=record.producer)
    if record.kind == StreamRecordKind.STREAM_RECORD_KIND_SUPERSEDED:
        if record.supersession is None:
            return None
        return Superseded(offset=offset, supersession=record.supersession)
    if record.kind == StreamRecordKind.STREAM_RECORD_KIND_CLOSE:
        result = await _convert.decode(converter, record.close_result, None)
        return Closed(offset=offset, result=result, failure=record.close_failure)
    return None
