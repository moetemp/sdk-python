from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, cast

import pytest
from typing_extensions import override

import temporalio.common
import temporalio.converter
from temporalio.streams import StreamNotFoundError, _backend, _reader
from temporalio.streams._items import Record, Superseded
from temporalio.streams.system import models


class _Pages(_backend.StreamServiceCalls):
    """Answers each read with the next page, and keeps the requests."""

    def __init__(self, *pages: models.ReadRecordsResponse[Any]) -> None:
        super().__init__(cast(Any, None))
        self.pages = list(pages)
        self.requests: list[models.ReadRecordsRequest] = []

    @override
    async def send_read_records(
        self, request: models.ReadRecordsRequest, body_type: type[Any]
    ) -> models.ReadRecordsResponse[Any]:
        assert body_type is temporalio.common.RawValue
        self.requests.append(request)
        return self.pages.pop(0)


async def test_a_waiting_read_carries_the_offset_until_records_come():
    record = models.StreamRecord(
        item=models.StreamItemData(models.StreamData(body=None)), offset=9
    )
    service = _Pages(
        models.ReadRecordsResponse(records=[], next_offset=8),
        models.ReadRecordsResponse(records=[record], next_offset=10),
    )
    page = await _reader._read_until_records(
        service,
        models.ReadRecordsRequest(ref=models.StreamRef(id="wf:run"), from_offset=1),
        timedelta(seconds=5),
    )
    assert page.records and page.records[0].offset == 9
    first, second = service.requests
    assert (first.from_offset, second.from_offset) == (1, 8)
    assert second.wait is not None
    assert timedelta() < second.wait <= timedelta(seconds=60)


def _activity(id: str, attempt: int) -> models.Producer:
    return models.Producer(
        id=id,
        epoch=models.ProducerEpoch(
            kind=models.ProducerEpochKindActivity(models.ActivityEpoch(attempt=attempt))
        ),
    )


async def test_superseded_lists_each_withdrawn_offset_once():
    failed = _activity("act:run:1:1", 1)
    other = _activity("act:run:2:2", 1)
    # A batch has consecutive offsets, and other Producers' Records come between batches.
    written = [(10, failed), (11, failed), (12, failed), (13, other), (20, failed)]
    facts = models.StreamSuperseded(
        writer=models.StreamWriter(producer=_activity(failed.id, 2), failed_tasks=[])
    )

    async def items() -> AsyncGenerator[_reader.Item, None]:
        for offset, producer in written:
            yield Record(data=None, offset=offset, producer=producer)
        yield Superseded(offset=30, facts=facts)
        yield Superseded(offset=31, facts=facts)

    read: _reader.Subscription[None] = _reader.Subscription(
        items(), _reader.Position(0)
    )
    superseded = [item async for item in read if isinstance(item, Superseded)]
    assert [item.withdrawn_offsets for item in superseded] == [(10, 11, 12, 20), ()]


class _Missing(_backend.StreamServiceCalls):
    """Answers each read at once with NOT_FOUND, as a Store that ignores create_wait."""

    def __init__(self) -> None:
        super().__init__(cast(Any, None))
        self.calls = 0

    @override
    async def send_read_records(
        self, request: models.ReadRecordsRequest, body_type: type[Any]
    ) -> models.ReadRecordsResponse[Any]:
        self.calls += 1
        raise StreamNotFoundError("nobody created it")


async def test_a_wait_for_the_create_pauses_between_reads():
    service = _Missing()
    items = _reader.read(
        service,
        temporalio.converter.DataConverter.default,
        "missing",
        position=_reader.Position(0),
        payload_type=None,
        from_offset=None,
        wait_new_records=False,
        create_wait=timedelta(milliseconds=500),
    )
    with pytest.raises(StreamNotFoundError):
        await anext(items)
    # The pauses double from 50 ms, so the half second has a few reads, not a hot loop.
    assert 2 <= service.calls <= 8
