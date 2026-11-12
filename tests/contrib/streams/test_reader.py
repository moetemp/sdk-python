from datetime import timedelta
from typing import Any, cast

import pytest
from typing_extensions import override

import temporalio.common
import temporalio.converter
from temporalio.streams import (
    Closed,
    End,
    Record,
    StreamError,
    StreamErrorReason,
    StreamOffset,
    _backend,
    _reader,
)
from temporalio.streams.system import models


def _off(n: int) -> bytes:
    return n.to_bytes(8, "big")


def _record(offset: int) -> models.StreamRecord[Any]:
    return models.StreamRecord(body=None, offset=_off(offset))


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
    record = _record(9)
    service = _Pages(
        models.ReadRecordsResponse(records=[], last_offset=_off(8), done=False),
        models.ReadRecordsResponse(records=[record], last_offset=_off(10), done=False),
    )
    page = await _reader._read_until_records(
        service,
        models.ReadRecordsRequest(
            ref=models.StreamRef(id="wf:run"), from_offset=_off(1)
        ),
        timedelta(seconds=5),
    )
    assert page.records and page.records[0].offset == _off(9)
    first, second = service.requests
    assert (first.from_offset, second.from_offset) == (_off(1), _off(8))
    assert second.wait is not None
    assert timedelta() < second.wait <= timedelta(seconds=60)


class _RecordLandsAfterDescribe(_backend.StreamServiceCalls):
    """A Stream with no Record, which gets one right after the describe answers."""

    def __init__(self) -> None:
        super().__init__(cast(Any, None))
        self.records: list[models.StreamRecord[Any]] = []

    @override
    async def send_describe_stream(
        self, request: models.DescribeStreamRequest
    ) -> models.DescribeStreamResponse:
        answer = models.DescribeStreamResponse(
            last_offset=self.records[-1].offset if self.records else None
        )
        if not self.records:
            self.records.append(_record(1))
        return answer

    @override
    async def send_read_records(
        self, request: models.ReadRecordsRequest, body_type: type[Any]
    ) -> models.ReadRecordsResponse[Any]:
        after = request.from_offset or b""
        found = [r for r in self.records if r.offset > after]
        # An empty page gives the request's from_offset, which is empty with no offset.
        return models.ReadRecordsResponse(
            records=found,
            last_offset=found[-1].offset if found else after or None,
            done=False,
        )


async def test_a_read_from_the_end_of_an_empty_stream_misses_no_record():
    items = _reader.read(
        _RecordLandsAfterDescribe(),
        temporalio.converter.DataConverter.default,
        "wf:run",
        position=_reader.Position(None),
        payload_type=None,
        from_offset=End.END,
        wait_new_records=False,
    )
    assert [item.offset async for item in items] == [StreamOffset(_off(1))]


class _Described(_backend.StreamServiceCalls):
    """Answers a describe with an offset, and a read with an empty page."""

    def __init__(self) -> None:
        super().__init__(cast(Any, None))
        self.requests: list[models.ReadRecordsRequest] = []

    @override
    async def send_describe_stream(
        self, request: models.DescribeStreamRequest
    ) -> models.DescribeStreamResponse:
        return models.DescribeStreamResponse(last_offset=_off(7))

    @override
    async def send_read_records(
        self, request: models.ReadRecordsRequest, body_type: type[Any]
    ) -> models.ReadRecordsResponse[Any]:
        self.requests.append(request)
        return models.ReadRecordsResponse(
            records=[], last_offset=request.from_offset, done=False
        )


async def test_a_read_from_the_end_starts_after_the_described_offset():
    service = _Described()
    items = _reader.read(
        service,
        temporalio.converter.DataConverter.default,
        "wf:run",
        position=_reader.Position(None),
        payload_type=None,
        from_offset=End.END,
        wait_new_records=False,
    )
    assert [item async for item in items] == []
    assert [request.from_offset for request in service.requests] == [_off(7)]


async def test_a_done_page_ends_the_read_with_one_closed_item():
    service = _Pages(
        models.ReadRecordsResponse(
            records=[_record(3), _record(4)], last_offset=_off(4), done=True
        )
    )
    position = _reader.Position(None)
    items = [
        item
        async for item in _reader.read(
            service,
            temporalio.converter.DataConverter.default,
            "wf:run",
            position=position,
            payload_type=None,
            from_offset=None,
            wait_new_records=True,
        )
    ]
    assert [type(item) for item in items] == [Record, Record, Closed]
    assert items[-1] == Closed(offset=StreamOffset(_off(4)))
    assert position.last_offset == _off(4)
    assert len(service.requests) == 1


async def test_a_closed_stream_with_no_record_gives_a_closed_item_with_no_offset():
    service = _Pages(
        models.ReadRecordsResponse(records=[], last_offset=None, done=True)
    )
    items = [
        item
        async for item in _reader.read(
            service,
            temporalio.converter.DataConverter.default,
            "wf:run",
            position=_reader.Position(None),
            payload_type=None,
            from_offset=None,
            wait_new_records=True,
        )
    ]
    assert items == [Closed(offset=None)]


class _Missing(_backend.StreamServiceCalls):
    """Answers each read with NOT_FOUND, as for a Stream that nobody created."""

    def __init__(self) -> None:
        super().__init__(cast(Any, None))
        self.calls = 0

    @override
    async def send_read_records(
        self, request: models.ReadRecordsRequest, body_type: type[Any]
    ) -> models.ReadRecordsResponse[Any]:
        self.calls += 1
        raise StreamError(
            "nobody created it", reason=StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
        )


async def test_a_read_of_a_missing_stream_raises_not_found_at_once():
    service = _Missing()
    items = _reader.read(
        service,
        temporalio.converter.DataConverter.default,
        "missing",
        position=_reader.Position(None),
        payload_type=None,
        from_offset=None,
        wait_new_records=True,
    )
    with pytest.raises(StreamError) as missing:
        await anext(items)
    assert missing.value.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
    assert service.calls == 1
