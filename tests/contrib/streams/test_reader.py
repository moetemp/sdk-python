from datetime import timedelta
from typing import Any, cast

import pytest
from typing_extensions import override

import temporalio.common
import temporalio.converter
from temporalio.streams import StreamError, StreamErrorReason, _backend, _reader
from temporalio.streams.system import models


def _off(n: int) -> bytes:
    return n.to_bytes(8, "big")


def _record(offset: int) -> models.StreamRecord[Any]:
    return models.StreamRecord(
        item=models.StreamItemData(models.StreamData(body=None, offset=_off(offset)))
    )


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
        models.ReadRecordsResponse(records=[], last_offset=_off(8)),
        models.ReadRecordsResponse(records=[record], last_offset=_off(10)),
    )
    page = await _reader._read_until_records(
        service,
        models.ReadRecordsRequest(
            ref=models.StreamRef(id="wf:run"),
            position=models.ReadPositionFromOffset(_off(1)),
        ),
        timedelta(seconds=5),
    )
    assert page.records and _reader._offset_of(page.records[0]) == _off(9)
    first, second = service.requests
    assert (first.position, second.position) == (
        models.ReadPositionFromOffset(_off(1)),
        models.ReadPositionFromOffset(_off(8)),
    )
    assert second.wait is not None
    assert timedelta() < second.wait <= timedelta(seconds=60)


class _RecordLandsBetweenPolls(_backend.StreamServiceCalls):
    """A Stream with no Record, which gets one right after the first poll answers."""

    def __init__(self) -> None:
        super().__init__(cast(Any, None))
        self.records: list[models.StreamRecord[Any]] = []

    @override
    async def send_read_records(
        self, request: models.ReadRecordsRequest, body_type: type[Any]
    ) -> models.ReadRecordsResponse[Any]:
        match request.position:
            case models.ReadPositionFromOffset(value=offset):
                after = offset
            case models.ReadPositionStart(
                value=models.StreamReadStart.StreamReadStartEnd
            ):
                after = _reader._offset_of(self.records[-1]) if self.records else b""
            case _:
                after = b""
        found = [r for r in self.records if _reader._offset_of(r) > after]
        if not self.records:
            self.records.append(_record(1))
        # An empty page gives the request's from-offset, which is empty with no offset.
        return models.ReadRecordsResponse(
            records=found,
            last_offset=_reader._offset_of(found[-1]) if found else after or None,
        )


async def test_a_waiting_read_from_the_end_of_an_empty_stream_misses_no_record():
    page = await _reader._read_until_records(
        _RecordLandsBetweenPolls(),
        models.ReadRecordsRequest(
            ref=models.StreamRef(id="wf:run"),
            position=models.ReadPositionStart(
                models.StreamReadStart.StreamReadStartEnd
            ),
        ),
        timedelta(seconds=1),
    )
    assert [_reader._offset_of(record) for record in page.records] == [_off(1)]


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
