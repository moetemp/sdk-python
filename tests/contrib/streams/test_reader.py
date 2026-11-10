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
        item=models.StreamItemData(models.StreamData(body=None)), offset=_off(9)
    )
    service = _Pages(
        models.ReadRecordsResponse(records=[], next_offset=_off(8)),
        models.ReadRecordsResponse(records=[record], next_offset=_off(10)),
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
        raise StreamError(
            "nobody created it", reason=StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
        )


async def test_a_wait_for_the_create_pauses_between_reads():
    service = _Missing()
    items = _reader.read(
        service,
        temporalio.converter.DataConverter.default,
        "missing",
        position=_reader.Position(None),
        payload_type=None,
        from_offset=None,
        wait_new_records=False,
        create_wait=timedelta(milliseconds=500),
    )
    with pytest.raises(StreamError) as missing:
        await anext(items)
    assert missing.value.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
    # The pauses double from 50 ms, so the half second has a few reads, not a hot loop.
    assert 2 <= service.calls <= 8
