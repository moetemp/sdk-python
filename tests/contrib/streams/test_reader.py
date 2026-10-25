from collections.abc import AsyncGenerator, Awaitable
from datetime import timedelta
from typing import Any

from typing_extensions import override

from temporalio.common._streams import _reader
from temporalio.common._streams._items import Record, Superseded
from temporalio.common._streams.system import client, models


class _Pages(client.StreamServiceClientExtension):
    """Answers each read with the next page, and keeps the requests."""

    def __init__(self, *pages: models.ReadRecordsResponse) -> None:
        self.pages = list(pages)
        self.requests: list[models.ReadRecordsRequest] = []

    @override
    def _create_nexus_client(self, *, service: str, endpoint: str | None) -> Any:
        return self

    async def start_operation(
        self, operation: str, input: Any, *, output_type: type[Any] | None = None
    ) -> Awaitable[Any]:
        assert operation == "ReadRecords"
        assert output_type is models.ReadRecordsResponse
        self.requests.append(input)
        page = self.pages.pop(0)

        async def done() -> Any:
            return page

        return done()


async def test_a_waiting_read_carries_the_offset_until_records_come():
    record = models.StreamRecord(
        item=models.StreamItemData(models.StreamData(body=None)), offset=9
    )
    service = _Pages(
        models.ReadRecordsResponse(next_offset=8),
        models.ReadRecordsResponse(records=[record], next_offset=10),
    )
    page = await _reader._read_until_records(
        service,
        models.ReadRecordsRequest(stream=models.StreamRef(id="wf:run"), from_offset=1),
        timedelta(seconds=5),
    )
    assert page.records and page.records[0].offset == 9
    first, second = service.requests
    assert (first.from_offset, second.from_offset) == (1, 8)
    assert second.wait is not None
    assert timedelta() < second.wait <= timedelta(seconds=60)


async def test_superseded_lists_each_withdrawn_offset_once():
    kind = models.StreamProducerKind.StreamProducerKindActivity
    failed = models.Producer(
        id="act:run:1:1", kind=kind, epoch=models.ProducerEpoch(attempt=1)
    )
    other = models.Producer(
        id="act:run:2:2", kind=kind, epoch=models.ProducerEpoch(attempt=1)
    )
    # A batch has consecutive offsets, and other Producers' Records come between batches.
    written = [(10, failed), (11, failed), (12, failed), (13, other), (20, failed)]
    supersession = models.Supersession(
        producer_id=failed.id, producer_kind=kind, epoch=models.ProducerEpoch(attempt=2)
    )

    async def items() -> AsyncGenerator[_reader.Item, None]:
        for offset, producer in written:
            yield Record(data=None, offset=offset, producer=producer)
        yield Superseded(offset=30, supersession=supersession)
        yield Superseded(offset=31, supersession=supersession)

    read: _reader.Subscription[None] = _reader.Subscription(
        items(), _reader.Position(0)
    )
    superseded = [item async for item in read if isinstance(item, Superseded)]
    assert [item.withdrawn_offsets for item in superseded] == [(10, 11, 12, 20), ()]
