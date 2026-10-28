from collections.abc import Awaitable
from datetime import timedelta
from typing import Any

from typing_extensions import override

from temporalio.common._streams.system import client, models


def _data(offset: int) -> models.StreamRecord:
    return models.StreamRecord(
        item=models.StreamItemData(models.StreamData(body=None)), offset=offset
    )


class FakeNexusClient:
    def __init__(self, *answers: models.ReadRecordsResponse) -> None:
        self.requests: list[models.ReadRecordsRequest] = []
        self.answers = list(answers)

    async def start_operation(
        self, operation: str, input: Any, *, output_type: type[Any] | None = None
    ) -> Awaitable[Any]:
        assert operation == "ReadRecords"
        assert output_type is models.ReadRecordsResponse
        self.requests.append(input)
        answer = self.answers.pop(0)

        async def done() -> Any:
            return answer

        return done()


class FakeService(client.StreamServiceClientExtension):
    def __init__(self, nexus_client: FakeNexusClient) -> None:
        self.nexus_client = nexus_client

    @override
    def _create_nexus_client(
        self, *, service: str, endpoint: str | None
    ) -> FakeNexusClient:
        assert service == "temporal.api.streamservice.v1.StreamService"
        assert endpoint == "__temporal_system"
        return self.nexus_client


async def test_a_poll_carries_the_next_offset_until_the_close():
    nexus_client = FakeNexusClient(
        models.ReadRecordsResponse(next_offset=8),
        models.ReadRecordsResponse(
            records=[_data(9)],
            next_offset=10,
        ),
        models.ReadRecordsResponse(next_offset=10, done=True),
    )
    stream = client.Stream(
        _client=FakeService(nexus_client),
        stream=models.StreamRef(id="wf:run"),
        created=None,
        status=None,
    )
    pages = [
        page
        async for page in stream.iter_read(from_offset=1, wait=timedelta(seconds=5))
    ]
    assert [page.next_offset for page in pages] == [8, 10, 10]
    assert [request.from_offset for request in nexus_client.requests] == [1, 8, 10]
    assert all(request.wait for request in nexus_client.requests)


async def test_a_read_with_no_wait_ends_at_an_empty_page_that_didnt_move():
    nexus_client = FakeNexusClient(
        models.ReadRecordsResponse(
            records=[_data(4)],
            next_offset=5,
        ),
        models.ReadRecordsResponse(next_offset=5),
    )
    pages = [
        page
        async for page in FakeService(nexus_client).iter_read_records(
            stream=models.StreamRef(id="wf:run"),
            start=models.StreamReadStart.StreamReadStartBeginning,
        )
    ]
    assert len(pages) == 2
    first, second = nexus_client.requests
    assert first.start == models.StreamReadStart.StreamReadStartBeginning
    # The second page starts where the first ended, so the start no longer applies.
    assert (second.from_offset, second.start) == (5, None)
