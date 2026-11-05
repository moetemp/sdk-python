import asyncio
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, cast, get_origin

import pytest
from typing_extensions import override

import temporalio.client
import temporalio.nexus.system._support as _support
from temporalio.streams.system import client, models, services


def _data(offset: int) -> models.StreamRecord[str]:
    return models.StreamRecord(
        item=models.StreamItemData(models.StreamData(body="token")), offset=offset
    )


class FakeNexusClient:
    def __init__(self, *answers: models.ReadRecordsResponse[str]) -> None:
        self.requests: list[models.ReadRecordsRequest] = []
        self.answers = list(answers)

    async def start_operation(
        self,
        operation: str,
        arg: Any,
        *,
        id: str,
        result_type: type[Any] | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any:
        del rpc_metadata, rpc_timeout
        assert operation == "ReadRecords" and id
        assert get_origin(result_type) is models.ReadRecordsResponse
        self.requests.append(arg)
        answer = self.answers.pop(0)

        class Done:
            async def result(self) -> Any:
                return answer

        return Done()


class FakeService(client.StreamServiceClientExtension):
    def __init__(self, nexus_client: FakeNexusClient) -> None:
        self.nexus_client = nexus_client

    @override
    def create_nexus_client(
        self, service: type[Any] | str, endpoint: str
    ) -> temporalio.client.NexusClient[Any]:
        assert service is services.StreamService
        assert endpoint == "__temporal_system"
        return cast(Any, self.nexus_client)


async def test_a_poll_carries_the_next_offset_until_the_close():
    nexus_client = FakeNexusClient(
        models.ReadRecordsResponse(records=[], next_offset=8),
        models.ReadRecordsResponse(
            records=[_data(9)],
            next_offset=10,
        ),
        models.ReadRecordsResponse(records=[], next_offset=10, done=True),
    )
    stream = FakeService(nexus_client).get_stream[str](id="wf:run")
    items = [
        item
        async for item in stream.iter_read_items(
            from_offset=1, wait=timedelta(seconds=5)
        )
    ]
    assert [item.offset for item in items] == [9]
    assert [request.from_offset for request in nexus_client.requests] == [1, 8, 10]
    assert all(request.wait for request in nexus_client.requests)


async def test_a_read_with_no_wait_ends_at_an_empty_page_that_didnt_move():
    nexus_client = FakeNexusClient(
        models.ReadRecordsResponse(
            records=[_data(4)],
            next_offset=5,
        ),
        models.ReadRecordsResponse(records=[], next_offset=5),
    )
    stream = FakeService(nexus_client).get_stream[str](id="wf:run")
    items = [
        item
        async for item in stream.iter_read_items(
            start=models.StreamReadStart.StreamReadStartBeginning
        )
    ]
    assert [item.offset for item in items] == [4]
    first, second = nexus_client.requests
    assert first.start == models.StreamReadStart.StreamReadStartBeginning
    # The second page starts where the first ended, so the start no longer applies.
    assert (second.from_offset, second.start) == (5, None)


async def test_a_poll_resumes_after_the_last_item_it_gave():
    nexus_client = FakeNexusClient(
        models.ReadRecordsResponse(records=[_data(4), _data(7)], next_offset=8),
    )
    stream = FakeService(nexus_client).get_stream[str](id="wf:run")
    items = stream.iter_read_items(start=models.StreamReadStart.StreamReadStartEnd)
    first = await anext(items)
    assert first.offset == 4
    # A read that resumes here starts past the item, so its start no longer applies.
    assert items.next_request is not None
    assert (items.next_request.from_offset, items.next_request.start) == (5, None)
    # Nor does a count of the newest Records, which would read them again.
    newest = models.ReadRecordsRequest(ref=stream.ref, last=2)
    resumed = _support.resume_read_request(newest, first)
    assert (resumed.from_offset, resumed.last) == (5, None)


async def test_a_poll_resumes_at_a_superseded_item_and_keeps_its_record():
    superseded = models.StreamRecord[str](
        item=models.StreamItemSuperseded(
            models.StreamSuperseded(
                writer=models.StreamWriter(
                    producer=models.Producer(id="act:run:a:k"), failed_tasks=[]
                )
            )
        ),
        offset=7,
    )
    nexus_client = FakeNexusClient(
        models.ReadRecordsResponse(records=[superseded, _data(7)], next_offset=8),
    )
    stream = FakeService(nexus_client).get_stream[str](id="wf:run")
    items = stream.iter_read_items()
    assert (await anext(items)).offset == 7
    # The first Record of the batch has the Superseded item's offset and comes next, so a read
    # that resumes here must not skip it.
    assert items.next_request is not None
    assert items.next_request.from_offset == 7


async def test_start_now_runs_a_call_to_its_first_wait_at_once():
    steps: list[str] = []
    gate: asyncio.Future[str] = asyncio.get_running_loop().create_future()

    async def call() -> str:
        steps.append("started")
        return await gate

    task = _support.start_now(call())
    # The call's first step ran before start_now returned, as a command would.
    assert steps == ["started"]
    gate.set_result("done")
    assert await task == "done"

    async def fails() -> None:
        raise ValueError("refused")

    with pytest.raises(ValueError, match="refused"):
        await _support.start_now(fails())


def test_a_create_outside_a_workflow_sends_a_new_request_id_for_each_call():
    first, second = _support.request_id(), _support.request_id()
    assert first and second and first != second
