"""A client reader that sleeps on the stream's notifier while the stream is idle.

With notifications on, a read waits its idle time in the store, and a read
that finds nothing arms the stream. The reader then waits on the notifier
with ``WaitForNotification`` and reads again with ``arm`` after every answer.
The fakes here script Core's answers. The live cases need a server with the
stream notifier and run in the Jetty lane.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.bridge.proto.streams import (
    ReadRecord,
    ReadRequest,
    ReadResponse,
    StreamFailure,
    StreamFailureKind,
    WaitForNotificationRequest,
    WaitForNotificationResponse,
)
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.bridge.streams import StreamCallFailure
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef, StreamStorePlugin, get_stream_handle
from temporalio.contrib.streams._handles import StreamHandle
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError
from tests.contrib.streams._support import connect_with, read_all
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

_UNSUPPORTED = StreamCallFailure(
    StreamFailure(
        kind=StreamFailureKind.STREAM_FAILURE_KIND_UNSUPPORTED,
        message="the server has no stream notifier",
    )
)


class FakeService:
    """Answers reads and waits from scripts, and records each request."""

    def __init__(self, reads: list[ReadResponse], waits: list[Any]) -> None:
        self.reads = reads
        self.waits = waits
        self.calls: list[ReadRequest | WaitForNotificationRequest] = []

    async def read(self, request: ReadRequest) -> ReadResponse:
        copy = ReadRequest()
        copy.CopyFrom(request)
        self.calls.append(copy)
        return self.reads.pop(0)

    async def wait_for_notification(
        self, request: WaitForNotificationRequest
    ) -> WaitForNotificationResponse:
        self.calls.append(request)
        answer = self.waits.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def fake_handle(client: Client, store: StreamStorePlugin, service: FakeService):
    async def service_for(_client: Client) -> Any:
        return service

    setattr(store, "_service_for", service_for)
    return StreamHandle(store, client, StreamRef.for_workflow("owner", topic="t"))


def record(cursor: str, value: str) -> ReadRecord:
    [body] = DataConverter.default.payload_converter.to_payloads([value])
    return ReadRecord(
        cursor=cursor,
        stored=WireRecord(
            topic="t", kind=StreamRecordKind.STREAM_RECORD_KIND_DATA, body=body
        ),
    )


def summary(calls: list[Any]) -> list[tuple]:
    out: list[tuple] = []
    for call in calls:
        if isinstance(call, ReadRequest):
            out.append(("read", call.after, call.arm, call.wait.ToTimedelta()))
        else:
            out.append(("wait", call.after_counter))
    return out


IDLE = timedelta(seconds=2)
BLOCKING = timedelta(seconds=30)


async def test_an_idle_read_waits_on_the_notifier_then_reads_again(client: Client):
    service = FakeService(
        reads=[
            ReadResponse(records=[record("c1", "a")], cursor="c1"),
            ReadResponse(cursor="c1", armed=True, wake_after=41),
            ReadResponse(records=[record("c2", "b")], cursor="c2", done=True),
        ],
        waits=[WaitForNotificationResponse(counter=42, position="p42")],
    )
    handle = fake_handle(client, MemoryStreams().notify_on_append(), service)
    records = await read_all(handle.read())
    assert [r.value for r in records] == ["a", "b"]
    assert summary(service.calls) == [
        ("read", "", True, IDLE),
        ("read", "c1", True, IDLE),
        ("wait", 41),
        ("read", "c1", True, IDLE),
    ]


async def test_a_wait_at_the_poll_limit_reads_again_with_arm(client: Client):
    # The read after the limit is what finds a record whose notification was lost.
    service = FakeService(
        reads=[
            ReadResponse(cursor="c0", armed=True, wake_after=7),
            ReadResponse(cursor="c0", armed=True, wake_after=7),
            ReadResponse(records=[record("c1", "late")], cursor="c1", done=True),
        ],
        waits=[
            WaitForNotificationResponse(counter=7, timed_out=True),
            WaitForNotificationResponse(counter=7, timed_out=True),
        ],
    )
    handle = fake_handle(client, MemoryStreams().notify_on_append(), service)
    records = await read_all(handle.read())
    assert [r.value for r in records] == ["late"]
    assert summary(service.calls) == [
        ("read", "", True, IDLE),
        ("wait", 7),
        ("read", "c0", True, IDLE),
        ("wait", 7),
        ("read", "c0", True, IDLE),
    ]


async def test_a_closed_stream_is_read_to_its_end(client: Client):
    service = FakeService(
        reads=[
            ReadResponse(cursor="c0", armed=True, wake_after=3),
            ReadResponse(records=[record("c1", "last")], cursor="c1", done=True),
        ],
        waits=[WaitForNotificationResponse(counter=4, closed=True)],
    )
    handle = fake_handle(client, MemoryStreams().notify_on_append(), service)
    assert [r.value for r in await read_all(handle.read())] == ["last"]


async def test_without_a_notifier_a_read_waits_in_the_store(client: Client):
    store = MemoryStreams().notify_on_append()
    service = FakeService(
        reads=[
            ReadResponse(cursor="c0", armed=True, wake_after=0),
            ReadResponse(records=[record("c1", "a")], cursor="c1", done=True),
            ReadResponse(cursor="", done=True),
        ],
        waits=[_UNSUPPORTED],
    )
    handle = fake_handle(client, store, service)
    assert [r.value for r in await read_all(handle.read())] == ["a"]
    # The process's later reads don't arm either.
    await read_all(handle.read())
    assert summary(service.calls) == [
        ("read", "", True, IDLE),
        ("wait", 0),
        ("read", "c0", False, BLOCKING),
        ("read", "", False, BLOCKING),
    ]


async def test_without_notifications_a_read_never_arms(client: Client):
    service = FakeService(
        reads=[ReadResponse(records=[record("c1", "a")], cursor="c1", done=True)],
        waits=[],
    )
    handle = fake_handle(client, MemoryStreams(), service)
    assert [r.value for r in await read_all(handle.read())] == ["a"]
    assert summary(service.calls) == [("read", "", False, BLOCKING)]


async def test_an_idle_read_on_a_server_without_a_notifier_still_gets_records(
    client: Client,
):
    # The test server has no notifier, so the reader falls back to the
    # store's blocking read and still sees an append after its idle time.
    store = MemoryStreams().notify_on_append()
    streams_client = await connect_with(client, store)
    async with new_worker(streams_client, Waits) as worker:
        workflow_id = await running_owner(streams_client, worker.task_queue)
        handle = get_stream_handle(streams_client, workflow_id, topic="t")
        records = handle.read()
        first = asyncio.ensure_future(records.__anext__())
        await asyncio.sleep(IDLE.total_seconds() + 1)
        await handle.producer(producer_id="writer", attempt=1).append("woke")
        assert (await asyncio.wait_for(first, 10)).value == "woke"
        await records.aclose()
        await streams_client.get_workflow_handle(workflow_id).signal(Waits.finish)


async def test_a_read_raises_the_close_failure_after_the_last_record(client: Client):
    failure = Failure(
        message="gave up",
        application_failure_info=ApplicationFailureInfo(type="GaveUp"),
    )
    service = FakeService(
        reads=[
            ReadResponse(
                records=[record("c1", "last")],
                cursor="c1",
                done=True,
                close_failure=failure,
                closed_by="@run",
            )
        ],
        waits=[],
    )
    handle = fake_handle(client, MemoryStreams(), service)
    seen = []
    with pytest.raises(ApplicationError) as raised:
        async for r in handle.read():
            seen.append(r.value)
    assert seen == ["last"]
    assert raised.value.type == "GaveUp"


async def test_a_close_with_a_result_ends_the_read_quietly(client: Client):
    [result] = DataConverter.default.payload_converter.to_payloads(["done"])
    service = FakeService(
        reads=[ReadResponse(cursor="c0", done=True, close_result=result)], waits=[]
    )
    handle = fake_handle(client, MemoryStreams(), service)
    assert await read_all(handle.read()) == []
