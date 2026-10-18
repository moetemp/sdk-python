"""How long a client read waits in the store before it answers empty.

The fakes here script the stream runtime's answers and record each request.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from temporalio.bridge.proto.streams import ReadRecord, ReadRequest, ReadResponse
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef, StreamStorePlugin
from temporalio.contrib.streams._handles import StreamHandle
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter
from tests.contrib.streams._support import read_all


class FakeService:
    """Answers reads from a script, and records each request."""

    def __init__(self, reads: list[ReadResponse]) -> None:
        self.reads = reads
        self.calls: list[ReadRequest] = []

    async def read(self, request: ReadRequest) -> ReadResponse:
        copy = ReadRequest()
        copy.CopyFrom(request)
        self.calls.append(copy)
        return self.reads.pop(0)


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


def summary(calls: list[ReadRequest]) -> list[tuple[str, timedelta]]:
    return [(call.after, call.wait.ToTimedelta()) for call in calls]


def idle_then_one_record() -> FakeService:
    return FakeService(
        reads=[
            ReadResponse(cursor="c0"),
            ReadResponse(records=[record("c1", "a")], cursor="c1", done=True),
        ]
    )


async def test_an_idle_read_waits_two_seconds_in_the_store(client: Client):
    service = idle_then_one_record()
    handle = fake_handle(client, MemoryStreams(), service)
    records = await read_all(handle.read())
    assert [r.value for r in records] == ["a"]
    two = timedelta(seconds=2)
    assert summary(service.calls) == [("", two), ("c0", two)]


async def test_idle_wait_sets_how_long_an_idle_read_waits_in_the_store(
    client: Client,
):
    service = idle_then_one_record()
    handle = fake_handle(client, MemoryStreams(), service)
    idle = timedelta(milliseconds=250)
    records = await read_all(handle.read(idle_wait=idle))
    assert [r.value for r in records] == ["a"]
    assert summary(service.calls) == [("", idle), ("c0", idle)]


@pytest.mark.parametrize("idle", [timedelta(0), timedelta(seconds=-1)])
def test_an_idle_wait_that_is_not_positive_is_refused(client: Client, idle: timedelta):
    handle = fake_handle(client, MemoryStreams(), FakeService(reads=[]))
    with pytest.raises(ValueError, match="idle_wait"):
        handle.read(idle_wait=idle)
