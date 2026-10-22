import concurrent.futures
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest

import temporalio.api.streamservice.v1 as stream_pb
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib import streams
from temporalio.contrib.streams import native
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.contrib.streams.native import NativeStreamPlugin
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.converter import PayloadConverter
from tests.contrib.streams.fake_native import (
    FakeStreamService,
    FakeSystemNexus,
    records_of,
    start_frontend,
)
from tests.helpers import new_worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


@pytest.fixture
async def fake() -> AsyncIterator[tuple[FakeStreamService, Client]]:
    service = FakeStreamService()
    server, port = await start_frontend(service)
    client = await Client.connect(
        f"127.0.0.1:{port}", plugins=[NativeStreamPlugin()], namespace="ns"
    )
    yield service, client
    await server.stop(grace=None)


def decode(payload: Any) -> Any:
    return PayloadConverter.default.from_payload(payload)


async def test_client_calls_go_to_the_frontend(
    fake: tuple[FakeStreamService, Client],
):
    service, client = fake
    stream_client = streams.StreamClient(client)
    stream = await stream_client.create(Events, id="chat")
    assert stream.backend == "native"
    tokens = stream.topic(Events.tokens)
    assert await tokens.publish("a", "b") == 2
    assert await stream.get_offset() == 3
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "a",
        "b",
    ]
    await stream.close(result="done")
    items = [item async for item in stream.subscribe()]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"
    with pytest.raises(streams.StreamClosedError):
        await stream_client.create(Events, id="chat")
    appends = [request for name, request in service.calls if name == "AppendRecords"]
    assert appends[0].namespace == "ns"
    assert appends[0].producer.kind == stream_pb.ProducerKind.PRODUCER_KIND_CLIENT
    assert appends[0].sequence == 1


async def test_server_errors_keep_their_reason(
    fake: tuple[FakeStreamService, Client],
):
    _, client = fake
    missing = streams.StreamClient(client).get_handle(Events, id="nobody")
    with pytest.raises(streams.StreamNotFoundError):
        await missing.get_offset()
    with pytest.raises(streams.StreamNotFoundError):
        await missing.topic(Events.tokens).publish("lost")


async def test_a_reader_waits_for_new_records(
    fake: tuple[FakeStreamService, Client],
):
    _, client = fake
    stream = await streams.StreamClient(client).create(Events, id="live")
    reader = stream.subscribe()
    await stream.topic(Events.tokens).publish("late")
    first = await reader.__anext__()
    await reader.aclose()
    assert isinstance(first, streams.Record) and first.data == "late"


@activity.defn
async def write_native(stream: streams.ActivityStream[Events]) -> None:
    await stream.topic(Events.tokens).publish("activity")


@activity.defn
def write_native_sync(stream: streams.SyncActivityStream[Events]) -> None:
    stream.topic(Events.tokens).publish("sync activity")


@workflow.defn
class WritesNativeAndRedis:
    @workflow.run
    async def run(self) -> list[str | None]:
        on_redis = await stream_workflow.create_stream(Events, name="old")
        on_native = await stream_workflow.create_stream(
            Events, name="new", backend="native"
        )
        await on_redis.topic(Events.tokens).publish("redis")
        await on_native.topic(Events.tokens).publish("workflow 1", "workflow 2")
        await workflow.execute_activity(
            write_native,
            streams.ActivityStream.from_handle(on_native),
            start_to_close_timeout=timedelta(seconds=10),
        )
        await workflow.execute_activity(
            write_native_sync,
            streams.SyncActivityStream.from_handle(on_native),
            start_to_close_timeout=timedelta(seconds=10),
        )
        await on_native.topic(Events.tokens).publish("last")
        return [on_redis.backend, on_native.backend]


async def test_redis_and_native_in_one_worker(
    client: Client,
    redis_url: str,
    fake: tuple[FakeStreamService, Client],
    monkeypatch: pytest.MonkeyPatch,
):
    # Workflow calls go to the System Nexus handler and Activity calls to the frontend, while
    # Redis Streams keep their buffered path.
    service, fake_client = fake
    plugin = NativeStreamPlugin()
    config = client.config()
    config["interceptors"] = [FakeSystemNexus(service)]
    config["plugins"] = [
        RedisStreamPlugin(redis_url, key_prefix=f"t-{uuid.uuid4()}"),
        plugin,
    ]
    two = Client(**config)
    # The dev server has no Stream service, so the Activities' calls go to the fake frontend.
    monkeypatch.setattr(native, "_activity_client", lambda: None)
    async with new_worker(
        two,
        WritesNativeAndRedis,
        activities=[write_native, write_native_sync],
        max_concurrent_activities=2,
        activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=2),
    ) as worker:
        plugin._worker_client = fake_client
        handle = await two.start_workflow(
            WritesNativeAndRedis.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == ["redis", "native"]
    first_run = handle.first_execution_run_id
    assert first_run
    native_id = f"{handle.id}:{first_run}:new"
    assert records_of(service, native_id, decode) == [
        "workflow 1",
        "workflow 2",
        "activity",
        "sync activity",
        "last",
        ("closed", ["redis", "native"]),
    ]
    old = streams.StreamClient(two).workflow_stream(
        handle, Events, name="old", backend="redis"
    )
    items = [item async for item in old.subscribe(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "redis"
    ]
    assert isinstance(items[-1], streams.Closed)
