"""A store configuration registers as a client plugin, and Core connects to it."""

from __future__ import annotations

import hashlib
import uuid
from datetime import timedelta

import pytest

from temporalio import activity
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams import (
    AppendRecord,
    AppendRequest,
    LatestRequest,
    NamedProducer,
    StreamAddress,
    StreamOwnerKind,
)
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.client import Client
from temporalio.contrib.streams import StreamError, StreamNotFoundError
from temporalio.contrib.streams._plugin import call
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.worker import Worker
from tests.contrib.streams._support import connect_with


async def test_one_store_per_client(client: Client):
    first, second = MemoryStreams(), MemoryStreams()
    config = client.config()
    config["plugins"] = [first, second]
    with pytest.raises(ValueError, match="is already registered"):
        Client(**config)
    # The same instance twice is one store, not two.
    config["plugins"] = [first, first]
    Client(**config)


async def test_a_worker_takes_the_store_of_its_client(client: Client):
    store = MemoryStreams()
    streams_client = await connect_with(client, store)
    Worker(streams_client, task_queue=str(uuid.uuid4()), activities=[_noop])
    # The store on the client and the Worker configures the Worker twice.
    with pytest.warns(UserWarning, match="same plugin type"):
        Worker(
            streams_client,
            task_queue=str(uuid.uuid4()),
            activities=[_noop],
            plugins=[store],
        )


async def test_a_store_on_the_worker_alone_is_refused(client: Client):
    with pytest.raises(ValueError, match="registered on the Worker's client"):
        Worker(client, task_queue="tq", activities=[_noop], plugins=[MemoryStreams()])
    # A client made without connecting has not connected its store either.
    config = client.config()
    config["plugins"] = [MemoryStreams()]
    with pytest.raises(ValueError, match="registered on the Worker's client"):
        Worker(Client(**config), task_queue="tq", activities=[_noop])


async def test_a_client_made_without_connecting_connects_its_store_on_use(
    client: Client,
):
    store = MemoryStreams()
    config = client.config()
    config["plugins"] = [store]
    service = await store._service_for(Client(**config))
    # An owner that does not exist is Core's answer, so the call reached the store.
    stream = StreamAddress(
        namespace=client.namespace,
        owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
        workflow_id=f"no-such-owner-{uuid.uuid4()}",
        topic="out",
    )
    with pytest.raises(StreamNotFoundError):
        await call(service.latest(LatestRequest(stream=stream)))


async def test_a_store_core_cannot_reach_fails_the_connect(client: Client):
    with pytest.raises(StreamError):
        await connect_with(
            client,
            RedisStreams("redis://127.0.0.1:1", response_timeout=timedelta(seconds=1)),
        )


def test_redis_settings_go_into_the_store_config():
    store = RedisStreams(
        ["redis://a:6379", "redis://b:6379"],
        cluster=True,
        key_prefix="app",
        retention=timedelta(hours=2),
        blocking_reads_per_node=4,
        response_timeout=timedelta(seconds=3),
    )
    config = store._config.redis
    assert list(config.urls) == ["redis://a:6379", "redis://b:6379"]
    assert config.cluster
    assert config.key_prefix == "app"
    assert config.retention.ToTimedelta() == timedelta(hours=2)
    assert config.blocking_reads_per_node == 4
    assert config.response_timeout.ToTimedelta() == timedelta(seconds=3)
    # Unset settings leave Core's defaults.
    defaults = RedisStreams("redis://a:6379")._config.redis
    assert defaults.blocking_reads_per_node == 0
    assert not defaults.HasField("response_timeout")


def test_redis_setting_mistakes_are_value_errors():
    with pytest.raises(ValueError, match="needs a URL"):
        RedisStreams([])
    with pytest.raises(ValueError, match="key_prefix"):
        RedisStreams("redis://a", key_prefix="")
    with pytest.raises(ValueError, match="retention"):
        RedisStreams("redis://a", retention=timedelta(microseconds=10))


async def test_deleting_removes_what_a_workflow_stream_holds(client: Client):
    store = MemoryStreams()
    with pytest.raises(ValueError, match="no client has connected"):
        await store.delete_workflow_streams(client.namespace, "wf")
    streams_client = await connect_with(client, store)
    service = await store._service_for(streams_client)
    workflow_id = f"stream-owner-{uuid.uuid4()}"
    await streams_client.start_workflow(
        "StreamOwner", id=workflow_id, task_queue=f"unpolled-{uuid.uuid4()}"
    )
    stream = address(client, workflow_id)
    await call(
        service.append(
            AppendRequest(
                stream=stream,
                named=NamedProducer(producer_id="p", attempt=1),
                sequence=1,
                records=[
                    AppendRecord(
                        kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
                        body=Payload(data=b"a"),
                        content_hash=hashlib.sha256(b"a").digest(),
                    )
                ],
                digest=hashlib.sha256(b"batch").digest(),
            )
        )
    )
    assert (await call(service.latest(LatestRequest(stream=stream)))).cursor

    assert await store.delete_workflow_streams(client.namespace, workflow_id) > 0
    assert (await call(service.latest(LatestRequest(stream=stream)))).cursor == ""


async def test_one_store_serves_one_client_connection(client: Client):
    store = MemoryStreams()
    first = await connect_with(client, store)
    # Core asks about owners over the connection that connected the store, so a
    # second connection would get the first one's answers.
    with pytest.raises(ValueError, match="one client connection"):
        await connect_with(client, store)
    config = client.config()
    config["plugins"] = [store]
    with pytest.raises(ValueError, match="one client connection"):
        await store._service_for(Client(**config))
    # A client copied from the connected one shares its connection.
    copied = first.config()
    assert await store._service_for(Client(**copied))


async def test_a_worker_needs_a_client_made_with_connect_after_a_lazy_connect(
    client: Client,
):
    store = MemoryStreams()
    config = client.config()
    config["plugins"] = [store]
    lazy = Client(**config)
    # A handle call connects the store, but the client still wasn't connected
    # through the plugin, so Workers built from it are refused all the same.
    await store._service_for(lazy)
    with pytest.raises(ValueError, match="registered on the Worker's client"):
        Worker(lazy, task_queue=str(uuid.uuid4()), activities=[_noop])


@activity.defn
async def _noop() -> None:
    pass


def address(client: Client, workflow_id: str) -> StreamAddress:
    return StreamAddress(
        namespace=client.namespace,
        owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
        workflow_id=workflow_id,
        topic="out",
    )


class _CountsFlushes(MemoryStreams):
    def __init__(self) -> None:
        super().__init__()
        self.worker: Worker | None = None
        self.flushed_after_shutdown: list[bool] = []

    async def flush_notifications(self) -> None:
        assert self.worker is not None
        self.flushed_after_shutdown.append(self.worker.is_shutdown)
        await super().flush_notifications()


async def test_a_worker_flushes_notifications_after_it_stops(client: Client):
    store = _CountsFlushes()
    streams_client = await connect_with(client, store)
    store.worker = Worker(
        streams_client, task_queue=str(uuid.uuid4()), activities=[_noop]
    )
    async with store.worker:
        assert store.flushed_after_shutdown == []
    # The block waits for the flush, which runs once the Worker stopped.
    assert store.flushed_after_shutdown == [True]


async def test_a_flush_before_any_connect_returns_at_once():
    await MemoryStreams().flush_notifications()


async def test_a_flush_reaches_core(client: Client):
    store = MemoryStreams()
    await connect_with(client, store)
    # Nothing is out, so Core answers at once.
    await store.flush_notifications()
