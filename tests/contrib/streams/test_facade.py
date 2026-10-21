import uuid
from dataclasses import dataclass
from datetime import timedelta

import pytest

import temporalio.api.streamservice.v1 as stream_pb
from temporalio import workflow
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.contrib import streams
from temporalio.contrib.streams import _errors, _ids
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.system import models
from temporalio.plugin import SimplePlugin
from temporalio.service import RPCStatusCode
from temporalio.worker import ExtensionError
from tests.contrib.streams.fake_extension import FakeExtension
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


def fake_client(client: Client) -> tuple[Client, FakeExtension]:
    extension = FakeExtension()
    plugin = SimplePlugin("fake-streams")
    setattr(plugin, "_temporal_stream_backend", streams.StreamBackend(extension))
    config = client.config()
    config["plugins"] = [plugin]
    return Client(**config), extension


def test_ids_match_the_crate():
    assert _ids.default_stream_id("agent-1", "run-1") == "agent-1:run-1"
    assert (
        _ids.scoped_stream_id("a:b/c", "r%1", "é name")
        == "a%3Ab%2Fc:r%251:%C3%A9%20name"
    )
    assert _ids.workflow_producer_id("run:1") == "wf:run%3A1"
    assert _ids.activity_producer_id("run:1", "a/1", "s-1") == "act:run%3A1:a%2F1:s-1"
    assert _ids.client_producer_id("1@host", "p-1") == "client:1%40host:p-1"


def test_extension_errors_map_to_stream_errors():
    details = ext_pb.StreamErrorDetails(reason="stream_fenced").SerializeToString()
    err = _errors.from_extension_error(
        ExtensionError("fenced", RPCStatusCode.FAILED_PRECONDITION, details)
    )
    assert isinstance(err, streams.StreamFencedError)
    assert err.reason == "stream_fenced"
    other = _errors.from_extension_error(
        ExtensionError("odd", RPCStatusCode.INTERNAL, b"")
    )
    assert type(other) is streams.StreamError


def test_superseded_withdraws_earlier_attempts_of_one_producer():
    def producer(attempt: int) -> models.Producer:
        return models.Producer(
            kind=stream_pb.ProducerKind.PRODUCER_KIND_ACTIVITY,
            id="act:r:a:s",
            run_id="r",
            epoch=models.ProducerEpoch(attempt=attempt),
        )

    item = streams.Superseded(
        offset=9,
        supersession=models.Supersession(
            producer_kind=stream_pb.ProducerKind.PRODUCER_KIND_ACTIVITY,
            producer_id="act:r:a:s",
            epoch=models.ProducerEpoch(attempt=2),
        ),
    )
    old = streams.Record(topic="t", data=1, offset=1, producer=producer(1))
    new = streams.Record(topic="t", data=2, offset=10, producer=producer(2))
    assert item.withdraws(old)
    assert not item.withdraws(new)


async def test_client_writes_reads_and_closes(client: Client):
    client, extension = fake_client(client)
    stream = await streams.create_stream(client, "chat-42")
    turns = stream.topic("turns", type=Turn)
    turns.publish(Turn("model"))
    stream.topic("tokens", type=str).publish("hello", force_flush=True)
    await stream.flush()
    assert await stream.get_offset() == 3
    await stream.close(result="bye")
    items = [item async for item in stream.subscribe()]
    assert [type(item) for item in items] == [
        streams.Record,
        streams.Record,
        streams.Closed,
    ]
    first, second, closed = items
    assert isinstance(first, streams.Record) and first.data == Turn("model")
    assert isinstance(second, streams.Record) and second.data == "hello"
    assert first.producer is not None
    assert first.producer.kind == stream_pb.ProducerKind.PRODUCER_KIND_CLIENT
    assert isinstance(closed, streams.Closed) and closed.result == "bye"
    assert extension.namespaces == {client.namespace}

    only_tokens = [
        item async for item in stream.topic("tokens", type=RawValue).subscribe()
    ]
    assert isinstance(only_tokens[0], streams.Record)
    assert isinstance(only_tokens[0].data, RawValue)
    resumed = [item async for item in stream.subscribe(from_offset=second.offset)]
    assert [item.offset for item in resumed] == [second.offset, closed.offset]


async def test_read_ends_idle_and_resumes(client: Client):
    client, _ = fake_client(client)
    stream = await streams.create_stream(client, "idle-1")
    stream.topic("t", type=int).publish(1)
    await stream.flush()
    items = [
        item async for item in stream.subscribe(idle_timeout=timedelta(milliseconds=50))
    ]
    assert isinstance(items[-1], streams.Idle)
    assert [
        item
        async for item in stream.subscribe(
            from_offset=items[-1].offset, wait_new_records=False
        )
    ] == []


async def test_read_waits_for_create(client: Client):
    client, _ = fake_client(client)
    handle = client.get_workflow_handle("wf-1", first_execution_run_id="run-1")
    with pytest.raises(streams.StreamNotFoundError):
        async for _ in streams.for_handle(handle).subscribe():
            pass
    waiting = streams.for_handle(handle, wait_for_create=timedelta(seconds=5))

    async def create_later() -> None:
        created = await streams.create_stream(
            client, _ids.default_stream_id("wf-1", "run-1")
        )
        created.topic("t", type=int).publish(5)
        await created.close()

    import asyncio

    task = asyncio.create_task(create_later())
    items = [item async for item in waiting.subscribe()]
    await task
    assert isinstance(items[0], streams.Record) and items[0].data == 5
    assert isinstance(items[-1], streams.Closed)


def test_client_without_plugin_has_no_transport(client: Client):
    handle = client.get_workflow_handle("wf-1", first_execution_run_id="run-1")
    with pytest.raises(streams.StreamTransportMissingError):
        streams.for_handle(handle)


@workflow.defn
class PublishWithoutPlugin:
    @workflow.run
    async def run(self) -> str:
        try:
            streams.workflow_stream().topic("t", type=str).publish("x")
        except streams.StreamTransportMissingError as err:
            return type(err).__name__
        return "published"


async def test_workflow_without_plugin_has_no_transport(client: Client):
    async with new_worker(client, PublishWithoutPlugin) as worker:
        result = await client.execute_workflow(
            PublishWithoutPlugin.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert result == "StreamTransportMissingError"
