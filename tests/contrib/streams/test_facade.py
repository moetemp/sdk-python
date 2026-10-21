import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

import temporalio.api.streamservice.v1 as stream_pb
from temporalio import workflow
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.contrib import streams
from temporalio.contrib.streams import _errors, _ids
from temporalio.contrib.streams._stream import _Writable
from temporalio.contrib.streams._writer import BatchOptions, StreamWriter
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.system import models
from temporalio.exceptions import ApplicationError
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


def test_stream_errors_fail_the_workflow_not_the_task():
    err = streams.StreamClosedError("closed", reason="stream_closed")
    assert isinstance(err, ApplicationError)
    assert err.non_retryable and err.type == "StreamClosedError"
    assert not streams.StreamStorageError("down").non_retryable
    details = ext_pb.StreamErrorDetails(reason="stream_closed").SerializeToString()
    mapped = _errors.from_extension_error(
        ExtensionError("closed", RPCStatusCode.FAILED_PRECONDITION, details)
    )
    assert mapped.code == RPCStatusCode.FAILED_PRECONDITION


async def test_each_client_writer_is_its_own_producer(client: Client):
    client, _ = fake_client(client)
    stream = await streams.create_stream(client, "two-writers")
    other = streams.for_handle(
        client.get_workflow_handle("unused", first_execution_run_id="run")
    )
    other._id = stream.ref.id
    stream.topic("t", type=int).publish(1)
    other.topic("t", type=int).publish(2)
    await stream.flush()
    await other.flush()
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    producers = {
        item.producer.id
        for item in items
        if isinstance(item, streams.Record) and item.producer
    }
    assert len(producers) == 2


async def test_for_handle_finds_the_first_run_with_one_describe(client: Client):
    async with new_worker(client, PublishWithoutPlugin) as worker:
        handle = await client.start_workflow(
            PublishWithoutPlugin.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
    fake, _ = fake_client(client)
    first_run = handle.first_execution_run_id
    assert first_run
    await streams.create_stream(fake, _ids.default_stream_id(handle.id, first_run))
    by_id = streams.for_handle(fake.get_workflow_handle(handle.id))
    with pytest.raises(RuntimeError):
        by_id.ref
    assert await by_id.get_offset() == 0
    assert by_id.ref.id == _ids.default_stream_id(handle.id, first_run)


async def test_writer_publishes_from_another_thread(client: Client):
    extension = FakeExtension()
    backend = streams.StreamBackend(extension)
    await backend.call_async(
        "CreateStream",
        stream_pb.CreateStreamRequest(stream_id="threads"),
        stream_pb.CreateStreamResponse,
    )

    async def stream_id() -> str:
        return "threads"

    writer = StreamWriter(
        backend=backend,
        converter=client.data_converter,
        namespace="default",
        stream_id=stream_id,
        producer=stream_pb.Producer(
            kind=stream_pb.ProducerKind.PRODUCER_KIND_CLIENT, id="client:a:b"
        ),
        batch=BatchOptions(),
        create_first=False,
        loop=asyncio.get_running_loop(),
    )
    await asyncio.to_thread(writer.publish, "t", "from a thread", False)
    await writer.flush()
    assert len(extension.streams["threads"].records) == 1


async def test_for_ref_opens_a_client_stream(client: Client):
    client, _ = fake_client(client)
    created = await streams.create_stream(client, "by-ref")
    created.topic("t", type=int).publish(7)
    await created.flush()
    opened = streams.for_ref(client, created.ref)
    reader = opened.topic("t", type=int).subscribe(wait_new_records=False)
    first = await reader.__anext__()
    await reader.aclose()
    assert isinstance(first, streams.Record) and first.data == 7


def test_topic_types_check():
    untyped: streams.Topic[Any] = streams.Topic(_NoWrites(), "t", None)
    typed: streams.Topic[int] = streams.Topic(_NoWrites(), "t", int)
    assert untyped.type is None and typed.type is int


class _NoWrites(_Writable):
    def _publish(self, topic: str, value: Any, force_flush: bool) -> None:
        raise AssertionError("no writes")


async def test_create_wait_ends_with_the_run(client: Client):
    async with new_worker(client, PublishWithoutPlugin) as worker:
        handle = await client.start_workflow(
            PublishWithoutPlugin.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
    fake, _ = fake_client(client)
    stream = streams.for_handle(
        fake.get_workflow_handle(
            handle.id, first_execution_run_id=handle.first_execution_run_id
        ),
        wait_for_create=timedelta(minutes=5),
        end_with_run=True,
    )
    start = asyncio.get_running_loop().time()
    assert [item async for item in stream.subscribe()] == []
    assert asyncio.get_running_loop().time() - start < 30


async def test_read_default_type_for_untyped_topics(client: Client):
    client, _ = fake_client(client)
    stream = await streams.create_stream(client, "default-type")
    stream.topic("turns").publish(Turn("model"))
    stream.topic("tokens").publish("hi")
    await stream.flush()
    reader = streams.for_ref(client, stream.ref)
    reader.topic("tokens", type=RawValue)
    items = [item async for item in reader.subscribe(type=Turn, wait_new_records=False)]
    first, second = items
    assert isinstance(first, streams.Record) and first.data == Turn("model")
    # A typed Topic keeps its own type.
    assert isinstance(second, streams.Record) and isinstance(second.data, RawValue)
