import asyncio
from dataclasses import dataclass
from datetime import timedelta

import pytest

import temporalio.api.streamservice.v1 as stream_pb
from temporalio.client import Client
from temporalio.common import RawValue
from temporalio.contrib import streams
from temporalio.contrib.streams import _errors, _ids
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.system import models
from temporalio.exceptions import ApplicationError
from temporalio.plugin import SimplePlugin
from temporalio.service import RPCStatusCode
from temporalio.worker import ExtensionError
from tests.contrib.streams.fake_extension import FakeExtension


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


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


async def test_client_writes_reads_and_closes(client: Client):
    client, extension = fake_client(client)
    stream = await streams.StreamClient(client).create(Events, id="chat-42")
    first = await stream.topic(Events.turns).publish(Turn("model"))
    second = await stream.topic("tokens").publish("hello")
    assert second == first + 1
    assert await stream.get_offset() == second + 1
    await stream.close(result="bye")
    items = [item async for item in stream.subscribe()]
    assert [type(item) for item in items] == [
        streams.Record,
        streams.Record,
        streams.Closed,
    ]
    turn, token, closed = items
    assert isinstance(turn, streams.Record) and turn.data == Turn("model")
    assert isinstance(token, streams.Record) and token.data == "hello"
    assert turn.producer is not None
    assert turn.producer.kind == stream_pb.ProducerKind.PRODUCER_KIND_CLIENT
    assert isinstance(closed, streams.Closed) and closed.result == "bye"
    assert extension.namespaces == {client.namespace}

    raw = stream.topic("raw", type=RawValue)
    assert raw.type is RawValue
    resumed = [item async for item in stream.subscribe(from_offset=token.offset)]
    assert [item.offset for item in resumed] == [token.offset, closed.offset]


async def test_read_waits_for_create(client: Client):
    client, _ = fake_client(client)
    stream_client = streams.StreamClient(client)
    handle = client.get_workflow_handle("wf-1", first_execution_run_id="run-1")
    stream = stream_client.get_workflow_stream(handle, Events)
    with pytest.raises(streams.StreamNotFoundError):
        async for _ in stream.subscribe():
            pass

    async def create_later() -> None:
        created = await stream_client.create(Events, id=stream.id)
        await created.topic(Events.tokens).publish("five")
        await created.close()

    task = asyncio.create_task(create_later())
    items = [item async for item in stream.subscribe(create_wait=timedelta(seconds=5))]
    await task
    assert isinstance(items[0], streams.Record) and items[0].data == "five"
    assert isinstance(items[-1], streams.Closed)


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


async def test_each_client_handle_is_its_own_producer(client: Client):
    client, _ = fake_client(client)
    stream_client = streams.StreamClient(client)
    stream = await stream_client.create(Events, id="two-writers")
    other = stream_client.get_handle(Events, id="two-writers")
    await stream.topic(Events.tokens).publish("1")
    await other.topic(Events.tokens).publish("2")
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    producers = {
        item.producer.id
        for item in items
        if isinstance(item, streams.Record) and item.producer
    }
    assert len(producers) == 2


def test_workflow_stream_has_no_reads():
    assert not hasattr(streams.WorkflowStream, "subscribe")
    assert not hasattr(streams.WorkflowStream, "get_offset")


def test_unsupported_capability_maps_to_its_error():
    details = ext_pb.StreamErrorDetails(reason="stream_unsupported").SerializeToString()
    err = _errors.from_extension_error(
        ExtensionError(
            "The Store lacks replica_wait", RPCStatusCode.UNIMPLEMENTED, details
        )
    )
    assert isinstance(err, streams.StreamUnsupportedError)
    assert err.non_retryable and err.code == RPCStatusCode.UNIMPLEMENTED
    assert err.message.endswith("replica_wait")
