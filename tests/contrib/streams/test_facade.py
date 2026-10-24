import asyncio
import gc
from dataclasses import dataclass
from datetime import timedelta

import google.protobuf.any_pb2
import pytest

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
from temporalio.bridge.streams import StreamCallError
from temporalio.client import Client
from temporalio.common import RawValue, streams
from temporalio.common.streams import _errors
from temporalio.common.streams._backend import _ExtensionStreamBackend
from temporalio.common.streams.proto import v1 as ext_pb
from temporalio.common.streams.system import models
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from tests.contrib.streams.fake_extension import FakeExtension


@dataclass
class Turn:
    kind: str
    text: str = ""


def fake_client(client: Client) -> tuple[Client, FakeExtension]:
    extension = FakeExtension()
    plugin = streams.StreamPlugin(
        "fake-streams",
        backend_name="fake",
        default=False,
        stream_backend=_ExtensionStreamBackend(extension),
    )
    config = client.config()
    config["plugins"] = [plugin]
    return Client(**config), extension


def test_runtime_errors_map_to_stream_errors():
    details = ext_pb.StreamErrorDetails(reason="stream_fenced").SerializeToString()
    err = _errors.from_call_error(
        StreamCallError(int(RPCStatusCode.FAILED_PRECONDITION), "fenced", details)
    )
    assert isinstance(err, streams.StreamFencedError)
    assert err.reason == "stream_fenced"
    other = _errors.from_call_error(
        StreamCallError(int(RPCStatusCode.INTERNAL), "odd", b"")
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
    stream = stream_client.workflow_stream(handle, Events)
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
    mapped = _errors.from_call_error(
        StreamCallError(int(RPCStatusCode.FAILED_PRECONDITION), "closed", details)
    )
    assert mapped.code == RPCStatusCode.FAILED_PRECONDITION


async def test_handles_of_one_client_share_a_producer_and_free_it(client: Client):
    client, extension = fake_client(client)
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
    assert len(producers) == 1
    # A handle that goes with no close frees its writer, so a server that makes a handle per
    # request keeps no writer for it.
    del other
    gc.collect()
    assert len(extension.freed) == 1


async def test_rpc_timeout_bounds_each_call_and_not_the_read(client: Client):
    client, extension = fake_client(client)
    stream = await streams.StreamClient(client).create(Events, id="slow")
    call = extension.call

    async def slow_describe(method: str, input: bytes) -> bytes:
        if method == "DescribeStream":
            await asyncio.sleep(1)
        return await call(method, input)

    setattr(extension, "call", slow_describe)
    with pytest.raises(RPCError) as err:
        await stream.get_offset(rpc_timeout=timedelta(milliseconds=100))
    assert err.value.status == RPCStatusCode.DEADLINE_EXCEEDED

    async def publish_later() -> None:
        await asyncio.sleep(1)
        await stream.topic(Events.tokens).publish("late")

    # Each call of a read that waits answers before the deadline, so the read goes on.
    reader = stream.topic(Events.tokens).subscribe(
        rpc_timeout=timedelta(milliseconds=300)
    )
    publisher = asyncio.create_task(publish_later())
    item = await anext(reader)
    await reader.aclose()
    await publisher
    assert isinstance(item, streams.Record) and item.data == "late"


def test_unsupported_capability_maps_to_its_error():
    details = ext_pb.StreamErrorDetails(reason="stream_unsupported").SerializeToString()
    err = _errors.from_call_error(
        StreamCallError(
            int(RPCStatusCode.UNIMPLEMENTED), "The Store lacks replica_wait", details
        )
    )
    assert isinstance(err, streams.StreamUnsupportedError)
    assert err.non_retryable and err.code == RPCStatusCode.UNIMPLEMENTED
    assert err.message.endswith("replica_wait")


def test_server_failure_maps_to_its_stream_error():
    detail = google.protobuf.any_pb2.Any()
    detail.Pack(ext_pb.StreamErrorDetails(reason="stream_closed"))
    status = temporalio.api.common.v1.GrpcStatus(
        code=9, message="closed", details=[detail]
    )
    err = _errors.from_rpc_error(
        RPCError(
            "closed", RPCStatusCode.FAILED_PRECONDITION, status.SerializeToString()
        )
    )
    assert isinstance(err, streams.StreamClosedError)
    plain = RPCError("down", RPCStatusCode.UNAVAILABLE, b"")
    assert _errors.from_rpc_error(plain) is plain
