import asyncio
import gc
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

import pytest

from temporalio import common
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import StreamErrorReason, StreamProducerKind
from temporalio.api.stream.v1 import StreamErrorDetails
from temporalio.bridge.streams import StreamCallError
from temporalio.client import Client
from temporalio.common._streams import _errors, _stream_client
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from tests.contrib.streams.fake_extension import fake_client


@dataclass
class Turn:
    kind: str
    text: str = ""


def _details(reason: StreamErrorReason.ValueType) -> bytes:
    return StreamErrorDetails(reason=reason).SerializeToString()


def test_runtime_errors_map_to_stream_errors():
    fenced = StreamErrorReason.STREAM_ERROR_REASON_FENCED
    err = _errors.from_call_error(
        StreamCallError(
            int(RPCStatusCode.FAILED_PRECONDITION), "fenced", _details(fenced)
        )
    )
    assert isinstance(err, common.StreamFencedError)
    assert err.reason == fenced
    # A reason with no class of its own gives the base class, with the reason.
    sequence = StreamErrorReason.STREAM_ERROR_REASON_SEQUENCE
    behind = _errors.from_call_error(
        StreamCallError(
            int(RPCStatusCode.FAILED_PRECONDITION), "behind", _details(sequence)
        )
    )
    assert type(behind) is common.StreamError and behind.reason == sequence
    other = _errors.from_call_error(
        StreamCallError(int(RPCStatusCode.INTERNAL), "odd", b"")
    )
    assert type(other) is common.StreamError
    assert other.reason == StreamErrorReason.STREAM_ERROR_REASON_UNSPECIFIED


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@common.defn(name="tokens")
class Tokens(common.StreamDefinition[str]): ...


async def test_client_writes_reads_and_closes(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream(Turns, id="chat-42")
    first = await stream.publish(Turn("model"))
    second = await stream.publish(Turn("user", "hello"))
    assert second == first + 1
    assert await stream.get_offset() == second + 1
    await stream.close(result="bye")
    items = [item async for item in stream.subscribe()]
    assert [type(item) for item in items] == [
        common.Record,
        common.Record,
        common.Closed,
    ]
    turn, token, closed = items
    assert isinstance(turn, common.Record) and turn.data == Turn("model")
    assert isinstance(token, common.Record) and token.data == Turn("user", "hello")
    assert turn.producer is not None
    assert turn.producer.kind == StreamProducerKind.STREAM_PRODUCER_KIND_CLIENT
    assert isinstance(closed, common.Closed) and closed.result == "bye"
    assert extension.namespaces == {client.namespace}

    resumed = [item async for item in stream.subscribe(offset=token.offset)]
    assert [item.offset for item in resumed] == [token.offset, closed.offset]


async def test_read_waits_for_create(client: Client):
    client, _ = fake_client(client)
    stream = client.stream(Tokens, id="wf-1:run-1:tokens")
    with pytest.raises(common.StreamNotFoundError):
        async for _ in stream.subscribe():
            pass

    async def create_later() -> None:
        created = await client.create_stream(Tokens, id=stream.id)
        await created.publish("five")
        await created.close()

    task = asyncio.create_task(create_later())
    items = [item async for item in stream.subscribe(create_wait=timedelta(seconds=5))]
    await task
    assert isinstance(items[0], common.Record) and items[0].data == "five"
    assert isinstance(items[-1], common.Closed)


def test_stream_errors_fail_the_workflow_not_the_task():
    closed = StreamErrorReason.STREAM_ERROR_REASON_CLOSED
    err = common.StreamClosedError("closed", reason=closed)
    assert isinstance(err, ApplicationError)
    assert err.non_retryable and err.type == "StreamClosedError"
    assert not common.StreamStorageError("down").non_retryable
    mapped = _errors.from_call_error(
        StreamCallError(
            int(RPCStatusCode.FAILED_PRECONDITION), "closed", _details(closed)
        )
    )
    assert mapped.code == RPCStatusCode.FAILED_PRECONDITION
    # A Workflow await gets the error from a Local Activity, with the reason and no code.
    returned = _errors.from_activity_error(
        ApplicationError("closed", int(closed), type="StreamClosedError")
    )
    assert isinstance(returned, common.StreamClosedError)
    assert returned.reason == closed and returned.code is None


async def test_handles_of_one_client_share_a_producer_and_free_it(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream(Tokens, id="two-writers")
    other = client.stream(Tokens, id="two-writers")
    await stream.publish("1")
    await other.publish("2")
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    producers = {
        item.producer.id
        for item in items
        if isinstance(item, common.Record) and item.producer
    }
    assert len(producers) == 1
    # A handle that Python frees without a close frees its writer. So a server that makes a
    # handle per request doesn't keep a writer for each.
    del other
    gc.collect()
    assert len(extension.freed) == 1


async def test_rpc_timeout_bounds_each_call_and_not_the_read(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream(Tokens, id="slow")
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
        await stream.publish("late")

    # Each call of a waiting read returns before the deadline, so the read continues.
    reader = stream.subscribe(rpc_timeout=timedelta(milliseconds=300))
    publisher = asyncio.create_task(publish_later())
    item = await anext(reader)
    await reader.aclose()
    await publisher
    assert isinstance(item, common.Record) and item.data == "late"


class _CountingCodec(PayloadCodec):
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.calls.append(("encode", len(payloads)))
        return list(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        self.calls.append(("decode", len(payloads)))
        return list(payloads)


async def test_codec_runs_once_for_each_write_and_page(client: Client):
    # A remote codec costs a round trip for each call, so one call takes all the payloads of
    # a message.
    codec = _CountingCodec()
    config = client.config()
    config["data_converter"] = DataConverter(payload_codec=codec)
    client, _ = fake_client(Client(**config))
    stream = await client.create_stream(Tokens, id="coded")
    await stream.publish(*(str(n) for n in range(5)))
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, common.Record)] == [
        str(n) for n in range(5)
    ]
    assert codec.calls == [("encode", 5), ("decode", 5)]


async def test_forked_process_gets_its_own_client_producers(client: Client):
    # A forked child has the parent's key and count, so the fork hook replaces them.
    before = _stream_client._client_producer(client, None).id
    _stream_client._new_process_key()
    after = _stream_client._client_producer(client, None).id
    assert before != after
