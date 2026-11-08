import asyncio
import gc
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

import pytest

import temporalio.api.stream.v1
import temporalio.client
from temporalio import streams
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import StreamErrorReason, StreamIdConflictPolicy
from temporalio.api.stream.v1 import StreamErrorDetails
from temporalio.bridge.streams import StreamCallError
from temporalio.client import Client
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams import _errors, _stream_client
from tests.contrib.streams.fake_extension import fake_client


@dataclass
class Turn:
    kind: str
    text: str = ""


def _details(reason: StreamErrorReason.ValueType) -> bytes:
    return StreamErrorDetails(reason=reason).SerializeToString()


def _mapped(err: BaseException) -> streams.StreamError:
    mapped = _errors.from_failure(err)
    assert mapped is not None
    return mapped


def test_runtime_errors_map_to_stream_errors():
    fenced = StreamErrorReason.STREAM_ERROR_REASON_FENCED
    err = _mapped(
        StreamCallError(
            int(RPCStatusCode.FAILED_PRECONDITION), "fenced", _details(fenced)
        )
    )
    assert isinstance(err, streams.StreamFencedError)
    assert err.reason == fenced
    # A reason with no class of its own gives the base class, with the reason.
    for reason in (
        StreamErrorReason.STREAM_ERROR_REASON_SEQUENCE_TOO_LOW,
        StreamErrorReason.STREAM_ERROR_REASON_STORAGE_CORRUPT,
    ):
        other = _mapped(
            StreamCallError(int(RPCStatusCode.DATA_LOSS), "other", _details(reason))
        )
        assert type(other) is streams.StreamError and other.reason == reason
        assert other.non_retryable
    unknown = _mapped(StreamCallError(int(RPCStatusCode.INTERNAL), "odd", b""))
    assert type(unknown) is streams.StreamError
    assert unknown.reason == StreamErrorReason.STREAM_ERROR_REASON_UNSPECIFIED
    exists = _mapped(
        StreamCallError(
            int(RPCStatusCode.ALREADY_EXISTS),
            "exists",
            _details(StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS),
        )
    )
    assert isinstance(exists, streams.StreamAlreadyExistsError)


async def test_client_writes_reads_and_closes(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream("chat-42", payload_type=Turn)
    first = await stream.append(Turn("model"))
    second = await stream.append(Turn("user", "hello"))
    assert first < second
    assert await stream.get_offset() > second
    await stream.close()
    # A close writes no item. The read ends once it gave the last Record.
    items = [item async for item in stream.read()]
    assert [item.kind for item in items] == [streams.ItemKind.RECORD] * 2
    turn, token = items
    assert isinstance(turn, streams.Record) and turn.data == Turn("model")
    assert isinstance(token, streams.Record) and token.data == Turn("user", "hello")
    assert turn.producer is not None and turn.producer.epoch is not None
    assert isinstance(turn.producer.epoch.kind, streams.ProducerEpochKindClient)
    assert extension.namespaces == {client.namespace}

    resumed = [item async for item in stream.read(offset=token.offset)]
    assert [item.offset for item in resumed] == [token.offset]


async def test_create_fails_on_an_id_that_exists(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream("taken", payload_type=str)
    with pytest.raises(streams.StreamAlreadyExistsError):
        await client.create_stream("taken", payload_type=str)
    # USE_EXISTING gets the Stream instead of failing.
    got = await client.create_stream(
        "taken",
        payload_type=str,
        id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
    )
    assert got.ref == stream.ref
    policies = [create.id_conflict_policy for create in extension.creates]
    assert policies == [
        StreamIdConflictPolicy.STREAM_ID_CONFLICT_POLICY_FAIL,
        StreamIdConflictPolicy.STREAM_ID_CONFLICT_POLICY_FAIL,
        StreamIdConflictPolicy.STREAM_ID_CONFLICT_POLICY_USE_EXISTING,
    ]
    # Each create call sends its own request id, so a retry of one isn't another create.
    request_ids = [create.request_id for create in extension.creates]
    assert all(request_ids) and len(set(request_ids)) == 3


async def test_a_stream_gives_its_ref_and_its_wire_message(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream("chat-proto", payload_type=Turn)
    assert stream.ref == streams.StreamRef(id="chat-proto", backend="fake")
    assert stream.proto_ref == temporalio.api.stream.v1.StreamRef(
        id="chat-proto", backend="fake"
    )


async def test_read_waits_for_create(client: Client):
    client, _ = fake_client(client)
    stream = client.get_stream("tokens-7", payload_type=str)
    with pytest.raises(streams.StreamNotFoundError):
        async for _ in stream.read():
            pass

    async def create_later() -> None:
        created = await client.create_stream(stream.ref.id, payload_type=str)
        await created.append("five")
        await created.close()

    task = asyncio.create_task(create_later())
    items = [item async for item in stream.read(create_wait=timedelta(seconds=5))]
    await task
    assert [item.data for item in items if isinstance(item, streams.Record)] == ["five"]


async def test_a_waiting_read_ends_at_a_close(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream("closes-later", payload_type=str)
    reader = stream.read()

    async def read() -> list[str]:
        return [item.data async for item in reader if isinstance(item, streams.Record)]

    task = asyncio.create_task(read())
    await stream.append("last")
    await asyncio.sleep(0.05)
    await stream.close()
    assert await asyncio.wait_for(task, 5) == ["last"]


def test_stream_errors_fail_the_workflow_not_the_task():
    closed = StreamErrorReason.STREAM_ERROR_REASON_CLOSED
    err = streams.StreamClosedError("closed")
    assert isinstance(err, ApplicationError)
    assert err.non_retryable and err.type == "StreamClosedError"
    assert err.reason == closed
    # A Store outage is the one error that a retry can fix. Its reason, not a class, marks it.
    outage = streams.StreamError(
        "down", reason=StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE
    )
    assert not outage.non_retryable
    mapped = _mapped(
        StreamCallError(
            int(RPCStatusCode.FAILED_PRECONDITION), "closed", _details(closed)
        )
    )
    assert isinstance(mapped, streams.StreamClosedError) and mapped.reason == closed
    # A Workflow await gets the error from a Local Activity, with the reason in its details.
    returned = _mapped(
        ApplicationError(
            "closed", StreamErrorDetails(reason=closed), type="StreamClosedError"
        )
    )
    assert isinstance(returned, streams.StreamClosedError)
    assert returned.reason == closed
    # An error with no Stream reason in it isn't a Stream error.
    assert _errors.from_failure(ValueError("other")) is None


async def test_streams_of_one_client_share_a_producer_and_free_it(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream("two-writers", payload_type=str)
    other = client.get_stream("two-writers", payload_type=str)
    await stream.append("1")
    await other.append("2")
    items = [item async for item in stream.read(wait_new_records=False)]
    producers = {
        item.producer.id
        for item in items
        if isinstance(item, streams.Record) and item.producer
    }
    assert len(producers) == 1
    # A Stream that Python frees without a close frees its writer. So a server that makes a
    # Stream per request doesn't keep a writer for each.
    del other
    gc.collect()
    assert len(extension.freed) == 1


async def test_rpc_timeout_bounds_each_call_and_not_the_read(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream("slow", payload_type=str)
    call = extension.call

    async def slow_describe(method: str, input: bytes) -> bytes:
        if method == "DescribeStream":
            await asyncio.sleep(1)
        return await call(method, input)

    setattr(extension, "call", slow_describe)
    with pytest.raises(RPCError) as err:
        await stream.get_offset(rpc_timeout=timedelta(milliseconds=100))
    assert err.value.status == RPCStatusCode.DEADLINE_EXCEEDED

    async def append_later() -> None:
        await asyncio.sleep(1)
        await stream.append("late")

    # Each call of a waiting read returns before the deadline, so the read continues.
    reader = stream.read(rpc_timeout=timedelta(milliseconds=300))
    appender = asyncio.create_task(append_later())
    item = await anext(reader)
    await reader.aclose()
    await appender
    assert isinstance(item, streams.Record) and item.data == "late"


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
    stream = await client.create_stream("coded", payload_type=str)
    await stream.append(*(str(n) for n in range(5)))
    items = [item async for item in stream.read(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        str(n) for n in range(5)
    ]
    assert codec.calls == [("encode", 5), ("decode", 5)]


async def test_forked_process_gets_its_own_client_producers(client: Client):
    # A forked child has the parent's key and count, so the fork hook replaces them.
    before = _stream_client._client_producer(client, None).id
    _stream_client._new_process_key()
    after = _stream_client._client_producer(client, None).id
    assert before != after


async def test_client_streams_are_of_the_exported_class(client: Client):
    client, _ = fake_client(client)
    stream = client.get_stream("exported", payload_type=str)
    assert isinstance(stream, temporalio.client.Stream)
    assert "Stream" in temporalio.client.__all__


def test_import_of_temporalio_client_loads_no_stream_code():
    code = "import sys, temporalio.client; print('temporalio.streams' in sys.modules)"
    loaded = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert loaded.strip() == "False"


async def test_bad_create_and_read_arguments_raise_with_no_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(ValueError, match="id is empty"):
        await client.create_stream("", payload_type=str)
    stream = client.get_stream("args", payload_type=str)
    with pytest.raises(ValueError, match="from 1 to 1000"):
        stream.read(last=1001)
    assert not extension.creates and not extension.namespaces


class _Interrupt(BaseException):
    pass


async def test_block_end_closes_on_any_exception(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream("interrupted", payload_type=str)
    with pytest.raises(_Interrupt):
        async with stream:
            raise _Interrupt
    assert extension.streams["interrupted"].closed


async def test_a_failed_close_is_tried_again(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream("closes-twice", payload_type=str)
    await stream.append("a")
    call = extension.call
    refused: list[str] = []

    async def refuses_the_first_close(method: str, input: bytes) -> bytes:
        if method == "CloseStream" and not refused:
            refused.append(method)
            raise StreamCallError(
                int(RPCStatusCode.UNAVAILABLE),
                "down",
                _details(StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE),
            )
        return await call(method, input)

    setattr(extension, "call", refuses_the_first_close)
    with pytest.raises(streams.StreamError) as failed:
        async with stream:
            pass
    assert (
        failed.value.reason == StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE
    )
    # The writer closed before the Store refused, so this object writes no more.
    with pytest.raises(streams.StreamClosedError, match="closed its writer"):
        stream.append("late")
    # The Stream is still open, and the next close goes through.
    assert not extension.streams["closes-twice"].closed
    await stream.close()
    assert extension.streams["closes-twice"].closed
