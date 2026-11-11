import asyncio
import dataclasses
import gc
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import streams, workflow
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import BufferedAppender, Client, Stream
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import (
    DataConverter,
    StorageDriverActivityInfo,
    StorageDriverWorkflowInfo,
)
from temporalio.streams._extension import ExtensionStreamBackend
from temporalio.streams.superseded import Superseded, with_superseded
from temporalio.streams.system import models
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer
from tests.contrib.streams._support import only_backend
from tests.contrib.streams.fake_extension import fake_client
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


_BEGINNING = streams.StreamOffset(b"")


def _client(client: Client, stream_config: streams.StreamConfig) -> Client:
    config = client.config()
    config["stream_config"] = stream_config
    return Client(**config)


async def test_ref_with_an_unknown_backend_raises_before_any_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(
        streams.StreamConfigError, match="no Stream backend redis. It has fake"
    ):
        client.get_stream(streams.StreamRef(id="chat-1", backend="redis"))
    assert not extension.streams and not extension.namespaces
    stream = await client.create_stream("chat-1", payload_type=Turn)
    assert stream.ref.backend == "fake"


async def test_client_without_backend_has_no_transport(client: Client):
    with pytest.raises(streams.StreamConfigError, match="no Stream backend"):
        client.get_stream("chat-1", payload_type=Turn)
    with pytest.raises(streams.StreamConfigError, match="no Stream backend"):
        await client.create_stream("chat-1", payload_type=Turn)


async def test_get_stream_gives_the_stream_of_a_ref(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream("chat-2", payload_type=Turn)
    converter = DataConverter.default.payload_converter
    ref = converter.from_payload(
        converter.to_payload(stream.ref), streams.StreamRef[Turn]
    )
    assert ref == stream.ref
    typed = client.get_stream(ref.id, payload_type=Turn)
    assert typed.ref == ref and await typed.get_offset() == _BEGINNING
    await typed.append(Turn("model", "hi"))
    [record] = [r async for r in typed.read(wait_new_records=False)]
    assert isinstance(record, streams.Record) and record.data == Turn("model", "hi")
    # With no payload type, a read decodes each Record as a JSON value.
    untyped = client.get_stream(ref)
    [raw] = [r async for r in untyped.read(wait_new_records=False)]
    assert isinstance(raw, streams.Record) and raw.data == {
        "kind": "model",
        "text": "hi",
    }


async def test_a_call_with_no_type_argument_reads_json_values(client: Client):
    client, _ = fake_client(client)
    typed = await client.create_stream("chat-3", payload_type=Turn)
    await typed.append(Turn("model", "hi"))
    untyped: Any = client.get_stream("chat-3")
    [raw] = [r async for r in untyped.read(wait_new_records=False)]
    assert raw.data == {"kind": "model", "text": "hi"}


async def test_append_starts_the_write_at_the_call(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream("ordered", payload_type=str)
    futures = [stream.append(str(n)) for n in range(3)]
    # A cancel of a future doesn't stop its write, which already has its place.
    futures[1].cancel()
    stream.append("3")
    await stream.close()
    items = [item async for item in stream.read()]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "0",
        "1",
        "2",
        "3",
    ]
    assert [await futures[0], await futures[2]] == [items[0].offset, items[2].offset]
    with pytest.raises(streams.StreamError) as closed:
        stream.append("4")
    assert closed.value.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED


async def test_next_call_raises_a_write_error_that_no_caller_got(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream("lost", payload_type=str)
    await client.get_stream("lost", payload_type=str).close()
    late = stream.append("late")
    await asyncio.wait([late])
    with pytest.raises(streams.StreamError) as closed:
        await stream.get_offset()
    assert closed.value.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED
    # The call owns the error, so the next one makes its call.
    assert await stream.get_offset() == _BEGINNING


@workflow.defn
class HandsOutStream:
    @workflow.run
    async def run(self) -> dict[str, str]:
        return {"id": "handed-out"}

    @workflow.query
    def stream(self) -> dict[str, str]:
        # The proto JSON of a ref, as a Workflow's Stream that a query returns.
        return {"id": "handed-out"}


async def test_query_and_workflow_results_give_refs(client: Client):
    client, _ = fake_client(client)
    created = await client.create_stream("handed-out", payload_type=str)
    async with new_worker(client, HandsOutStream) as worker:
        # By name, since the method's own return type is the JSON.
        handle = await client.start_workflow(
            "HandsOutStream",
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            result_type=streams.StreamRef[str],
        )
        result = await handle.result()
        queried = await handle.query("stream", result_type=streams.StreamRef[str])
    # The ref has no backend, so its Stream uses the default one.
    assert queried.id == "handed-out" and queried.backend is None
    await client.get_stream(queried).append("from the query result")
    await client.get_stream(result).append("from the Workflow result")
    items = [item async for item in created.read(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "from the query result",
        "from the Workflow result",
    ]


async def test_async_with_closes_a_client_stream(stream_client: Client):
    async with await stream_client.create_stream(
        f"block-{uuid.uuid4()}", payload_type=str
    ) as done:
        await done.append("a")
    failed = await stream_client.create_stream(
        f"block-{uuid.uuid4()}", payload_type=str
    )
    with pytest.raises(ValueError):
        async with failed:
            raise ValueError("turn failed")
    early = await stream_client.create_stream(f"block-{uuid.uuid4()}", payload_type=str)
    async with early:
        await early.close()
    cancelled = await stream_client.create_stream(
        f"block-{uuid.uuid4()}", payload_type=str
    )
    with pytest.raises(asyncio.CancelledError):
        async with cancelled:
            raise asyncio.CancelledError()
    for stream in (done, failed, early, cancelled):
        # A read of a closed Stream ends once it gave the last item, with no wait.
        items = await asyncio.wait_for(
            _records(stream_client.get_stream(stream.ref.id, payload_type=str)), 5
        )
        assert items == (["a"] if stream is done else [])
        with pytest.raises(streams.StreamError) as closed:
            await stream_client.get_stream(stream.ref.id, payload_type=str).append(
                "late"
            )
        assert closed.value.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED


async def _records(stream: Stream[Any]) -> list[Any]:
    return [r.data async for r in stream.read() if isinstance(r, streams.Record)]


async def test_create_fails_on_a_stream_that_exists(stream_client: Client):
    stream_id = f"again-{uuid.uuid4()}"
    first = await stream_client.create_stream(stream_id, payload_type=str)
    await first.append("a")
    with pytest.raises(streams.StreamError) as exists:
        await stream_client.create_stream(stream_id, payload_type=str)
    assert exists.value.reason == StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS
    # USE_EXISTING returns the Stream instead of failing.
    again = await stream_client.create_stream(
        stream_id,
        payload_type=str,
        id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
    )
    assert again.ref == first.ref
    again_offset = await again.get_offset()
    assert again_offset == await first.get_offset()
    assert again_offset > _BEGINNING
    await first.close()
    # A closed Stream fails both policies with CLOSED, which comes before ALREADY_EXISTS.
    for policy in (
        streams.StreamIDConflictPolicy.FAIL,
        streams.StreamIDConflictPolicy.USE_EXISTING,
    ):
        with pytest.raises(streams.StreamError) as closed:
            await stream_client.create_stream(
                stream_id, payload_type=str, id_conflict_policy=policy
            )
        assert closed.value.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED


async def test_a_write_with_an_incomplete_producer_is_refused(stream_client: Client):
    # The SDK always sends a complete epoch. A Producer that a caller builds by hand can lack one.
    stream = await stream_client.create_stream(
        f"incomplete-{uuid.uuid4()}", payload_type=str
    )
    service = (
        only_backend(stream_client)
        ._backend_for(stream_client)
        .service(stream_client.namespace)
    )
    producer = models.Producer(
        id="client:incomplete",
        kind=models.ProducerKindClient(
            models.ClientProducer(epoch=models.ClientEpoch(attempt=0))
        ),
    )
    with pytest.raises(streams.StreamError) as refused:
        await service.send_append_records(
            models.AppendRecordsRequest(
                ref=models.StreamRef(id=stream.ref.id),
                producer=producer,
                records=[models.AppendRecord(body="a")],
            )
        )
    assert refused.value.reason == StreamErrorReason.STREAM_ERROR_REASON_INVALID


async def test_a_writer_open_carries_the_namespace(client: Client):
    client, extension = fake_client(client)
    stream = await client.create_stream("namespaced", payload_type=str)
    await stream.append("a")
    assert extension.writer_namespaces == [client.namespace]


def _writer_calls(client: Client) -> Any:
    backend: Any = only_backend(client)._backend_for(client)
    return backend


def _named_producer(name: str) -> models.Producer:
    return models.Producer(
        id=f"client:{name}",
        kind=models.ProducerKindClient(
            models.ClientProducer(epoch=models.ClientEpoch(attempt=1))
        ),
    )


async def _records_of(stream: Stream[str]) -> list[str]:
    items = stream.read(wait_new_records=False)
    return [item.data async for item in items if isinstance(item, streams.Record)]


async def test_a_write_after_a_writer_close_raises_closed(stream_client: Client):
    stream = await stream_client.create_stream(
        f"writer-close-{uuid.uuid4()}", payload_type=str
    )
    writers = _writer_calls(stream_client).writers(
        stream_client.namespace, stream_client.data_converter
    )
    writer = await writers.open_writer(
        ref=models.StreamRef(id=stream.ref.id),
        producer=_named_producer(str(uuid.uuid4())),
    )
    await writer.write_records([models.AppendRecord(body="a")])
    await writer.close()
    with pytest.raises(streams.StreamError) as closed:
        await writer.write_records([models.AppendRecord(body="b")])
    assert closed.value.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED
    assert await _records_of(stream) == ["a"]


async def test_a_writer_that_python_frees_is_released(stream_client: Client):
    stream = await stream_client.create_stream(
        f"writer-freed-{uuid.uuid4()}", payload_type=str
    )
    calls = _writer_calls(stream_client)
    writers = calls.writers(stream_client.namespace, stream_client.data_converter)
    producer = _named_producer(str(uuid.uuid4()))
    ref = models.StreamRef(id=stream.ref.id)
    first = await writers.open_writer(ref=ref, producer=producer)
    await first.write_records([models.AppendRecord(body="a")])
    opened = len(calls._writers)
    # No close. Dropping the last reference releases the crate's writer.
    del first
    gc.collect()
    assert len(calls._writers) == opened - 1
    # A new open of the same epoch goes on in order after the released one.
    second = await writers.open_writer(ref=ref, producer=producer)
    await second.write_records([models.AppendRecord(body="b")])
    await second.close()
    assert await _records_of(stream) == ["a", "b"]


async def test_backend_shutdown_closes_the_open_writers(stream_client: Client):
    stream = await stream_client.create_stream(
        f"shutdown-writers-{uuid.uuid4()}", payload_type=str
    )
    await stream.append("before")
    backend = only_backend(stream_client)
    assert isinstance(backend, ExtensionStreamBackend)
    await backend.shutdown()
    # The Stream's writer is closed, so its next write fails, and the Stream stays open.
    with pytest.raises(streams.StreamError) as closed:
        await stream.append("after")
    assert closed.value.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED
    assert await _records_of(stream) == ["before"]


async def test_a_create_retry_with_the_same_request_id_succeeds(stream_client: Client):
    # A retry after a lost answer sends the same request id, so it isn't a conflict.
    backend = only_backend(stream_client)
    service = backend._backend_for(stream_client).service(stream_client.namespace)
    request = models.CreateStreamRequest(
        stream_ref=models.StreamRef(id=f"retry-{uuid.uuid4()}"), request_id="create-1"
    )
    await service._stream_service_create_stream(request, object)
    await service._stream_service_create_stream(request, object)
    with pytest.raises(streams.StreamError) as exists:
        await service._stream_service_create_stream(
            dataclasses.replace(request, request_id="create-2"), object
        )
    assert exists.value.reason == StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS


async def test_append_to_a_stream_that_nobody_created_raises(stream_client: Client):
    stream = stream_client.get_stream(f"missing-{uuid.uuid4()}", payload_type=str)
    with pytest.raises(streams.StreamError) as missing:
        await stream.append("lost")
    assert missing.value.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND


async def test_last_offset_resumes_after_the_last_item(stream_client: Client):
    stream = await stream_client.create_stream(
        f"resume-{uuid.uuid4()}", payload_type=str
    )
    last = await stream.append("a", "b")
    tokens = stream.read(wait_new_records=False)
    assert tokens.last_offset == _BEGINNING
    assert [item.data async for item in tokens if isinstance(item, streams.Record)] == [
        "a",
        "b",
    ]
    assert tokens.last_offset == last
    resume_from = tokens.last_offset
    new = stream.read(offset=streams.END, wait_new_records=False)
    assert new.last_offset is None
    assert [item async for item in new] == []
    assert new.last_offset == last
    await stream.append("c")
    resumed = stream.read(offset=resume_from, wait_new_records=False)
    assert [
        item.data async for item in resumed if isinstance(item, streams.Record)
    ] == ["c"]


async def test_a_waiting_read_ends_when_the_stream_closes(stream_client: Client):
    stream = await stream_client.create_stream(
        f"wakes-{uuid.uuid4()}", payload_type=str
    )
    await stream.append("a")
    reader = asyncio.create_task(
        _records(stream_client.get_stream(stream.ref.id, payload_type=str))
    )
    await asyncio.sleep(0.2)
    assert not reader.done()
    await stream.close()
    assert await asyncio.wait_for(reader, 10) == ["a"]
    # A read from the end of a closed Stream has no item left, so it answers at once.
    tail = stream_client.get_stream(stream.ref.id, payload_type=str).read(
        offset=streams.END
    )
    assert await asyncio.wait_for(_items(tail), 5) == []


async def _items(read: streams.Reader[Any]) -> list[Any]:
    return [item async for item in read]


async def test_payload_type_of_the_call_decodes_each_value(stream_client: Client):
    stream = await stream_client.create_stream(
        f"typed-{uuid.uuid4()}", payload_type=Turn
    )
    await stream.append(Turn("model"))
    turns = stream_client.get_stream(stream.ref.id, payload_type=Turn)
    dicts = stream_client.get_stream(stream.ref.id, payload_type=dict[str, Any])
    for reader, expected in (
        (turns, Turn("model")),
        (dicts, {"kind": "model", "text": ""}),
    ):
        read = reader.read(wait_new_records=False)
        assert [r.data async for r in read if isinstance(r, streams.Record)] == [
            expected
        ]


async def test_buffered_appender_keeps_the_append_order(stream_client: Client):
    stream = await stream_client.create_stream(
        f"buffered-{uuid.uuid4()}", payload_type=Turn
    )
    async with BufferedAppender(stream) as appender:
        appender.append(Turn("user"))
        appender.append(Turn("a"), Turn("b"))
        appender.append(Turn("model"))
    written = [
        item.data
        async for item in stream.read(wait_new_records=False)
        if isinstance(item, streams.Record)
    ]
    assert written == [Turn("user"), Turn("a"), Turn("b"), Turn("model")]


async def test_record_that_fails_to_decode_raises_and_resumes_past_it(
    stream_client: Client,
):
    stream_id = f"poison-{uuid.uuid4()}"
    writer = await stream_client.create_stream(stream_id, payload_type=str)
    bad = await writer.append("not a turn")
    reader = stream_client.get_stream(stream_id, payload_type=Turn)
    await reader.append(Turn("after"))
    read = reader.read(wait_new_records=False)
    with pytest.raises(streams.StreamError, match=f"offset {bad}"):
        await anext(read)
    assert read.last_offset == bad
    resumed = reader.read(offset=read.last_offset, wait_new_records=False)
    assert [
        item.data async for item in resumed if isinstance(item, streams.Record)
    ] == [Turn("after")]


async def test_record_over_the_store_limit_raises_too_large(stream_client: Client):
    # The client's writer splits a batch at the Store's limit, so only one Record can be over it.
    stream = await stream_client.create_stream(
        f"large-{uuid.uuid4()}", payload_type=str
    )
    with pytest.raises(streams.StreamError) as refused:
        await stream.append("x" * 2_000_000)
    assert refused.value.reason == StreamErrorReason.STREAM_ERROR_REASON_TOO_LARGE
    assert refused.value.non_retryable
    assert await stream.get_offset() == _BEGINNING


async def test_create_of_an_expired_stream_raises(client: Client):
    # Retention is a backend setting. With no last_write_ttl an open Stream doesn't expire, so
    # only a closed Stream expires, one Retention after its close.
    one = _client(
        client,
        streams.StreamConfig(
            backends=[MemoryStreamBackend(retention=timedelta(milliseconds=200))]
        ),
    )
    stream_id = f"expired-{uuid.uuid4()}"
    stream = await one.create_stream(stream_id, payload_type=str)
    await stream.append("old")
    await stream.close()
    await asyncio.sleep(0.5)
    with pytest.raises(streams.StreamError) as expired:
        await one.create_stream(
            stream_id,
            payload_type=str,
            id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
        )
    assert expired.value.reason == StreamErrorReason.STREAM_ERROR_REASON_EXPIRED


@pytest.mark.parametrize("store", ["memory", "redis"])
async def test_last_write_ttl_expires_only_an_open_stream_that_sets_it(
    client: Client, store: str, request: pytest.FixtureRequest
):
    def backend(ttl: timedelta | None) -> streams.StreamBackend:
        if store == "memory":
            return MemoryStreamBackend(last_write_ttl=ttl)
        redis_url = request.getfixturevalue("redis_url")
        return RedisStreamBackend(
            redis_url, key_prefix=f"t-{uuid.uuid4()}", last_write_ttl=ttl
        )

    ttl = timedelta(milliseconds=300)
    with_ttl = _client(client, streams.StreamConfig(backends=[backend(ttl)]))
    without = _client(client, streams.StreamConfig(backends=[backend(None)]))
    stream_id = f"ttl-{uuid.uuid4()}"
    expiring = await with_ttl.create_stream(stream_id, payload_type=str)
    kept = await without.create_stream(stream_id, payload_type=str)
    await expiring.append("a")
    await kept.append("a")
    await asyncio.sleep(1)
    with pytest.raises(streams.StreamError) as expired:
        await with_ttl.create_stream(
            stream_id,
            payload_type=str,
            id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
        )
    assert expired.value.reason == StreamErrorReason.STREAM_ERROR_REASON_EXPIRED
    await kept.append("b")


async def test_named_producer_fences_its_earlier_epoch(stream_client: Client):
    stream = await stream_client.create_stream(
        f"named-{uuid.uuid4()}", payload_type=str
    )
    # A server that restarts writes as the same name with a higher epoch, so the zombie that it
    # replaces can't write after it.
    with streams.producer("ingest", epoch=1):
        await stream.append("before restart")
    with streams.producer("ingest", epoch=2):
        await stream.append("after restart")
    with streams.producer("ingest", epoch=1):
        with pytest.raises(streams.StreamError) as fenced:
            await stream.append("zombie")
        assert fenced.value.reason == StreamErrorReason.STREAM_ERROR_REASON_FENCED
    # A plain read keeps both Records. A wrapped read withdraws the lower epoch's Record, as for
    # every Producer.
    items = [item async for item in stream.read(wait_new_records=False)]
    assert [item.kind for item in items] == [streams.ItemKind.RECORD] * 2
    wrapped = [
        item async for item in with_superseded(stream.read(wait_new_records=False))
    ]
    [superseded] = [item for item in wrapped if isinstance(item, Superseded)]
    assert superseded.withdrawn_offsets == (items[0].offset,)
    assert wrapped.index(superseded) == 1
    with pytest.raises(ValueError):
        with streams.producer("ingest", epoch=0):
            pass


async def test_clients_of_one_runtime_share_the_backend_calls(stream_client: Client):
    # A service that makes a client for each tenant keeps one set of Redis connections.
    other = Client(**stream_client.config())
    backend = only_backend(stream_client)
    assert backend._backend_for(other) is backend._backend_for(stream_client)
    stream = await other.create_stream(f"shared-{uuid.uuid4()}", payload_type=str)
    await stream.append("from the other client")
    again = stream_client.get_stream(stream.ref.id, payload_type=str)
    items = [item async for item in again.read(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "from the other client"
    ]


async def test_two_namespaces_on_one_backend_share_a_stream_id(stream_client: Client):
    # The Store keys a Stream by its id alone, so a namespace keeps its Streams apart only with a
    # backend or a key prefix of its own.
    config = stream_client.config()
    config["namespace"] = f"other-{uuid.uuid4()}"
    other = Client(**config)
    stream_id = f"shared-{uuid.uuid4()}"
    stream = await stream_client.create_stream(stream_id, payload_type=str)
    await stream.append("from default")
    seen = other.get_stream(stream_id, payload_type=str)
    assert [
        item.data
        async for item in seen.read(wait_new_records=False)
        if isinstance(item, streams.Record)
    ] == ["from default"]
    with pytest.raises(streams.StreamError) as exists:
        await other.create_stream(stream_id, payload_type=str)
    assert exists.value.reason == StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS


async def test_one_backend_needs_no_selector(client: Client):
    one = _client(client, streams.StreamConfig(backends=[MemoryStreamBackend()]))
    created = await one.create_stream(f"one-{uuid.uuid4()}", payload_type=str)
    assert created.ref.backend == "memory"
    await created.append("hi")
    assert one.get_stream(created.ref.id, payload_type=str).ref == created.ref
    # A ref with no backend name goes where a get by id goes.
    assert await one.get_stream(streams.StreamRef(id=created.ref.id)).get_offset() > (
        _BEGINNING
    )


async def test_selector_routes_by_id(client: Client):
    a, b = MemoryStreamBackend(name="a"), MemoryStreamBackend(name="b")
    contexts: list[streams.StreamSelectContext] = []

    def select(ctx: streams.StreamSelectContext) -> streams.StreamBackend:
        contexts.append(ctx)
        return a if ctx.id.startswith("legacy-") else b

    two = _client(
        client, streams.StreamConfig(backends=[a, b], backend_selector=select)
    )
    legacy = await two.create_stream("legacy-1", payload_type=str)
    new = await two.create_stream("chat-1", payload_type=str)
    assert (legacy.ref.backend, new.ref.backend) == ("a", "b")
    await new.append("on b")
    by_id = two.get_stream("chat-1", payload_type=str)
    assert by_id.ref == new.ref and await by_id.get_offset() > _BEGINNING
    assert two.get_stream("legacy-1", payload_type=str).ref.backend == "a"
    # A plain client call has no target.
    assert [(c.id, c.target) for c in contexts] == [
        ("legacy-1", None),
        ("chat-1", None),
        ("chat-1", None),
        ("legacy-1", None),
    ]
    with pytest.raises(ValueError):
        two.get_stream("", payload_type=str)


async def test_ref_finds_its_backend_by_name(client: Client):
    a, b = MemoryStreamBackend(name="a"), MemoryStreamBackend(name="b")
    calls: list[str] = []

    def select(ctx: streams.StreamSelectContext) -> streams.StreamBackend:
        calls.append(ctx.id)
        return b

    # The Stream is on a, where this selector never sends a Stream.
    on_a = await _client(client, streams.StreamConfig(backends=[a])).create_stream(
        f"on-a-{uuid.uuid4()}", payload_type=str
    )
    await on_a.append("on a")
    two = _client(
        client, streams.StreamConfig(backends=[a, b], backend_selector=select)
    )
    found = two.get_stream(on_a.ref)
    assert found.ref.backend == "a" and await found.get_offset() > _BEGINNING
    assert not calls


async def test_selector_gets_the_target_that_external_storage_gets(client: Client):
    # An Activity of a Workflow gets the Workflow, and a standalone Activity gets itself.
    a, b = MemoryStreamBackend(name="a"), MemoryStreamBackend(name="b")
    targets: list[object] = []

    def select(ctx: streams.StreamSelectContext) -> streams.StreamBackend:
        targets.append(ctx.target)
        return a

    two = _client(
        client, streams.StreamConfig(backends=[a, b], backend_selector=select)
    )

    async def create() -> None:
        await two.create_stream(f"in-activity-{uuid.uuid4()}", payload_type=str)

    await ActivityEnvironment().run(create)
    standalone = ActivityEnvironment()
    standalone.info = dataclasses.replace(
        standalone.info,
        workflow_id=None,
        workflow_namespace=None,
        workflow_run_id=None,
        workflow_type=None,
        activity_run_id="activity-run",
    )
    await standalone.run(create)
    assert targets == [
        StorageDriverWorkflowInfo(
            namespace="default", id="test", run_id="test-run", type="test"
        ),
        StorageDriverActivityInfo(
            namespace="default", id="test", run_id="activity-run", type="unknown"
        ),
    ]


@workflow.defn
class WriteToStream:
    @workflow.run
    async def run(self, ref: streams.StreamRef[Turn]) -> None:
        await workflow.get_stream(ref).append(Turn("workflow"))


async def test_client_stream_takes_workflow_writes(stream_client: Client):
    stream = await stream_client.create_stream(
        f"chat-{uuid.uuid4()}", payload_type=Turn
    )
    assert await stream.get_offset() == _BEGINNING
    first = await stream.append(Turn("client"))
    last = await stream.append(Turn("a"), Turn("b"))
    assert last > first
    async with new_worker(stream_client, WriteToStream) as worker:
        await stream_client.execute_workflow(
            WriteToStream.run,
            stream.ref,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    await stream.close()
    items = [item async for item in stream.read()]
    data = [item.data for item in items if isinstance(item, streams.Record)]
    assert data == [Turn("client"), Turn("a"), Turn("b"), Turn("workflow")]

    # Another Stream of the same id resumes after the first Record.
    again = stream_client.get_stream(stream.ref.id, payload_type=Turn)
    head = again.read()
    await anext(head)
    after_first = head.last_offset
    await head.aclose()
    assert after_first == first
    resumed = [
        item.data
        async for item in again.read(offset=after_first)
        if isinstance(item, streams.Record)
    ]
    assert resumed == [Turn("a"), Turn("b"), Turn("workflow")]


@workflow.defn
class Agent:
    def __init__(self) -> None:
        self._tokens: workflow.WorkflowStream[str] | None = None

    @workflow.run
    async def run(self, prompt: str) -> str:
        await workflow.sleep(timedelta(milliseconds=200))
        self._tokens = stream = await workflow.create_stream(
            f"{workflow.info().workflow_id}-tokens", payload_type=str
        )
        await stream.append(*prompt.split())
        await stream.close()
        return prompt.upper()

    @workflow.query
    def stream(self) -> streams.StreamRef[str] | None:
        # A query has no ref before the run creates the Stream.
        return self._tokens.ref if self._tokens else None


async def test_client_reads_a_workflow_stream(stream_client: Client):
    async with new_worker(stream_client, Agent) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        ref = await handle.query(Agent.stream)
        while ref is None:
            await asyncio.sleep(0.05)
            ref = await handle.query(Agent.stream)
        reader = stream_client.get_stream(ref)
        items = [item async for item in reader.read()]
        await handle.result()
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "hi",
        "there",
    ]
    assert reader.ref.backend is not None


async def test_empty_stream_id_raises_with_no_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(ValueError, match="id is empty"):
        client.get_stream("", payload_type=Turn)
    assert not extension.namespaces


async def test_worker_and_replayer_serve_one_backend(client: Client):
    a, b = MemoryStreamBackend(name="a"), MemoryStreamBackend(name="b")
    two = streams.StreamConfig(backends=[a, b], backend_selector=lambda ctx: b)
    with pytest.raises(streams.StreamConfigError, match="one backend"):
        new_worker(_client(client, two), Agent)
    with pytest.raises(streams.StreamConfigError, match="one backend"):
        Replayer(workflows=[Agent], stream_config=two)
