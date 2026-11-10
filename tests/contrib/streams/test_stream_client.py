import asyncio
import dataclasses
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import streams, workflow
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import BufferedAppender, Client, Stream
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import DataConverter
from temporalio.streams.system import models
from temporalio.worker import Replayer
from tests.contrib.streams.fake_extension import fake_client
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


_BEGINNING = streams.StreamOffset(b"")


async def test_unknown_backend_raises_before_any_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(streams.StreamConfigError, match="no Stream backend redis"):
        await client.create_stream("chat-1", payload_type=Turn, backend="redis")
    with pytest.raises(streams.StreamConfigError, match="It has fake"):
        client.get_stream("chat-1", payload_type=Turn, backend="redis")
    with pytest.raises(streams.StreamConfigError, match="It has fake"):
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
    typed = client.get_stream(ref.id, payload_type=Turn, backend=ref.backend)
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


async def test_a_create_retry_with_the_same_request_id_succeeds(stream_client: Client):
    # A retry after a lost answer sends the same request id, so it isn't a conflict.
    [backend] = stream_client.config(active_config=True)["stream_backends"]
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


async def test_next_offset_resumes_after_the_last_item(stream_client: Client):
    stream = await stream_client.create_stream(
        f"resume-{uuid.uuid4()}", payload_type=str
    )
    last = await stream.append("a", "b")
    tokens = stream.read(wait_new_records=False)
    assert tokens.next_offset == _BEGINNING
    assert [item.data async for item in tokens if isinstance(item, streams.Record)] == [
        "a",
        "b",
    ]
    assert tokens.next_offset == last
    resume_from = tokens.next_offset
    new = stream.read(offset=streams.END, wait_new_records=False)
    assert new.next_offset is None
    assert [item async for item in new] == []
    assert new.next_offset == last
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


async def test_last_reads_the_newest_records_then_follows(stream_client: Client):
    stream = await stream_client.create_stream(f"last-{uuid.uuid4()}", payload_type=str)
    await stream.append("a", "b", "c")
    # A chat view shows the last few Records first, then waits for new ones.
    tail = stream.read(last=2)
    assert tail.next_offset is None
    shown = [await anext(tail), await anext(tail)]
    await stream.append("d")
    shown.append(await anext(tail))
    await tail.aclose()
    assert [item.data for item in shown if isinstance(item, streams.Record)] == [
        "b",
        "c",
        "d",
    ]
    newest = stream.read(last=1, wait_new_records=False)
    assert [item.data async for item in newest if isinstance(item, streams.Record)] == [
        "d"
    ]
    with pytest.raises(ValueError, match="no offset"):
        stream.read(last=2, offset=_BEGINNING)


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
    assert read.next_offset == bad
    resumed = reader.read(offset=read.next_offset, wait_new_records=False)
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
    # Retention is a backend setting now. safety_ttl keeps an open Stream, so only a closed
    # Stream expires, one Retention after its close.
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(retention=timedelta(milliseconds=200), default=True)
    ]
    one = Client(**config)
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
    # The earlier epoch's Records are real messages, so nothing withdraws them.
    items = [item async for item in stream.read(wait_new_records=False)]
    assert [item.kind for item in items] == [streams.ItemKind.RECORD] * 2
    with pytest.raises(ValueError):
        with streams.producer("ingest", epoch=0):
            pass


async def test_clients_of_one_runtime_share_the_backend_calls(stream_client: Client):
    # A service that makes a client for each tenant keeps one set of Redis connections.
    other = Client(**stream_client.config())
    [backend] = stream_client.config(active_config=True)["stream_backends"]
    assert backend._backend_for(other) is backend._backend_for(stream_client)
    stream = await other.create_stream(f"shared-{uuid.uuid4()}", payload_type=str)
    await stream.append("from the other client")
    again = stream_client.get_stream(stream.ref.id, payload_type=str)
    items = [item async for item in again.read(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "from the other client"
    ]


async def test_default_is_native_with_no_default_backend(client: Client):
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend()]
    one = Client(**config)
    with pytest.raises(streams.StreamConfigError, match="default is native"):
        one.get_stream("d-2", payload_type=str)
    with pytest.raises(streams.StreamConfigError, match="default is native"):
        await one.create_stream("d-2", payload_type=str)
    with pytest.raises(streams.StreamConfigError, match="default is native"):
        one.get_stream(streams.StreamRef(id="d-2"))
    on_memory = one.get_stream("d-2", payload_type=str, backend="memory")
    assert on_memory.ref.backend == "memory"
    created = await one.create_stream("d-2", payload_type=str, backend="memory")
    assert created.ref.backend == "memory"


async def test_stream_takes_the_named_backend_or_the_default(client: Client):
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(name="a", default=True),
        MemoryStreamBackend(name="b"),
    ]
    two = Client(**config)
    created = await two.create_stream("moving", payload_type=str, backend="b")
    await created.append("on b")
    on_b = two.get_stream("moving", payload_type=str, backend="b")
    assert on_b.ref == created.ref and await on_b.get_offset() > _BEGINNING
    assert two.get_stream("moving", payload_type=str).ref.backend == "a"
    # A ref with no backend name uses the default backend, which doesn't have the Stream.
    with pytest.raises(streams.StreamError) as missing:
        await two.get_stream(streams.StreamRef(id="moving")).get_offset()
    assert missing.value.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
    with pytest.raises(ValueError):
        two.get_stream("", payload_type=str)


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
    after_first = head.next_offset
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


async def test_registration_rules(client: Client):
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(),
        MemoryStreamBackend(name="b", default=True),
    ]
    two = Client(**config)
    assert (await two.create_stream("d-1", payload_type=str)).ref.backend == "b"
    assert two.get_stream("d-1", payload_type=str).ref.backend == "b"
    # A Worker serves only one backend, and so does a Replayer.
    with pytest.raises(streams.StreamConfigError, match="one backend"):
        new_worker(Client(**config), Agent)
    with pytest.raises(streams.StreamConfigError, match="one backend"):
        Replayer(workflows=[Agent], stream_backends=config["stream_backends"])
