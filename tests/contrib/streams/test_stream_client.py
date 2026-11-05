import asyncio
import dataclasses
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import streams, workflow
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import Client, Stream
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


async def test_unknown_backend_raises_before_any_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(streams.StreamConfigError, match="no Stream backend redis"):
        await client.create_stream[Turn]("chat-1", backend="redis")
    with pytest.raises(streams.StreamConfigError, match="It has fake"):
        client.get_stream[Turn]("chat-1", backend="redis")
    with pytest.raises(streams.StreamConfigError, match="It has fake"):
        client.to_stream(streams.StreamRef(id="chat-1", backend="redis"))
    assert not extension.streams and not extension.namespaces
    stream = await client.create_stream[Turn]("chat-1")
    assert stream.ref.backend == "fake"


async def test_client_without_backend_has_no_transport(client: Client):
    with pytest.raises(streams.StreamConfigError, match="no Stream backend"):
        client.get_stream[Turn]("chat-1")
    with pytest.raises(streams.StreamConfigError, match="no Stream backend"):
        await client.create_stream[Turn]("chat-1")


async def test_to_stream_gives_the_stream_of_a_ref(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream[Turn]("chat-2")
    converter = DataConverter.default.payload_converter
    ref = converter.from_payload(
        converter.to_payload(stream.ref), streams.StreamRef[Turn]
    )
    assert ref == stream.ref
    typed = client.to_stream(ref, payload_type=Turn)
    assert typed.ref is ref and await typed.get_offset() == 0
    await typed.publish(Turn("model", "hi"))
    [record] = [r async for r in typed.subscribe(wait_new_records=False)]
    assert isinstance(record, streams.Record) and record.data == Turn("model", "hi")
    # With no payload type, a read decodes each Record as a JSON value.
    untyped = client.to_stream(ref)
    [raw] = [r async for r in untyped.subscribe(wait_new_records=False)]
    assert isinstance(raw, streams.Record) and raw.data == {
        "kind": "model",
        "text": "hi",
    }


async def test_a_call_with_no_type_argument_reads_json_values(client: Client):
    client, _ = fake_client(client)
    typed = await client.create_stream[Turn]("chat-3")
    await typed.publish(Turn("model", "hi"))
    untyped: Any = client.get_stream("chat-3")
    [raw] = [r async for r in untyped.subscribe(wait_new_records=False)]
    assert raw.data == {"kind": "model", "text": "hi"}


async def test_publish_starts_the_write_at_the_call(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream[str]("ordered")
    futures = [stream.publish(str(n)) for n in range(3)]
    # A cancel of a future doesn't stop its write, which already has its place.
    futures[1].cancel()
    stream.publish("3")
    await stream.close()
    items = [item async for item in stream.subscribe()]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "0",
        "1",
        "2",
        "3",
    ]
    assert [await futures[0], await futures[2]] == [items[0].offset, items[2].offset]
    with pytest.raises(streams.StreamClosedError):
        stream.publish("4")


async def test_next_call_raises_a_write_error_that_no_caller_got(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream[str]("lost")
    await client.get_stream[str]("lost").close()
    late = stream.publish("late")
    await asyncio.wait([late])
    with pytest.raises(streams.StreamClosedError):
        await stream.get_offset()
    # The call owns the error, so the next one makes its call.
    assert await stream.get_offset() == 0


async def test_a_client_cant_close_a_stream_bound_to_a_workflow(client: Client):
    client, extension = fake_client(client)
    await client.create_stream[str]("bound")
    owner = streams.Execution(
        type=streams.ExecutionType.ExecutionTypeWorkflow,
        business_id="wf",
        run_id="run",
    )
    ref: streams.StreamRef[str] = streams.StreamRef(id="bound", owner=owner)
    bound = client.to_stream(ref)
    with pytest.raises(streams.StreamNotOwnerError):
        await bound.close()
    with pytest.raises(streams.StreamNotOwnerError):
        async with bound:
            pass
    assert not extension.streams["bound"].closed


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
    created = await client.create_stream[str]("handed-out")
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
    await client.to_stream(queried, payload_type=str).publish("from the query result")
    await client.to_stream(result).publish("from the Workflow result")
    items = [item async for item in created.subscribe(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "from the query result",
        "from the Workflow result",
    ]


async def test_async_with_closes_a_client_stream(stream_client: Client):
    async with await stream_client.create_stream[str](f"block-{uuid.uuid4()}") as done:
        await done.publish("a")
    failed = await stream_client.create_stream[str](f"block-{uuid.uuid4()}")
    with pytest.raises(ValueError):
        async with failed:
            raise ValueError("turn failed")
    early = await stream_client.create_stream[str](f"block-{uuid.uuid4()}")
    async with early:
        await early.close()
    cancelled = await stream_client.create_stream[str](f"block-{uuid.uuid4()}")
    with pytest.raises(asyncio.CancelledError):
        async with cancelled:
            raise asyncio.CancelledError()
    for stream in (done, failed, early, cancelled):
        # A read of a closed Stream ends once it gave the last item, with no wait.
        items = await asyncio.wait_for(
            _records(stream_client.get_stream[str](stream.ref.id)), 5
        )
        assert items == (["a"] if stream is done else [])
        with pytest.raises(streams.StreamClosedError):
            await stream_client.get_stream[str](stream.ref.id).publish("late")


async def _records(stream: Stream[Any]) -> list[Any]:
    return [r.data async for r in stream.subscribe() if isinstance(r, streams.Record)]


async def test_create_fails_on_a_stream_that_exists(stream_client: Client):
    stream_id = f"again-{uuid.uuid4()}"
    first = await stream_client.create_stream[str](
        stream_id, retention=timedelta(days=1)
    )
    await first.publish("a")
    with pytest.raises(streams.StreamAlreadyExistsError) as exists:
        await stream_client.create_stream[str](stream_id)
    assert exists.value.reason == StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS
    # The create-or-get ignores the other Retention and returns the Stream.
    again = await stream_client.create_or_get_stream[str](
        stream_id, retention=timedelta(days=2)
    )
    assert again.ref == first.ref
    assert await again.get_offset() == await first.get_offset() > 0
    await first.close()
    # A closed Stream fails both creates with CLOSED, which comes before ALREADY_EXISTS.
    for create in (stream_client.create_stream, stream_client.create_or_get_stream):
        with pytest.raises(streams.StreamClosedError) as closed:
            await create[str](stream_id)
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
    with pytest.raises(streams.StreamAlreadyExistsError):
        await service._stream_service_create_stream(
            dataclasses.replace(request, request_id="create-2"), object
        )


async def test_publish_to_a_stream_that_nobody_created_raises(stream_client: Client):
    stream = stream_client.get_stream[str](f"missing-{uuid.uuid4()}")
    with pytest.raises(streams.StreamNotFoundError) as missing:
        await stream.publish("lost")
    assert missing.value.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND


async def test_next_offset_resumes_after_the_last_item(stream_client: Client):
    stream = await stream_client.create_stream[str](f"resume-{uuid.uuid4()}")
    last = await stream.publish("a", "b")
    tokens = stream.subscribe(wait_new_records=False)
    assert tokens.next_offset == 0
    assert [item.data async for item in tokens if isinstance(item, streams.Record)] == [
        "a",
        "b",
    ]
    assert tokens.next_offset == last + 1
    new = stream.subscribe(offset=streams.END, wait_new_records=False)
    assert new.next_offset is None
    assert [item async for item in new] == []
    assert new.next_offset == last + 1
    await stream.publish("c")
    resumed = stream.subscribe(offset=tokens.next_offset, wait_new_records=False)
    assert [
        item.data async for item in resumed if isinstance(item, streams.Record)
    ] == ["c"]


async def test_a_waiting_read_ends_when_the_stream_closes(stream_client: Client):
    stream = await stream_client.create_stream[str](f"wakes-{uuid.uuid4()}")
    await stream.publish("a")
    reader = asyncio.create_task(_records(stream_client.get_stream[str](stream.ref.id)))
    await asyncio.sleep(0.2)
    assert not reader.done()
    await stream.close()
    assert await asyncio.wait_for(reader, 10) == ["a"]
    # A read from the end of a closed Stream has no item left, so it answers at once.
    tail = stream_client.get_stream[str](stream.ref.id).subscribe(offset=streams.END)
    assert await asyncio.wait_for(_items(tail), 5) == []


async def _items(read: streams.Subscription[Any]) -> list[Any]:
    return [item async for item in read]


async def test_last_reads_the_newest_records_then_follows(stream_client: Client):
    stream = await stream_client.create_stream[str](f"last-{uuid.uuid4()}")
    await stream.publish("a", "b", "c")
    # A chat view shows the last few Records first, then waits for new ones.
    tail = stream.subscribe(last=2)
    assert tail.next_offset is None
    shown = [await anext(tail), await anext(tail)]
    await stream.publish("d")
    shown.append(await anext(tail))
    await tail.aclose()
    assert [item.data for item in shown if isinstance(item, streams.Record)] == [
        "b",
        "c",
        "d",
    ]
    newest = stream.subscribe(last=1, wait_new_records=False)
    assert [item.data async for item in newest if isinstance(item, streams.Record)] == [
        "d"
    ]
    with pytest.raises(ValueError, match="no offset"):
        stream.subscribe(last=2, offset=0)


async def test_payload_type_of_the_call_decodes_each_value(stream_client: Client):
    stream = await stream_client.create_stream[Turn](f"typed-{uuid.uuid4()}")
    await stream.publish(Turn("model"))
    turns = stream_client.get_stream[Turn](stream.ref.id)
    dicts = stream_client.get_stream[dict[str, Any]](stream.ref.id)
    for reader, expected in (
        (turns, Turn("model")),
        (dicts, {"kind": "model", "text": ""}),
    ):
        read = reader.subscribe(wait_new_records=False)
        assert [r.data async for r in read if isinstance(r, streams.Record)] == [
            expected
        ]


async def test_record_that_fails_to_decode_raises_and_resumes_past_it(
    stream_client: Client,
):
    stream_id = f"poison-{uuid.uuid4()}"
    writer = await stream_client.create_stream[str](stream_id)
    bad = await writer.publish("not a turn")
    reader = stream_client.get_stream[Turn](stream_id)
    await reader.publish(Turn("after"))
    read = reader.subscribe(wait_new_records=False)
    with pytest.raises(streams.StreamError, match=f"offset {bad}"):
        await anext(read)
    assert read.next_offset == bad + 1
    resumed = reader.subscribe(offset=read.next_offset, wait_new_records=False)
    assert [
        item.data async for item in resumed if isinstance(item, streams.Record)
    ] == [Turn("after")]


async def test_record_over_the_store_limit_raises_too_large(stream_client: Client):
    # The client's writer splits a batch at the Store's limit, so only one Record can be over it.
    stream = await stream_client.create_stream[str](f"large-{uuid.uuid4()}")
    with pytest.raises(streams.StreamTooLargeError) as refused:
        await stream.publish("x" * 2_000_000)
    assert refused.value.reason == StreamErrorReason.STREAM_ERROR_REASON_TOO_LARGE
    assert refused.value.non_retryable
    assert await stream.get_offset() == 0


async def test_read_keeps_no_offsets_of_client_records(stream_client: Client):
    # Nothing withdraws a client's Records, so a long UI read of them keeps no state.
    stream = await stream_client.create_stream[str](f"chat-{uuid.uuid4()}")
    await stream.publish("a", "b", "c")
    read = stream.subscribe(wait_new_records=False)
    assert [item.data async for item in read if isinstance(item, streams.Record)] == [
        "a",
        "b",
        "c",
    ]
    assert read._shown == {}


async def test_create_of_an_expired_stream_raises(stream_client: Client):
    # An open standalone Stream expires one Retention after its last write.
    stream_id = f"expired-{uuid.uuid4()}"
    retention = timedelta(milliseconds=200)
    stream = await stream_client.create_stream[str](stream_id, retention=retention)
    await stream.publish("old")
    await asyncio.sleep(0.5)
    with pytest.raises(streams.StreamExpiredError):
        await stream_client.create_or_get_stream[str](stream_id, retention=retention)


async def test_named_producer_fences_its_earlier_epoch(stream_client: Client):
    stream = await stream_client.create_stream[str](f"named-{uuid.uuid4()}")
    # A server that restarts writes as the same name with a higher epoch, so the zombie that it
    # replaces can't write after it.
    before = stream.producer("ingest", epoch=1)
    await before.publish("before restart")
    after = stream.producer("ingest", epoch=2)
    await after.publish("after restart")
    with pytest.raises(streams.StreamFencedError):
        await before.publish("zombie")
    # The earlier epoch's Records are real messages, so nothing withdraws them.
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [item.kind for item in items] == [streams.ItemKind.RECORD] * 2
    with pytest.raises(ValueError):
        stream.producer("ingest", epoch=0)


async def test_clients_of_one_runtime_share_the_backend_calls(stream_client: Client):
    # A service that makes a client for each tenant keeps one set of Redis connections.
    other = Client(**stream_client.config())
    [backend] = stream_client.config(active_config=True)["stream_backends"]
    assert backend._backend_for(other) is backend._backend_for(stream_client)
    stream = await other.create_stream[str](f"shared-{uuid.uuid4()}")
    await stream.publish("from the other client")
    again = stream_client.get_stream[str](stream.ref.id)
    items = [item async for item in again.subscribe(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "from the other client"
    ]


async def test_default_is_native_with_no_default_backend(client: Client):
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend()]
    one = Client(**config)
    with pytest.raises(streams.StreamConfigError, match="default is native"):
        one.get_stream[str]("d-2")
    with pytest.raises(streams.StreamConfigError, match="default is native"):
        await one.create_stream[str]("d-2")
    with pytest.raises(streams.StreamConfigError, match="default is native"):
        one.to_stream(streams.StreamRef(id="d-2"))
    on_memory = one.get_stream[str]("d-2", backend="memory")
    assert on_memory.ref.backend == "memory"
    created = await one.create_stream[str]("d-2", backend="memory")
    assert created.ref.backend == "memory"


async def test_stream_takes_the_named_backend_or_the_default(client: Client):
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(name="a", default=True),
        MemoryStreamBackend(name="b"),
    ]
    two = Client(**config)
    created = await two.create_stream[str]("moving", backend="b")
    await created.publish("on b")
    on_b = two.get_stream[str]("moving", backend="b")
    assert on_b.ref == created.ref and await on_b.get_offset() > 0
    assert two.get_stream[str]("moving").ref.backend == "a"
    # A ref with no backend name uses the default backend, which doesn't have the Stream.
    with pytest.raises(streams.StreamNotFoundError):
        await two.to_stream(streams.StreamRef(id="moving")).get_offset()
    with pytest.raises(ValueError):
        two.get_stream[str]("")


@workflow.defn
class WriteToStream:
    @workflow.run
    async def run(self, ref: streams.StreamRef[Turn]) -> None:
        await workflow.to_stream(ref).publish(Turn("workflow"))


async def test_client_stream_takes_workflow_writes(stream_client: Client):
    stream = await stream_client.create_stream[Turn](f"chat-{uuid.uuid4()}")
    assert await stream.get_offset() == 0
    first = await stream.publish(Turn("client"))
    last = await stream.publish(Turn("a"), Turn("b"))
    assert last > first
    async with new_worker(stream_client, WriteToStream) as worker:
        await stream_client.execute_workflow(
            WriteToStream.run,
            stream.ref,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    await stream.close()
    items = [item async for item in stream.subscribe()]
    data = [item.data for item in items if isinstance(item, streams.Record)]
    assert data == [Turn("client"), Turn("a"), Turn("b"), Turn("workflow")]

    # Another Stream of the same id reads it from an offset.
    again = stream_client.get_stream[Turn](stream.ref.id)
    resumed = [
        item.data
        async for item in again.subscribe(offset=first + 1)
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
        self._tokens = stream = await workflow.create_stream[str](
            f"{workflow.info().workflow_id}-tokens"
        )
        await stream.publish(*prompt.split())
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
        reader = stream_client.to_stream(ref)
        items = [item async for item in reader.subscribe()]
        await handle.result()
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "hi",
        "there",
    ]
    # A Workflow creates only standalone Streams, so the ref has no owner.
    assert reader.ref.owner is None and reader.ref.backend is not None


async def test_empty_stream_id_raises_with_no_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(ValueError, match="id is empty"):
        client.get_stream[Turn]("")
    assert not extension.namespaces


async def test_registration_rules(client: Client):
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(),
        MemoryStreamBackend(name="b", default=True),
    ]
    two = Client(**config)
    assert (await two.create_stream[str]("d-1")).ref.backend == "b"
    assert two.get_stream[str]("d-1").ref.backend == "b"
    # A Worker serves only one backend, and so does a Replayer.
    with pytest.raises(streams.StreamConfigError, match="one backend"):
        new_worker(Client(**config), Agent)
    with pytest.raises(streams.StreamConfigError, match="one backend"):
        Replayer(workflows=[Agent], stream_backends=config["stream_backends"])
