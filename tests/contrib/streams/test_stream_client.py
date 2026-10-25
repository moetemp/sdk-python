import asyncio
import dataclasses
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import common, workflow
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import Client
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import DataConverter, DefaultFailureConverter
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer
from tests.contrib.streams.fake_extension import fake_client
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@common.defn(name="tokens")
class Tokens(common.StreamDefinition[str]): ...


async def test_unknown_backend_raises_before_any_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(common.StreamConfigError, match="no Stream backend redis"):
        await client.create_stream(Turns, id="chat-1", backend="redis")
    with pytest.raises(common.StreamConfigError, match="It has fake"):
        client.stream(Turns, id="chat-1", backend="redis")
    assert not extension.streams and not extension.namespaces
    stream = await client.create_stream(Turns, id="chat-1")
    assert stream.backend == "fake"


async def test_client_without_backend_has_no_transport(client: Client):
    with pytest.raises(common.StreamConfigError, match="no Stream backend"):
        client.stream(Turns, id="chat-1")
    with pytest.raises(common.StreamConfigError, match="no Stream backend"):
        await client.create_stream(Turns, id="chat-1")


async def test_decoded_client_stream_has_no_client(client: Client):
    client, _ = fake_client(client)
    stream = await client.create_stream(Turns, id="chat-2")
    converter = DataConverter.default.payload_converter
    decoded = converter.from_payload(
        converter.to_payload(stream), common.ClientStream[Turn]
    )
    assert decoded == stream
    with pytest.raises(common.StreamConfigError, match="Client.stream"):
        await decoded.get_offset()


@workflow.defn
class HandsOutStream:
    @workflow.run
    async def run(self) -> None:
        pass

    @workflow.query
    def stream(self) -> dict[str, str]:
        # The proto JSON of a ref, as a Workflow Stream that a query returns.
        return {"id": "handed-out"}


async def test_query_result_binds_its_client_stream(client: Client):
    client, _ = fake_client(client)
    created = await client.create_stream(Tokens, id="handed-out")
    async with new_worker(client, HandsOutStream) as worker:
        handle = await client.start_workflow(
            HandsOutStream.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        stream = await handle.query("stream", result_type=common.ClientStream[str])
    # The ref names no backend, so the handle uses the default one.
    assert stream.id == "handed-out" and stream.backend is None
    await stream.publish("from the query result")
    items = [item async for item in created.subscribe(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, common.Record)] == [
        "from the query result"
    ]


@workflow.defn
class WriteToStream:
    @workflow.run
    async def run(self, stream: common.WorkflowStream[Turn]) -> None:
        await stream.publish(Turn("workflow"))


async def test_client_stream_takes_workflow_writes(stream_client: Client):
    stream = await stream_client.create_stream(Turns, id=f"chat-{uuid.uuid4()}")
    assert await stream.get_offset() == 0
    first = await stream.publish(Turn("client"))
    last = await stream.publish(Turn("a"), Turn("b"))
    assert last > first
    async with new_worker(stream_client, WriteToStream) as worker:
        await stream_client.execute_workflow(
            WriteToStream.run,
            common.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    await stream.close(result="bye")
    items = [item async for item in stream.subscribe()]
    data = [item.data for item in items if isinstance(item, common.Record)]
    assert data == [Turn("client"), Turn("a"), Turn("b"), Turn("workflow")]
    assert isinstance(items[-1], common.Closed) and items[-1].result == "bye"

    # Another handle of the same Stream reads it from an offset.
    again = stream_client.stream(Turns, id=stream.id)
    resumed = [
        item.data
        async for item in again.subscribe(offset=first + 1)
        if isinstance(item, common.Record)
    ]
    assert resumed == [Turn("a"), Turn("b"), Turn("workflow")]


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        await workflow.sleep(timedelta(milliseconds=200))
        stream = await workflow.create_stream(Tokens)
        await stream.publish(*prompt.split())
        await stream.close(result=prompt.upper())
        return prompt.upper()

    @workflow.query
    def stream(self) -> common.WorkflowStream[str]:
        # The handle needs no call, so a reader gets it before the create.
        return workflow.stream_handle(Tokens)


async def test_client_reads_a_workflow_stream(stream_client: Client):
    async with new_worker(stream_client, Agent) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = await handle.query(Agent.stream)
        reader = await handle.query("stream", result_type=common.ClientStream[str])
        # The Workflow creates its Stream later, so the read waits for the create.
        items = [
            item async for item in reader.subscribe(create_wait=timedelta(seconds=20))
        ]
    assert stream.id == reader.id
    assert [item.data for item in items if isinstance(item, common.Record)] == [
        "hi",
        "there",
    ]
    assert isinstance(items[-1], common.Closed) and items[-1].result == "HI THERE"
    assert reader.workflow_id == handle.id
    with pytest.raises(common.StreamNotOwnerError):
        await reader.close()


async def test_async_with_closes_a_client_stream(stream_client: Client):
    async with stream_client.create_stream(Tokens, id=f"block-{uuid.uuid4()}") as done:
        await done.publish("a")
    failed = stream_client.stream(Tokens, id=f"block-{uuid.uuid4()}")
    with pytest.raises(ValueError):
        async with stream_client.create_stream(Tokens, id=failed.id):
            raise ValueError("turn failed")
    async with await stream_client.create_stream(
        Tokens, id=f"block-{uuid.uuid4()}"
    ) as early:
        await early.close(result="early")
    cancelled = stream_client.stream(Tokens, id=f"block-{uuid.uuid4()}")
    with pytest.raises(asyncio.CancelledError):
        async with stream_client.create_stream(Tokens, id=cancelled.id):
            raise asyncio.CancelledError()
    for stream, result, failure, status in (
        (done, None, None, common.CloseStatus.COMPLETED),
        (failed, None, "turn failed", common.CloseStatus.FAILED),
        (early, "early", None, common.CloseStatus.COMPLETED),
        (cancelled, None, "Cancelled", common.CloseStatus.CANCELED),
    ):
        items = [item async for item in stream.subscribe()]
        closes = [item for item in items if isinstance(item, common.Closed)]
        assert len(closes) == 1 and closes[0].result == result
        assert closes[0].status == status
        assert (failure is None) == (closes[0].failure is None)
        assert failure is None or failure in str(closes[0].failure)


async def test_create_gets_a_stream_that_exists(stream_client: Client):
    stream_id = f"again-{uuid.uuid4()}"
    first = await stream_client.create_stream(
        Tokens, id=stream_id, retention=timedelta(days=1)
    )
    await first.publish("a")
    # The Stream exists, so the create ignores the other Retention and returns it.
    again = await stream_client.create_stream(
        Tokens, id=stream_id, retention=timedelta(days=2)
    )
    assert again == first
    assert await again.get_offset() == await first.get_offset() > 0
    await first.close()
    with pytest.raises(common.StreamClosedError) as closed:
        await stream_client.create_stream(Tokens, id=stream_id)
    assert closed.value.reason == StreamErrorReason.STREAM_ERROR_REASON_CLOSED


async def test_publish_to_a_stream_that_nobody_created_raises(stream_client: Client):
    stream = stream_client.stream(Tokens, id=f"missing-{uuid.uuid4()}")
    with pytest.raises(common.StreamNotFoundError) as missing:
        await stream.publish("lost")
    assert missing.value.reason == StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND


async def test_next_offset_resumes_after_the_last_item(stream_client: Client):
    stream = await stream_client.create_stream(Tokens, id=f"resume-{uuid.uuid4()}")
    last = await stream.publish("a", "b")
    tokens = stream.subscribe(wait_new_records=False)
    assert tokens.next_offset == 0
    assert [item.data async for item in tokens if isinstance(item, common.Record)] == [
        "a",
        "b",
    ]
    assert tokens.next_offset == last + 1
    new = stream.subscribe(offset=common.END, wait_new_records=False)
    assert new.next_offset is None
    assert [item async for item in new] == []
    assert new.next_offset == last + 1
    await stream.publish("c")
    resumed = stream.subscribe(offset=tokens.next_offset, wait_new_records=False)
    assert [item.data async for item in resumed if isinstance(item, common.Record)] == [
        "c"
    ]


class _EncodesCommonAttributes(DefaultFailureConverter):
    def __init__(self) -> None:
        super().__init__(encode_common_attributes=True)


async def test_closed_failure_decodes_with_the_clients_converter(stream_client: Client):
    # The failure converter puts the message in the encoded attributes, which only the payload
    # converter reads.
    config = stream_client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, failure_converter_class=_EncodesCommonAttributes
    )
    client = Client(**config)
    stream = await client.create_stream(Tokens, id=f"failed-{uuid.uuid4()}")
    await stream.close(
        failure=ApplicationError("tool failed", "detail", {"k": 1}, type="Tool")
    )
    [closed] = [item async for item in stream.subscribe()]
    assert isinstance(closed, common.Closed)
    assert isinstance(closed.failure, ApplicationError)
    assert closed.failure.message == "tool failed" and closed.failure.type == "Tool"
    assert closed.failure.details == ("detail", {"k": 1})


async def test_last_reads_the_newest_records_then_follows(stream_client: Client):
    stream = await stream_client.create_stream(Tokens, id=f"last-{uuid.uuid4()}")
    await stream.publish("a", "b", "c")
    # A chat view shows the last few Records first, then waits for new ones.
    tail = stream.subscribe(last=2)
    assert tail.next_offset is None
    shown = [await anext(tail), await anext(tail)]
    await stream.publish("d")
    shown.append(await anext(tail))
    await tail.aclose()
    assert [item.data for item in shown if isinstance(item, common.Record)] == [
        "b",
        "c",
        "d",
    ]
    newest = stream.subscribe(last=1, wait_new_records=False)
    assert [item.data async for item in newest if isinstance(item, common.Record)] == [
        "d"
    ]
    with pytest.raises(ValueError, match="no offset"):
        stream.subscribe(last=2, offset=0)


@common.defn(name="turns")
class TurnDicts(common.StreamDefinition[dict[str, Any]]): ...


async def test_payload_type_of_the_definition_decodes_each_value(
    stream_client: Client,
):
    stream = await stream_client.create_stream(Turns, id=f"typed-{uuid.uuid4()}")
    await stream.publish(Turn("model"))

    async def read(definition: type[common.StreamDefinition[Any]]) -> list[object]:
        items = stream_client.stream(definition, id=stream.id).subscribe(
            wait_new_records=False
        )
        return [item.data async for item in items if isinstance(item, common.Record)]

    assert await read(Turns) == [Turn("model")]
    assert await read(TurnDicts) == [{"kind": "model", "text": ""}]


async def test_buffered_publisher_keeps_the_publish_order(stream_client: Client):
    stream = await stream_client.create_stream(Turns, id=f"buffered-{uuid.uuid4()}")
    async with common.BufferedPublisher(stream) as publisher:
        publisher.publish(Turn("user"))
        publisher.publish(Turn("a"), Turn("b"))
        publisher.publish(Turn("model"))
    written = [
        item.data
        async for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, common.Record)
    ]
    assert written == [Turn("user"), Turn("a"), Turn("b"), Turn("model")]


@common.defn(name="turns")
class LooseTurns(common.StreamDefinition[str]): ...


async def test_record_that_fails_to_decode_raises_and_resumes_past_it(
    stream_client: Client,
):
    stream_id = f"poison-{uuid.uuid4()}"
    writer = await stream_client.create_stream(LooseTurns, id=stream_id)
    bad = await writer.publish("not a turn")
    reader = stream_client.stream(Turns, id=stream_id)
    await reader.publish(Turn("after"))
    read = reader.subscribe(wait_new_records=False)
    with pytest.raises(common.StreamError, match=f"offset {bad}"):
        await anext(read)
    assert read.next_offset == bad + 1
    resumed = reader.subscribe(offset=read.next_offset, wait_new_records=False)
    assert [item.data async for item in resumed if isinstance(item, common.Record)] == [
        Turn("after")
    ]


async def test_async_with_refuses_a_workflow_stream_before_the_block(
    stream_client: Client,
):
    # The close at the block end would raise and hide the block's own error.
    owned: common.ClientStream[Turn] = common.ClientStream(
        id="agent-1:run-1:turns", workflow_id="agent-1", first_execution_run_id="run-1"
    )
    ran = False
    with pytest.raises(common.StreamNotOwnerError):
        async with stream_client.stream(Turns, stream=owned):
            ran = True
    assert not ran


async def test_record_over_the_store_limit_raises_too_large(stream_client: Client):
    # The client's writer splits a batch at the Store's limit, so only one Record can be over it.
    stream = await stream_client.create_stream(Tokens, id=f"large-{uuid.uuid4()}")
    with pytest.raises(common.StreamTooLargeError) as refused:
        await stream.publish("x" * 2_000_000)
    assert refused.value.reason == StreamErrorReason.STREAM_ERROR_REASON_TOO_LARGE
    assert refused.value.non_retryable
    assert await stream.get_offset() == 0


async def test_read_keeps_no_offsets_of_client_records(stream_client: Client):
    # Nothing withdraws a client's Records, so a long UI read of them keeps no state.
    stream = await stream_client.create_stream(Tokens, id=f"chat-{uuid.uuid4()}")
    await stream.publish("a", "b", "c")
    read = stream.subscribe(wait_new_records=False)
    assert [item.data async for item in read if isinstance(item, common.Record)] == [
        "a",
        "b",
        "c",
    ]
    assert read._shown == {}


async def test_empty_stream_id_raises_with_no_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(ValueError, match="id is empty"):
        client.stream(Turns, id="")
    with pytest.raises(ValueError, match="id is empty"):
        client.stream(Turns, stream=common.WorkflowStream(id=""))
    assert not extension.namespaces


async def test_create_of_an_expired_stream_raises(stream_client: Client):
    # An open client Stream expires one Retention after its last write.
    stream_id = f"expired-{uuid.uuid4()}"
    retention = timedelta(milliseconds=200)
    stream = await stream_client.create_stream(
        Tokens, id=stream_id, retention=retention
    )
    await stream.publish("old")
    await asyncio.sleep(0.5)
    with pytest.raises(common.StreamExpiredError):
        await stream_client.create_stream(Tokens, id=stream_id, retention=retention)


async def test_named_producer_fences_its_earlier_epoch(stream_client: Client):
    stream = await stream_client.create_stream(Tokens, id=f"named-{uuid.uuid4()}")
    # A server that restarts writes as the same name with a higher epoch, so the zombie that it
    # replaces can't write after it.
    before = stream.producer("ingest", epoch=1)
    await before.publish("before restart")
    after = stream.producer("ingest", epoch=2)
    await after.publish("after restart")
    with pytest.raises(common.StreamFencedError):
        await before.publish("zombie")
    # The earlier epoch's Records are real messages, so nothing withdraws them.
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [type(item) for item in items] == [common.Record, common.Record]
    with pytest.raises(ValueError):
        stream.producer("ingest", epoch=0)


async def test_clients_of_one_runtime_share_the_backend_calls(stream_client: Client):
    # A service that makes a client for each tenant keeps one set of Redis connections.
    other = Client(**stream_client.config())
    [backend] = stream_client.config(active_config=True)["stream_backends"]
    assert backend._backend_for(other) is backend._backend_for(stream_client)
    stream = await other.create_stream(Tokens, id=f"shared-{uuid.uuid4()}")
    await stream.publish("from the other client")
    again = stream_client.stream(Tokens, id=stream.id)
    items = [item async for item in again.subscribe(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, common.Record)] == [
        "from the other client"
    ]


async def test_registration_rules(client: Client):
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(),
        MemoryStreamBackend(name="b", default=True),
    ]
    two = Client(**config)
    assert (await two.create_stream(Tokens, id="d-1")).backend == "b"
    assert two.stream(Tokens, id="d-1").backend == "b"
    # A Worker serves only one backend, and so does a Replayer.
    with pytest.raises(common.StreamConfigError, match="one backend"):
        new_worker(Client(**config), Agent)
    with pytest.raises(common.StreamConfigError, match="one backend"):
        Replayer(workflows=[Agent], stream_backends=config["stream_backends"])


async def test_default_is_native_with_no_default_backend(client: Client):
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend()]
    one = Client(**config)
    with pytest.raises(common.StreamConfigError, match="default is native"):
        one.stream(Tokens, id="d-2")
    with pytest.raises(common.StreamConfigError, match="default is native"):
        await one.create_stream(Tokens, id="d-2")
    assert one.stream(Tokens, id="d-2", backend="memory").backend == "memory"
    assert (await one.create_stream(Tokens, id="d-2", backend="memory")).backend == (
        "memory"
    )


async def test_stream_takes_the_handle_backend_or_the_default(client: Client):
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(name="a", default=True),
        MemoryStreamBackend(name="b"),
    ]
    two = Client(**config)
    created = await two.create_stream(Tokens, id="moving", backend="b")
    await created.publish("on b")
    assert await two.stream(Tokens, stream=created).get_offset() > 0
    # A handle with no backend name uses the default backend.
    assert two.stream(Tokens, stream=common.ClientStream(id="moving")).backend == "a"
    with pytest.raises(ValueError):
        two.stream(Tokens, stream=created, backend="a")
    with pytest.raises(TypeError):
        two.stream(Tokens)
