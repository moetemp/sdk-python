import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

import pytest

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.converter import DataConverter
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
