import uuid
from dataclasses import dataclass

import pytest

from temporalio import common, workflow
from temporalio.client import Client
from temporalio.converter import DataConverter
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
