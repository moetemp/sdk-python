import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.contrib.streams._backend import _ExtensionStreamBackend
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.contrib.streams.testing import MemoryStreamPlugin
from temporalio.converter import DataConverter
from tests.contrib.streams.fake_extension import FakeExtension
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


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


async def test_unknown_backend_raises_before_any_call(client: Client):
    client, extension = fake_client(client)
    stream_client = streams.StreamClient(client)
    with pytest.raises(streams.StreamConfigError, match="It has fake"):
        await stream_client.create(Events, id="chat-1", backend="redis")
    with pytest.raises(streams.StreamConfigError):
        stream_client.get_handle(Events, id="chat-1", backend="redis")
    assert not extension.streams and not extension.namespaces
    stream = await stream_client.create(Events, id="chat-1")
    assert stream.backend == "fake"


async def test_client_without_plugin_has_no_transport(client: Client):
    with pytest.raises(streams.StreamConfigError):
        streams.StreamClient(client).get_handle(Events, id="chat-1")


async def test_decoded_client_stream_has_no_client(client: Client):
    client, _ = fake_client(client)
    stream = await streams.StreamClient(client).create(Events, id="chat-2")
    converter = DataConverter.default.payload_converter
    decoded = converter.from_payload(
        converter.to_payload(stream), streams.ClientStream[Events]
    )
    assert decoded == stream
    with pytest.raises(streams.StreamConfigError, match="get_handle"):
        await decoded.get_offset()


async def test_handle_without_first_run_describes_once(client: Client):
    client, _ = fake_client(client)
    handle = client.get_workflow_handle("agent-1")
    calls = []

    async def describe(**_: Any) -> object:
        calls.append(1)

        class Info:
            first_run_id = "first"

        class Raw:
            workflow_execution_info = Info()

        class Description:
            raw_description = Raw()

        return Description()

    setattr(handle, "describe", describe)
    stream_client = streams.StreamClient(client)
    await stream_client.create(Events, id="agent-1:first:n")
    stream = stream_client.workflow_stream(handle, streams.with_name(Events, "n"))
    with pytest.raises(RuntimeError, match="not known yet"):
        stream.id
    assert await stream.get_offset() == 0
    assert stream.id == "agent-1:first:n"
    assert stream.first_execution_run_id == "first"
    assert await stream.get_offset() == 0
    assert calls == [1]


@workflow.defn
class WriteToStream:
    @workflow.run
    async def run(self, stream: stream_workflow.WorkflowStream[Events]) -> None:
        await stream.topic(Events.turns).publish(Turn("workflow"))


async def test_client_stream_takes_workflow_writes(stream_client: Client):
    client = streams.StreamClient(stream_client)
    stream = await client.create(Events, id=f"chat-{uuid.uuid4()}")
    assert await stream.get_offset() == 0
    first = await stream.topic(Events.turns).publish(Turn("client"))
    last = await stream.topic(Events.tokens).publish("a", "b")
    assert last > first
    async with new_worker(stream_client, WriteToStream) as worker:
        await stream_client.execute_workflow(
            WriteToStream.run,
            stream_workflow.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    await stream.close(result="bye")
    items = [item async for item in stream.subscribe()]
    data = [item.data for item in items if isinstance(item, streams.Record)]
    assert data == [Turn("client"), "a", "b", Turn("workflow")]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "bye"

    # Another handle of the same Stream reads it, from an offset and by Topic.
    again = client.get_handle(Events, id=stream.id)
    resumed = [
        item.data
        async for item in again.subscribe(Events.tokens, from_offset=first + 1)
        if isinstance(item, streams.Record)
    ]
    assert resumed == ["a", "b"]
    by_prefix = [
        item.data
        async for item in again.subscribe(prefixes=["tok"])
        if isinstance(item, streams.Record)
    ]
    assert by_prefix == ["a", "b"]
    only_turns = [item async for item in again.topic(Events.turns).subscribe()]
    assert [type(item) for item in only_turns] == [
        streams.Record,
        streams.Record,
        streams.Closed,
    ]


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        await workflow.sleep(timedelta(milliseconds=200))
        stream = await stream_workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish(*prompt.split())
        return prompt.upper()


async def test_client_reads_a_workflow_stream(stream_client: Client):
    async with new_worker(stream_client, Agent) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = streams.StreamClient(stream_client).workflow_stream(handle, Events)
        # The Workflow creates its Stream later, so the read waits for the create.
        items = [
            item async for item in stream.subscribe(create_wait=timedelta(seconds=20))
        ]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "hi",
        "there",
    ]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "HI THERE"
    assert stream.workflow_id == handle.id
    with pytest.raises(streams.StreamNotOwnerError):
        await stream.close()


async def test_async_with_closes_a_client_stream(stream_client: Client):
    client = streams.StreamClient(stream_client)
    async with client.create(Events, id=f"block-{uuid.uuid4()}") as done:
        await done.topic(Events.tokens).publish("a")
    failed = client.get_handle(Events, id=f"block-{uuid.uuid4()}")
    with pytest.raises(ValueError):
        async with client.create(Events, id=failed.id):
            raise ValueError("turn failed")
    async with await client.create(Events, id=f"block-{uuid.uuid4()}") as early:
        await early.close(result="early")
    cancelled = client.get_handle(Events, id=f"block-{uuid.uuid4()}")
    with pytest.raises(asyncio.CancelledError):
        async with client.create(Events, id=cancelled.id):
            raise asyncio.CancelledError()
    for stream, result, failure, status in (
        (done, None, None, streams.CloseStatus.COMPLETED),
        (failed, None, "turn failed", streams.CloseStatus.FAILED),
        (early, "early", None, streams.CloseStatus.COMPLETED),
        (cancelled, None, "Cancelled", streams.CloseStatus.CANCELED),
    ):
        items = [item async for item in stream.subscribe()]
        closes = [item for item in items if isinstance(item, streams.Closed)]
        assert len(closes) == 1 and closes[0].result == result
        assert closes[0].status == status
        assert (failure is None) == (closes[0].failure is None)
        assert failure is None or failure in str(closes[0].failure)


async def test_next_offset_resumes_after_the_last_item(stream_client: Client):
    stream = await streams.StreamClient(stream_client).create(
        Events, id=f"resume-{uuid.uuid4()}"
    )
    await stream.topic(Events.tokens).publish("a")
    turn = await stream.topic(Events.turns).publish(Turn("model"))
    tokens = stream.topic(Events.tokens).subscribe(wait_new_records=False)
    assert tokens.next_offset == 0
    assert [item.data async for item in tokens if isinstance(item, streams.Record)] == [
        "a"
    ]
    # The page ends past the Record that the Topic filter left out.
    assert tokens.next_offset == turn + 1
    new = stream.subscribe(from_offset=streams.END, wait_new_records=False)
    assert new.next_offset is None
    assert [item async for item in new] == []
    assert new.next_offset == turn + 1
    await stream.topic(Events.tokens).publish("b")
    resumed = stream.subscribe(from_offset=tokens.next_offset, wait_new_records=False)
    assert [
        item.data async for item in resumed if isinstance(item, streams.Record)
    ] == ["b"]


async def test_last_reads_the_newest_records_then_follows(stream_client: Client):
    stream = await streams.StreamClient(stream_client).create(
        Events, id=f"last-{uuid.uuid4()}"
    )
    tokens = stream.topic(Events.tokens)
    await tokens.publish("a", "b", "c")
    await stream.topic(Events.turns).publish(Turn("model"))
    # A chat view shows the last few Records first, then follows.
    tail = tokens.subscribe(last=2)
    assert tail.next_offset is None
    shown = [await anext(tail), await anext(tail)]
    await tokens.publish("d")
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
    with pytest.raises(ValueError, match="no from_offset"):
        stream.subscribe(last=2, from_offset=0)


async def test_read_type_decodes_topics_outside_the_definition(stream_client: Client):
    stream = await streams.StreamClient(stream_client).create(
        Events, id=f"typed-{uuid.uuid4()}"
    )
    await stream.topic("extra", type=Turn).publish(Turn("extra"))
    await stream.topic(Events.tokens).publish("a")

    async def read(**options: Any) -> list[object]:
        items = stream.subscribe(wait_new_records=False, **options)
        return [item.data async for item in items if isinstance(item, streams.Record)]

    assert await read() == [{"kind": "extra", "text": ""}, "a"]
    assert await read(type=Turn) == [Turn("extra"), "a"]


async def test_buffered_publisher_keeps_the_order_across_topics(stream_client: Client):
    stream = await streams.StreamClient(stream_client).create(
        Events, id=f"buffered-{uuid.uuid4()}"
    )
    async with streams.BufferedPublisher(stream) as publisher:
        publisher.publish(Events.turns, Turn("user"))
        publisher.publish(Events.tokens, "a", "b")
        publisher.publish("turns", Turn("model"))
        with pytest.raises(streams.StreamError, match="no Topic"):
            publisher.publish("turn", Turn("typo"))
    written = [
        (item.topic, item.data)
        async for item in stream.subscribe(wait_new_records=False)
        if isinstance(item, streams.Record)
    ]
    assert written == [
        ("turns", Turn("user")),
        ("tokens", "a"),
        ("tokens", "b"),
        ("turns", Turn("model")),
    ]


async def test_auto_close_of_a_client_stream(stream_client: Client):
    stream = await streams.StreamClient(stream_client).create(
        Events, id=f"quiet-{uuid.uuid4()}", auto_close=timedelta(seconds=1)
    )
    await stream.topic(Events.tokens).publish("only")
    await asyncio.sleep(1.5)
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [item.data for item in items if isinstance(item, streams.Record)] == ["only"]
    assert isinstance(items[-1], streams.Closed) and items[-1].result is None
    with pytest.raises(streams.StreamClosedError):
        await stream.topic(Events.tokens).publish("late")


async def test_unresolved_workflow_stream_compares_and_prints(client: Client):
    client, _ = fake_client(client)
    stream_client = streams.StreamClient(client)
    handle = client.get_workflow_handle("agent-2")
    first = stream_client.workflow_stream(handle, Events)
    same = stream_client.workflow_stream(handle, Events)
    other = stream_client.workflow_stream(handle, streams.with_name(Events, "other"))
    key = hash(first)
    assert first == same and first != other
    describes = []

    async def describe(**_: Any) -> Any:
        describes.append(1)
        await asyncio.sleep(0.05)
        info = SimpleNamespace(first_run_id="run-2")
        return SimpleNamespace(
            raw_description=SimpleNamespace(workflow_execution_info=info)
        )

    setattr(handle, "describe", describe)
    await stream_client.create(Events, id="agent-2:run-2:events")
    # Calls that start together share one Describe call.
    assert await asyncio.gather(first.get_offset(), first.get_offset()) == [0, 0]
    assert describes == [1]
    assert first.id == "agent-2:run-2:events"
    assert hash(first) == key


async def test_default_plugin_is_the_default_backend(client: Client):
    config = client.config()
    config["plugins"] = [
        MemoryStreamPlugin(),
        MemoryStreamPlugin(name="b", default=True),
    ]
    stream = await streams.StreamClient(Client(**config)).create(Events, id="d-1")
    assert stream.backend == "b"
    # The default is part of Workflow code, so it must not follow the order of the list.
    for plugins in (
        [MemoryStreamPlugin(), MemoryStreamPlugin(name="b")],
        [MemoryStreamPlugin(default=True), MemoryStreamPlugin(name="b", default=True)],
    ):
        config["plugins"] = plugins
        with pytest.raises(streams.StreamConfigError, match="exactly one"):
            Client(**config)
    config["plugins"] = [MemoryStreamPlugin()]
    with pytest.raises(streams.StreamConfigError, match="Worker has"):
        new_worker(Client(**config), Agent, plugins=[RedisStreamPlugin("redis://-")])


async def test_handle_looks_in_each_backend_during_a_move(client: Client):
    config = client.config()
    config["plugins"] = [
        MemoryStreamPlugin(name="a", default=True),
        MemoryStreamPlugin(name="b"),
    ]
    stream_client = streams.StreamClient(Client(**config))
    created = await stream_client.create(Events, id="moving", backend="b")
    await created.topic(Events.tokens).publish("on b")
    moving = stream_client.get_handle(Events, id="moving", backend=("a", "b"))
    assert await moving.get_offset() > 0
    assert moving.backend == "b"
    nowhere = stream_client.get_handle(Events, id="nowhere", backend=("a", "b"))
    with pytest.raises(streams.StreamNotFoundError):
        await nowhere.get_offset()
    # A handle with no backend name is ambiguous when the client has two backends.
    with pytest.raises(streams.StreamConfigError):
        stream_client.get_handle(Events, stream=streams.ClientStream(id="moving"))
