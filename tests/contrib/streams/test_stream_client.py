import asyncio
import dataclasses
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

from temporalio import workflow
from temporalio.client import Client
from temporalio.common import streams
from temporalio.common.streams import _plugin, _stream_client, _writes
from temporalio.contrib import stream as redis
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import DataConverter, DefaultFailureConverter
from temporalio.exceptions import ApplicationError
from temporalio.plugin import SimplePlugin
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
from tests.contrib.streams.fake_extension import fake_client
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


async def test_unknown_backend_raises_before_any_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(
        streams.StreamConfigError, match="no Redis Stream backend redis"
    ):
        await redis.create(client, Events, id="chat-1", backend="redis")
    with pytest.raises(streams.StreamConfigError, match="It has fake"):
        client.stream(Events, id="chat-1", backend="redis")
    assert not extension.streams and not extension.namespaces
    stream = await redis.create(client, Events, id="chat-1")
    assert stream.backend == "fake"


async def test_client_without_backend_has_only_native(client: Client):
    assert client.stream(Events, id="chat-1").backend == "native"
    with pytest.raises(streams.StreamConfigError, match="no Redis Stream backend"):
        await redis.create(client, Events, id="chat-1")


async def test_decoded_client_stream_has_no_client(client: Client):
    client, _ = fake_client(client)
    stream = await redis.create(client, Events, id="chat-2")
    converter = DataConverter.default.payload_converter
    decoded = converter.from_payload(
        converter.to_payload(stream), streams.ClientStream[Events]
    )
    assert decoded == stream
    with pytest.raises(streams.StreamConfigError, match="Client.stream"):
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
    await redis.create(client, Events, id="agent-1:first:n")
    stream = handle.stream(streams.with_name(Events, "n"))
    with pytest.raises(RuntimeError, match="not known yet"):
        stream.id
    assert await stream.get_offset() == 0
    assert stream.id == "agent-1:first:n"
    assert stream.first_execution_run_id == "first"
    assert await stream.get_offset() == 0
    # The handle keeps the first run id, so its other Streams make no call.
    other = handle.stream(streams.with_name(Events, "n"))
    assert await other.get_offset() == 0
    assert calls == [1]


async def test_failed_first_run_lookup_is_tried_again(client: Client):
    client, _ = fake_client(client)
    handle = client.get_workflow_handle("agent-4")
    describes = []

    async def describe(**_: Any) -> Any:
        describes.append(1)
        if len(describes) == 1:
            raise RuntimeError("the Workflow has not started yet")
        info = SimpleNamespace(first_run_id="run-4")
        return SimpleNamespace(
            raw_description=SimpleNamespace(workflow_execution_info=info)
        )

    setattr(handle, "describe", describe)
    await redis.create(client, Events, id="agent-4:run-4:events")
    stream = handle.stream(Events)
    with pytest.raises(RuntimeError, match="not started"):
        await stream.get_offset()
    assert await stream.get_offset() == 0
    assert stream.id == "agent-4:run-4:events"
    # The handle keeps the id it found, so its other Streams make no call.
    assert await handle.stream(Events).get_offset() == 0
    assert describes == [1, 1]


async def test_cancelled_first_run_lookup_is_tried_again(client: Client):
    client, _ = fake_client(client)
    handle = client.get_workflow_handle("agent-5")
    started = asyncio.Event()

    async def hangs(**_: Any) -> Any:
        started.set()
        await asyncio.Event().wait()

    setattr(handle, "describe", hangs)
    stream = handle.stream(Events)
    offset = asyncio.ensure_future(stream.get_offset())
    await started.wait()
    # The Describe call is cancelled, such as when the loop that ran it stops.
    lookup = _stream_client._FIRST_RUNS[handle]
    assert isinstance(lookup, asyncio.Future)
    lookup.cancel()
    with pytest.raises(asyncio.CancelledError):
        await offset

    async def describe(**_: Any) -> Any:
        info = SimpleNamespace(first_run_id="run-5")
        return SimpleNamespace(
            raw_description=SimpleNamespace(workflow_execution_info=info)
        )

    setattr(handle, "describe", describe)
    await redis.create(client, Events, id="agent-5:run-5:events")
    assert await stream.get_offset() == 0


@workflow.defn
class WriteToStream:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> None:
        await stream.topic(Events.turns).publish(Turn("workflow"))


async def test_client_stream_takes_workflow_writes(stream_client: Client):
    stream = await stream_client.create_stream(Events, id=f"chat-{uuid.uuid4()}")
    assert await stream.get_offset() == 0
    first = await stream.topic(Events.turns).publish(Turn("client"))
    last = await stream.topic(Events.tokens).publish("a", "b")
    assert last > first
    async with new_worker(stream_client, WriteToStream) as worker:
        await stream_client.execute_workflow(
            WriteToStream.run,
            streams.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    await stream.close(result="bye")
    items = [item async for item in stream.subscribe()]
    data = [item.data for item in items if isinstance(item, streams.Record)]
    assert data == [Turn("client"), "a", "b", Turn("workflow")]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "bye"

    # Another handle of the same Stream reads it, from an offset and by Topic.
    again = stream_client.stream(Events, id=stream.id)
    resumed = [
        item.data
        async for item in again.subscribe(Events.tokens, offset=first + 1)
        if isinstance(item, streams.Record)
    ]
    assert resumed == ["a", "b"]
    by_prefix = [
        item.data
        async for item in again.subscribe(topic_prefixes=["tok"])
        if isinstance(item, streams.Record)
    ]
    assert by_prefix == ["a", "b"]
    only_turns = [item async for item in again.subscribe(again.topic(Events.turns))]
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
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish(*prompt.split())
        await stream.close(result=prompt.upper())
        return prompt.upper()


async def test_client_reads_a_workflow_stream(stream_client: Client):
    async with new_worker(stream_client, Agent) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = handle.stream(Events)
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


async def test_client_creates_a_stream_on_its_default_backend(stream_client: Client):
    # A client owns the Stream it creates, as a Workflow and a Standalone Activity own theirs.
    # It goes on the default backend: native, unless a registered backend has default=True.
    lane, _ = _plugin.client_backend(stream_client, None)
    stream_id = f"own-{uuid.uuid4()}"
    stream = await stream_client.create_stream(Events, id=stream_id)
    assert (stream.backend, stream.workflow_id) == (lane, None)
    await stream.topic(Events.tokens).publish("a", "b")
    # A create of the same id and Retention finds the Stream.
    again = await stream_client.create_stream(Events, id=stream_id, backend=lane)
    await again.topic(Events.tokens).publish("c")
    await again.close(result="done")
    items = [
        item async for item in stream_client.stream(Events, id=stream_id).subscribe()
    ]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "a",
        "b",
        "c",
    ]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"
    with pytest.raises(streams.StreamClosedError):
        await stream_client.create_stream(Events, id=stream_id)
    with pytest.raises(streams.StreamConfigError, match="elsewhere"):
        await stream_client.create_stream(Events, id=stream_id, backend="elsewhere")


async def test_async_with_closes_a_client_stream(stream_client: Client):
    async with stream_client.create_stream(Events, id=f"block-{uuid.uuid4()}") as done:
        await done.topic(Events.tokens).publish("a")
    failed = stream_client.stream(Events, id=f"block-{uuid.uuid4()}")
    with pytest.raises(ValueError):
        async with stream_client.create_stream(Events, id=failed.id):
            raise ValueError("turn failed")
    async with await stream_client.create_stream(
        Events, id=f"block-{uuid.uuid4()}"
    ) as early:
        await early.close(result="early")
    cancelled = stream_client.stream(Events, id=f"block-{uuid.uuid4()}")
    with pytest.raises(asyncio.CancelledError):
        async with stream_client.create_stream(Events, id=cancelled.id):
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
    stream = await stream_client.create_stream(Events, id=f"resume-{uuid.uuid4()}")
    await stream.topic(Events.tokens).publish("a")
    turn = await stream.topic(Events.turns).publish(Turn("model"))
    tokens = stream.subscribe(Events.tokens, wait_new_records=False)
    assert tokens.next_offset == 0
    assert [item.data async for item in tokens if isinstance(item, streams.Record)] == [
        "a"
    ]
    # The page ends past the Record that the Topic filter left out.
    assert tokens.next_offset == turn + 1
    new = stream.subscribe(offset=streams.END, wait_new_records=False)
    assert new.next_offset is None
    assert [item async for item in new] == []
    assert new.next_offset == turn + 1
    await stream.topic(Events.tokens).publish("b")
    resumed = stream.subscribe(offset=tokens.next_offset, wait_new_records=False)
    assert [
        item.data async for item in resumed if isinstance(item, streams.Record)
    ] == ["b"]


class _EncodesCommonAttributes(DefaultFailureConverter):
    def __init__(self) -> None:
        super().__init__(encode_common_attributes=True)


async def test_closed_failure_decodes_with_the_clients_converter(stream_client: Client):
    # The message goes into the encoded attributes, which only the payload converter reads.
    config = stream_client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, failure_converter_class=_EncodesCommonAttributes
    )
    client = Client(**config)
    stream = await client.create_stream(Events, id=f"failed-{uuid.uuid4()}")
    await stream.close(
        failure=ApplicationError("tool failed", "detail", {"k": 1}, type="Tool")
    )
    [closed] = [item async for item in stream.subscribe()]
    assert isinstance(closed, streams.Closed)
    assert isinstance(closed.failure, ApplicationError)
    assert closed.failure.message == "tool failed" and closed.failure.type == "Tool"
    assert closed.failure.details == ("detail", {"k": 1})


async def test_no_wait_read_goes_past_records_of_other_topics(stream_client: Client):
    # A Store scans at most 10,000 entries per call, so the first pages of this read are empty.
    stream = await stream_client.create_stream(Events, id=f"scan-{uuid.uuid4()}")
    for _ in range(11):
        await stream.topic(Events.tokens).publish(*["x"] * 1000)
    turn = await stream.topic(Events.turns).publish(Turn("model"))
    turns = stream.subscribe(Events.turns, wait_new_records=False)
    assert [item.data async for item in turns if isinstance(item, streams.Record)] == [
        Turn("model")
    ]
    assert turns.next_offset == turn + 1


async def test_last_reads_the_newest_records_then_follows(stream_client: Client):
    stream = await stream_client.create_stream(Events, id=f"last-{uuid.uuid4()}")
    tokens = stream.topic(Events.tokens)
    await tokens.publish("a", "b", "c")
    await stream.topic(Events.turns).publish(Turn("model"))
    # A chat view shows the last few Records first, then follows.
    tail = stream.subscribe(tokens, last=2)
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
    with pytest.raises(ValueError, match="no offset"):
        stream.subscribe(last=2, offset=0)


async def test_read_type_decodes_topics_outside_the_definition(stream_client: Client):
    stream = await stream_client.create_stream(Events, id=f"typed-{uuid.uuid4()}")
    await stream.topic("extra", type=Turn).publish(Turn("extra"))
    await stream.topic(Events.tokens).publish("a")

    async def read(**options: Any) -> list[object]:
        items = stream.subscribe(wait_new_records=False, **options)
        return [item.data async for item in items if isinstance(item, streams.Record)]

    assert await read() == [{"kind": "extra", "text": ""}, "a"]
    assert await read(type=Turn) == [Turn("extra"), "a"]


async def test_buffered_publisher_keeps_the_order_across_topics(stream_client: Client):
    stream = await stream_client.create_stream(Events, id=f"buffered-{uuid.uuid4()}")
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


@streams.defn(name="events")
class LooseEvents:
    turns = streams.topic(str)
    tokens = streams.topic(str)


async def test_record_that_fails_to_decode_raises_and_resumes_past_it(
    stream_client: Client,
):
    stream_id = f"poison-{uuid.uuid4()}"
    writer = await stream_client.create_stream(LooseEvents, id=stream_id)
    bad = await writer.topic(LooseEvents.turns).publish("not a turn")
    await writer.topic(LooseEvents.tokens).publish("after")
    reader = stream_client.stream(Events, id=stream_id)
    read = reader.subscribe(wait_new_records=False)
    with pytest.raises(streams.StreamError, match=f"offset {bad} of the Topic turns"):
        await anext(read)
    assert read.next_offset == bad + 1
    resumed = reader.subscribe(offset=read.next_offset, wait_new_records=False)
    assert [
        item.data async for item in resumed if isinstance(item, streams.Record)
    ] == ["after"]


async def test_async_with_refuses_a_workflow_stream_before_the_block(
    stream_client: Client,
):
    # The close at the block end would raise and hide the block's own error.
    handle = stream_client.get_workflow_handle(
        "agent-1", first_execution_run_id="run-1"
    )
    ran = False
    with pytest.raises(streams.StreamNotOwnerError):
        async with handle.stream(Events):
            ran = True
    assert not ran


async def test_record_over_the_store_limit_raises_too_large(stream_client: Client):
    # The client's writer splits a batch at the Store's limit, so only a Record can be over.
    stream = await stream_client.create_stream(Events, id=f"large-{uuid.uuid4()}")
    with pytest.raises(streams.StreamTooLargeError) as refused:
        await stream.topic(Events.tokens).publish("x" * 2_200_000)
    assert refused.value.reason == "stream_too_large" and refused.value.non_retryable
    assert await stream.get_offset() == 0


async def test_read_keeps_no_offsets_of_client_records(stream_client: Client):
    # Nothing withdraws a client's Records, so a long UI read of them keeps no state.
    stream = await stream_client.create_stream(Events, id=f"chat-{uuid.uuid4()}")
    await stream.topic(Events.tokens).publish("a", "b", "c")
    read = stream.subscribe(wait_new_records=False)
    assert [item.data async for item in read if isinstance(item, streams.Record)] == [
        "a",
        "b",
        "c",
    ]
    assert read._shown == {}


async def test_empty_stream_id_raises_with_no_call(client: Client):
    client, extension = fake_client(client)
    with pytest.raises(ValueError, match="id is empty"):
        client.stream(Events, id="")
    with pytest.raises(ValueError, match="id is empty"):
        client.stream(Events, stream=streams.WorkflowStream(id=""))
    assert not extension.namespaces


async def test_create_of_an_expired_stream_raises(stream_client: Client):
    # An open client Stream expires one Retention after its last write.
    stream_id = f"expired-{uuid.uuid4()}"
    retention = timedelta(milliseconds=200)
    stream = await stream_client.create_stream(
        Events, id=stream_id, retention=retention
    )
    await stream.topic(Events.tokens).publish("old")
    await asyncio.sleep(0.5)
    with pytest.raises(streams.StreamExpiredError):
        await stream_client.create_stream(Events, id=stream_id, retention=retention)


async def test_named_producer_fences_its_earlier_epoch(stream_client: Client):
    stream = await stream_client.create_stream(Events, id=f"named-{uuid.uuid4()}")
    # A server that restarts writes as one name with a higher epoch, so the zombie it
    # replaces can't write after it.
    before = stream.producer("ingest", epoch=1)
    await before.topic(Events.tokens).publish("before restart")
    after = stream.producer("ingest", epoch=2)
    await after.topic(Events.tokens).publish("after restart")
    with pytest.raises(streams.StreamFencedError):
        await before.topic(Events.tokens).publish("zombie")
    # The earlier epoch's Records are real messages, so nothing withdraws them.
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert [type(item) for item in items] == [streams.Record, streams.Record]
    with pytest.raises(ValueError):
        stream.producer("ingest", epoch=0)


async def test_unresolved_workflow_stream_compares_and_prints(client: Client):
    client, _ = fake_client(client)
    handle = client.get_workflow_handle("agent-2")
    first = handle.stream(Events)
    same = handle.stream(Events)
    other = handle.stream(streams.with_name(Events, "other"))
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
    await redis.create(client, Events, id="agent-2:run-2:events")
    # Calls that start together share one Describe call.
    assert await asyncio.gather(first.get_offset(), first.get_offset()) == [0, 0]
    assert describes == [1]
    assert first.id == "agent-2:run-2:events"
    assert hash(first) == key


async def test_registration_rules(client: Client):
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(),
        MemoryStreamBackend(name="b", default=True),
    ]
    two = Client(**config)
    assert (await redis.create(two, Events, id="d-1")).backend == "b"
    assert two.stream(Events, id="d-1").backend == "b"
    # The default is part of Workflow code, so it must not follow the order of the list.
    for backends in (
        [MemoryStreamBackend(), MemoryStreamBackend(name="b")],
        [
            MemoryStreamBackend(default=True),
            MemoryStreamBackend(name="b", default=True),
        ],
    ):
        config["stream_backends"] = backends
        with pytest.raises(streams.StreamConfigError, match="exactly one"):
            Client(**config)
    config["stream_backends"] = [MemoryStreamBackend(), MemoryStreamBackend()]
    with pytest.raises(streams.StreamConfigError, match="two Stream backends"):
        Client(**config)
    # A Worker serves every backend of its client, and so does a Replayer.
    config["stream_backends"] = [
        MemoryStreamBackend(),
        MemoryStreamBackend(name="b", default=True),
    ]
    new_worker(Client(**config), Agent)
    Replayer(workflows=[Agent], stream_backends=config["stream_backends"])
    # A generator of backends works too, and a copy of the client keeps them.
    config["stream_backends"] = cast(
        Any, (b for b in [MemoryStreamBackend(default=True)])
    )
    from_generator = Client(**config)
    assert from_generator.stream(Events, id="d-3").backend == "memory"
    assert (
        Client(**from_generator.config()).stream(Events, id="d-3").backend == "memory"
    )


async def test_only_a_redis_backend_polls_activities(client: Client):
    # The native close is a command, so a Worker with no Redis backend polls no Activities and
    # shares its task queue with any other Worker. The Redis close is a regular Activity.
    def polled(worker_client: Client) -> list[Any]:
        worker = Worker(
            worker_client, task_queue=f"q-{uuid.uuid4()}", workflows=[Agent]
        )
        return list(worker.config(active_config=True).get("activities") or [])

    assert polled(client) == []
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend(default=True)]
    assert polled(Client(**config)) == [_writes.close_stream]
    config["stream_backends"] = [
        streams.NativeStreamBackend(retention=timedelta(days=1))
    ]
    assert polled(Client(**config)) == []


async def test_worker_that_polls_no_activities_warns_of_the_close(
    client: Client, caplog: pytest.LogCaptureFixture
):
    # The close of a Workflow's Stream is a regular Activity, which another Worker may run.
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend(default=True)]
    with caplog.at_level(logging.WARNING, logger="temporalio.common.streams"):
        async with new_worker(Client(**config), Agent, no_remote_activities=True):
            pass
    assert "polls no Activities" in caplog.text


async def test_backend_is_not_a_plugin(client: Client):
    # The connect refuses it before it connects, so the unreachable target doesn't matter.
    with pytest.raises(streams.StreamConfigError, match="not a plugin"):
        await Client.connect("localhost:1", plugins=[MemoryStreamBackend()])
    config = client.config()
    config["plugins"] = [MemoryStreamBackend()]
    with pytest.raises(streams.StreamConfigError, match="not a plugin"):
        Client(**config)
    with pytest.raises(streams.StreamConfigError, match="not a plugin"):
        new_worker(client, Agent, plugins=[RedisStreamBackend("redis://-")])
    with pytest.raises(streams.StreamConfigError, match="not a plugin"):
        Replayer(workflows=[Agent], plugins=[MemoryStreamBackend()])


async def test_worker_plugin_keeps_the_streams_passthrough(client: Client):
    # The backends configure the Worker after its plugins, so a plugin that replaces the
    # Workflow runner doesn't drop the passthrough.
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend(default=True)]
    plugin = SimplePlugin("replaces-runner", workflow_runner=SandboxedWorkflowRunner())
    worker = new_worker(Client(**config), Agent, plugins=[plugin])
    runner = worker.config(active_config=True).get("workflow_runner")
    assert isinstance(runner, SandboxedWorkflowRunner)
    assert "temporalio.common.streams" in runner.restrictions.passthrough_modules


async def test_default_is_native_with_no_default_backend(client: Client):
    # Every client has the native backend with no registration, and makes no call for it.
    assert client.stream(Events, id="d-2").backend == "native"
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend()]
    one = Client(**config)
    assert one.stream(Events, id="d-2").backend == "native"
    assert one.get_workflow_handle("agent-3").stream(Events).backend == "native"
    assert one.stream(Events, id="d-2", backend="memory").backend == "memory"
    # The Redis create picks the client's only Redis backend, and memory stands in for it.
    assert (await redis.create(one, Events, id="d-2")).backend == "memory"
    # A registered NativeStreamBackend configures native, and only it takes that name.
    config["stream_backends"] = [
        streams.NativeStreamBackend(default=True),
        MemoryStreamBackend(),
    ]
    assert Client(**config).stream(Events, id="d-2").backend == "native"
    config["stream_backends"] = [MemoryStreamBackend(name="native")]
    with pytest.raises(streams.StreamConfigError, match="native backend's"):
        Client(**config)


async def test_stream_takes_the_handle_backend_or_the_default(client: Client):
    config = client.config()
    config["stream_backends"] = [
        MemoryStreamBackend(name="a", default=True),
        MemoryStreamBackend(name="b"),
    ]
    two = Client(**config)
    created = await redis.create(two, Events, id="moving", backend="b")
    await created.topic(Events.tokens).publish("on b")
    assert await two.stream(Events, stream=created).get_offset() > 0
    # A handle with no backend name is of the default backend.
    assert two.stream(Events, stream=streams.ClientStream(id="moving")).backend == "a"
    with pytest.raises(ValueError):
        two.stream(Events, stream=created, backend="a")
    with pytest.raises(TypeError):
        two.stream(Events)


@workflow.defn
class CreatesAndCloses:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("from the environment")
        await stream.close()


async def test_workflow_environment_takes_stream_backends():
    async with await WorkflowEnvironment.start_local(
        stream_backends=[MemoryStreamBackend(default=True)]
    ) as env:
        async with new_worker(env.client, CreatesAndCloses) as worker:
            handle = await env.client.start_workflow(
                CreatesAndCloses.run,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            await handle.result()
        items = [item async for item in handle.stream(Events).subscribe()]
    data = [item.data for item in items if isinstance(item, streams.Record)]
    assert data == ["from the environment"]


async def test_only_the_native_backend_keeps_a_standalone_activitys_stream(
    stream_client: Client,
):
    # Nothing closes a Redis Stream when its Activity ends, so it would stay open. So an
    # Activity's Stream is native unless the call names another native backend.
    handle = stream_client.get_activity_handle("activity", run_id="run")
    stream = handle.stream(Events)
    assert stream.backend == "native"
    assert (stream.activity_id, stream.activity_run_id) == ("activity", "run")
    lane, _ = _plugin.client_backend(stream_client, None)
    if lane != "native":
        with pytest.raises(streams.StreamConfigError, match="native backend"):
            handle.stream(Events, backend=lane)
    with pytest.raises(ValueError, match="no run id"):
        stream_client.get_activity_handle("activity").stream(Events)
