import asyncio
import dataclasses
import shutil
import socket
import subprocess
import uuid
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.common import RawValue, RetryPolicy
from temporalio.contrib import streams
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.worker import UnsandboxedWorkflowRunner
from tests.helpers import new_worker

# Ports that other local services own.
_RESERVED = {7333, 7861, 7931, 6431, 7101, 7102, 7103}


def _free_port() -> int:
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in _RESERVED:
            return port


@pytest.fixture(scope="module")
def redis_url() -> Iterator[str]:
    server = shutil.which("redis-server")
    if server is None:
        pytest.skip("redis-server is not installed")
    port = _free_port()
    process = subprocess.Popen(
        [server, "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
    )
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                import time

                time.sleep(0.05)
        yield f"redis://127.0.0.1:{port}"
    finally:
        process.terminate()
        process.wait()


@pytest.fixture
async def stream_client(client: Client, redis_url: str) -> AsyncIterator[Client]:
    config = client.config()
    config["plugins"] = [RedisStreamPlugin(redis_url, key_prefix=f"t-{uuid.uuid4()}")]
    yield Client(**config)


@dataclass
class Turn:
    kind: str
    text: str = ""


@activity.defn
async def call_model(prompt: str) -> str:
    tokens = streams.activity_stream().topic("tokens", type=str)
    for word in prompt.split():
        tokens.publish(word)
    return prompt.upper()


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        turns = streams.workflow_stream().topic("turns", type=Turn)
        turns.publish(Turn("model"))
        reply = await workflow.execute_activity(
            call_model, prompt, start_to_close_timeout=timedelta(seconds=30)
        )
        turns.publish(Turn("done", reply))
        return reply


async def read_all(stream: streams.ClientStream) -> list[streams.Record | object]:
    return [item async for item in stream.subscribe(idle_timeout=timedelta(seconds=20))]


async def test_workflow_and_activity_write_the_default_stream(stream_client: Client):
    async with new_worker(stream_client, Agent, activities=[call_model]) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = streams.for_handle(handle, wait_for_create=timedelta(seconds=20))
        turns = stream.topic("turns", type=Turn)
        stream.topic("tokens", type=str)
        items = await read_all(stream)
        assert await handle.result() == "HI THERE"
    data = [
        (item.topic, item.data) for item in items if isinstance(item, streams.Record)
    ]
    assert data == [
        ("turns", Turn("model")),
        ("tokens", "hi"),
        ("tokens", "there"),
        ("turns", Turn("done", "HI THERE")),
    ]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "HI THERE"
    only_turns = [item async for item in turns.subscribe()]
    assert [type(item) for item in only_turns] == [
        streams.Record,
        streams.Record,
        streams.Closed,
    ]


@workflow.defn
class Chat:
    @workflow.run
    async def run(self, turns: int) -> None:
        index = streams.workflow_stream().topic("turns", type=int)
        for n in range(turns):
            with streams.create_workflow_stream(f"turn-{n}") as turn:
                index.publish(n)
                turn.topic("tokens", type=str).publish(f"token-{n}")
            await workflow.sleep(timedelta(milliseconds=10))


async def test_scoped_streams_close_at_the_end_of_their_block(stream_client: Client):
    async with new_worker(stream_client, Chat) as worker:
        handle = await stream_client.start_workflow(
            Chat.run, 2, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        turn = streams.for_handle(handle, name="turn-1")
        items = [item async for item in turn.topic("tokens", type=str).subscribe()]
    assert isinstance(items[0], streams.Record) and items[0].data == "token-1"
    assert isinstance(items[-1], streams.Closed)


@workflow.defn
class WriteToRef:
    @workflow.run
    async def run(self, ref: streams.StreamRef) -> None:
        streams.workflow_stream(ref).topic("t", type=str).publish("from workflow")


async def test_client_stream_takes_workflow_writes(stream_client: Client):
    stream = await streams.create_stream(stream_client, f"chat-{uuid.uuid4()}")
    stream.topic("t", type=str).publish("from client")
    await stream.flush()
    async with new_worker(stream_client, WriteToRef) as worker:
        await stream_client.execute_workflow(
            WriteToRef.run,
            stream.ref,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    await stream.close()
    items = [item async for item in stream.subscribe()]
    data = [item.data for item in items if isinstance(item, streams.Record)]
    assert data == ["from client", "from workflow"]
    assert isinstance(items[-1], streams.Closed)


attempts: dict[str, int] = {}


@activity.defn
async def flaky(key: str) -> None:
    attempts[key] = attempts.get(key, 0) + 1
    tokens = streams.activity_stream().topic("tokens", type=str)
    tokens.publish(f"attempt-{attempts[key]}")
    await streams.activity_stream().flush()
    if attempts[key] == 1:
        raise RuntimeError("first attempt fails")


@workflow.defn
class Retrying:
    @workflow.run
    async def run(self, key: str) -> None:
        streams.workflow_stream().topic("turns", type=str).publish("start")
        await workflow.execute_activity(
            flaky,
            key,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )


async def test_activity_retry_withdraws_the_failed_attempt(stream_client: Client):
    key = str(uuid.uuid4())
    async with new_worker(stream_client, Retrying, activities=[flaky]) as worker:
        handle = await stream_client.start_workflow(
            Retrying.run, key, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        items = await read_all(streams.for_handle(handle))
    records = [item for item in items if isinstance(item, streams.Record)]
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [record.data for record in records] == ["start", "attempt-1", "attempt-2"]
    assert len(superseded) == 1
    withdrawn = [record.data for record in records if superseded[0].withdraws(record)]
    assert withdrawn == ["attempt-1"]


@workflow.defn
class PublishThenFailTask:
    @workflow.run
    async def run(self, key: str) -> None:
        turns = streams.workflow_stream().topic("turns", type=str)
        turns.publish("before")
        await workflow.sleep(timedelta(milliseconds=10))
        turns.publish("after")
        if attempts.setdefault(key, 0) == 0:
            attempts[key] = 1
            raise RuntimeError("fail this Workflow Task once")


async def test_failed_workflow_task_is_withdrawn(stream_client: Client):
    key = str(uuid.uuid4())
    # The test's counter must survive the retry of the task, so no sandbox.
    async with new_worker(
        stream_client,
        PublishThenFailTask,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            PublishThenFailTask.run,
            key,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await read_all(streams.for_handle(handle))
    records = [item for item in items if isinstance(item, streams.Record)]
    # The failed task wrote nothing, so the reader gets each Record once.
    assert [record.data for record in records] == ["before", "after"]
    assert not any(isinstance(item, streams.Superseded) for item in items)
    assert isinstance(items[-1], streams.Closed)


@workflow.defn
class Reading:
    @workflow.run
    async def run(self) -> str:
        try:
            streams.workflow_stream().topic("t", type=str).subscribe()
        except TypeError as err:
            return str(err)
        return "read"


async def test_workflow_code_can_not_read(stream_client: Client):
    async with new_worker(stream_client, Reading) as worker:
        result = await stream_client.execute_workflow(
            Reading.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
    assert "can't read" in result
    await asyncio.sleep(0)


@activity.defn
async def local_tokens(count: int) -> None:
    tokens = streams.activity_stream().topic("tokens", type=int)
    for n in range(count):
        tokens.publish(n)


@workflow.defn
class LocalWriter:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_local_activity(
            local_tokens, 2, start_to_close_timeout=timedelta(seconds=30)
        )


async def test_local_activity_writes_the_default_stream(stream_client: Client):
    async with new_worker(stream_client, LocalWriter, activities=[local_tokens]) as w:
        handle = await stream_client.start_workflow(
            LocalWriter.run, id=f"wf-{uuid.uuid4()}", task_queue=w.task_queue
        )
        await handle.result()
        stream = streams.for_handle(handle)
        stream.topic("tokens", type=int)
        items = await read_all(stream)
    records = [item for item in items if isinstance(item, streams.Record)]
    assert [record.data for record in records] == [0, 1]
    producer = records[0].producer
    assert producer is not None and producer.id.startswith("la:")
    assert (
        producer.epoch is not None and (producer.epoch.task_started_event_id or 0) > 0
    )


@workflow.defn
class FailingRun:
    @workflow.run
    async def run(self) -> str:
        turns = streams.workflow_stream().topic("turns", type=str)
        info = workflow.info()
        turns.publish(f"run-{info.attempt}")
        if info.attempt == 1:
            raise ApplicationError("first run fails")
        return "done"


async def test_retry_run_withdraws_the_failed_run(stream_client: Client):
    async with new_worker(stream_client, FailingRun) as worker:
        handle = await stream_client.start_workflow(
            FailingRun.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=10), maximum_attempts=2
            ),
        )
        assert await handle.result() == "done"
        stream = streams.for_handle(handle)
        stream.topic("turns", type=str)
        items = await read_all(stream)
    records = [item for item in items if isinstance(item, streams.Record)]
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [record.data for record in records] == ["run-1", "run-2"]
    assert len(superseded) == 1
    assert [r.data for r in records if superseded[0].withdraws(r)] == ["run-1"]
    # The failed run had retries left, so only the last run closes the Stream.
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "done"


class _Rot13Codec(PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/rot"}, data=p.SerializeToString()[::-1]
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out = []
        for p in payloads:
            if p.metadata.get("encoding") != b"binary/rot":
                out.append(p)
                continue
            inner = Payload()
            inner.ParseFromString(p.data[::-1])
            out.append(inner)
        return out


async def test_records_go_through_the_payload_codec(stream_client: Client):
    config = stream_client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=_Rot13Codec()
    )
    codec_client = Client(**config)
    async with new_worker(codec_client, Agent, activities=[call_model]) as worker:
        handle = await codec_client.start_workflow(
            Agent.run, "a b", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        stream = streams.for_handle(handle)
        stream.topic("turns", type=Turn)
        stream.topic("tokens", type=str)
        decoded = await read_all(stream)
        raw = [
            item
            async for item in streams.for_handle(handle)
            .topic("tokens", type=RawValue)
            .subscribe(wait_new_records=False)
        ]
    data = [item.data for item in decoded if isinstance(item, streams.Record)]
    assert data == [Turn("model"), "a", "b", Turn("done", "A B")]
    first = raw[0]
    assert isinstance(first, streams.Record)
    assert first.data.payload.metadata["encoding"] == b"json/plain"


async def test_read_ends_idle_and_resumes(stream_client: Client):
    stream = await streams.create_stream(stream_client, f"idle-{uuid.uuid4()}")
    stream.topic("t", type=int).publish(1)
    await stream.flush()
    items = [
        item
        async for item in stream.subscribe(idle_timeout=timedelta(milliseconds=200))
    ]
    assert isinstance(items[-1], streams.Idle)
    stream.topic("t", type=int).publish(2)
    await stream.flush()
    resumed = [
        item
        async for item in stream.subscribe(
            from_offset=items[-1].offset, wait_new_records=False
        )
    ]
    assert [item.data for item in resumed if isinstance(item, streams.Record)] == [2]
    assert await stream.get_offset() > items[-1].offset
