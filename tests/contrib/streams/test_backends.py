import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.contrib.streams.testing import MemoryStreamPlugin
from temporalio.worker import Worker
from tests.helpers import assert_task_fail_eventually, new_worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


def two_backend_client(
    client: Client, redis_url: str, *, memory_default: bool = False
) -> Client:
    config = client.config()
    config["plugins"] = [
        RedisStreamPlugin(
            redis_url, key_prefix=f"t-{uuid.uuid4()}", default=not memory_default
        ),
        MemoryStreamPlugin(default=memory_default),
    ]
    return Client(**config)


def data(items: Sequence[object]) -> list[object]:
    return [item.data for item in items if isinstance(item, streams.Record)]


@dataclass
class WriteBothInput:
    on_redis: streams.ActivityStream[Events]
    on_memory: streams.ActivityStream[Events]


@activity.defn
async def write_both(input: WriteBothInput) -> None:
    attempt = activity.info().attempt
    await input.on_redis.topic(Events.tokens).publish(f"redis activity {attempt}")
    await input.on_memory.topic(Events.tokens).publish(f"memory activity {attempt}")
    if attempt == 1:
        raise RuntimeError("first attempt fails")


@activity.defn
async def write_memory_locally(stream: streams.ActivityStream[Events]) -> None:
    await stream.topic(Events.tokens).publish("memory local")


@workflow.defn
class WritesBoth:
    @workflow.run
    async def run(self) -> list[str | None]:
        on_redis = await stream_workflow.create_stream(streams.with_name(Events, "a"))
        on_memory = await stream_workflow.create_stream(
            streams.with_name(Events, "b"), backend="memory"
        )
        await on_redis.topic(Events.tokens).publish("redis workflow")
        await on_memory.topic(Events.tokens).publish("memory workflow")
        await workflow.execute_activity(
            write_both,
            WriteBothInput(
                streams.ActivityStream.from_handle(on_redis),
                streams.ActivityStream.from_handle(on_memory),
            ),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )
        await workflow.execute_local_activity(
            write_memory_locally,
            streams.ActivityStream.from_handle(on_memory),
            start_to_close_timeout=timedelta(seconds=10),
        )
        return [on_redis.backend, on_memory.backend]


async def read_all(
    client: Client, handle: WorkflowHandle[Any, Any], name: str, backend: str
) -> list[object]:
    stream = streams.StreamClient(client).workflow_stream(
        handle, streams.with_name(Events, name), backend=backend
    )
    return [item async for item in stream.subscribe(wait_new_records=False)]


async def test_one_worker_serves_two_backends(client: Client, redis_url: str):
    # Each call goes to the backend that the handle names: Workflow buffers, Activity
    # writes, a Local Activity's flush, retry withdrawals and the closes with the run.
    two = two_backend_client(client, redis_url)
    async with new_worker(
        two, WritesBoth, activities=[write_both, write_memory_locally]
    ) as worker:
        handle = await two.start_workflow(
            WritesBoth.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == ["redis", "memory"]
    on_redis = await read_all(two, handle, "a", "redis")
    on_memory = await read_all(two, handle, "b", "memory")
    assert any(isinstance(item, streams.Superseded) for item in on_redis)
    assert any(isinstance(item, streams.Superseded) for item in on_memory)
    assert data(on_redis)[-1] == "redis activity 2"
    assert data(on_memory)[-2:] == ["memory activity 2", "memory local"]
    assert "redis workflow" in data(on_redis)
    assert "memory workflow" in data(on_memory)
    for items in (on_redis, on_memory):
        closed = items[-1]
        assert isinstance(closed, streams.Closed)
        assert closed.result == ["redis", "memory"]
    # Each Stream is only in its own backend.
    with pytest.raises(streams.StreamNotFoundError):
        await read_all(two, handle, "b", "redis")


@workflow.defn
class CreatesDefault:
    @workflow.run
    async def run(self) -> str | None:
        stream = await stream_workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("default")
        return stream.backend


async def test_default_marks_the_worker_default(client: Client, redis_url: str):
    two = two_backend_client(client, redis_url, memory_default=True)
    async with new_worker(two, CreatesDefault) as worker:
        handle = await two.start_workflow(
            CreatesDefault.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "memory"
    assert data(await read_all(two, handle, "events", "memory")) == ["default"]


@activity.defn
async def write_elsewhere(stream: streams.ActivityStream[Events]) -> str:
    try:
        await stream.topic(Events.tokens).publish("lost")
    except streams.StreamConfigError as err:
        return str(err)
    return "written"


@workflow.defn
class PassesElsewhere:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            write_elsewhere,
            streams.ActivityStream[Events](id="s", backend="elsewhere"),
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn
class CreatesElsewhere:
    @workflow.run
    async def run(self) -> None:
        await stream_workflow.create_stream(Events, backend="elsewhere")


async def test_a_backend_that_nobody_registered(client: Client, redis_url: str):
    two = two_backend_client(client, redis_url)
    with pytest.raises(streams.StreamConfigError):
        streams.StreamClient(two).get_handle(Events, id="s", backend="elsewhere")
    async with new_worker(
        two, PassesElsewhere, CreatesElsewhere, activities=[write_elsewhere]
    ) as worker:
        message = await two.execute_workflow(
            PassesElsewhere.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert "The Worker has redis, memory" in message
        handle = await two.start_workflow(
            CreatesElsewhere.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await assert_task_fail_eventually(
            handle, message_contains="The Worker has redis, memory"
        )
        await handle.terminate()


@workflow.defn
class ContinuesOnBoth:
    @workflow.run
    async def run(self, again: bool) -> str:
        if again:
            await stream_workflow.create_stream(streams.with_name(Events, "a"))
            await stream_workflow.create_stream(
                streams.with_name(Events, "b"), backend="memory"
            )
            workflow.continue_as_new(False)
        return "last run"


async def test_continued_run_closes_each_backend(client: Client, redis_url: str):
    # The next run learns the backend of each Stream that closes with the chain.
    two = two_backend_client(client, redis_url)
    async with new_worker(two, ContinuesOnBoth) as worker:
        handle = await two.start_workflow(
            ContinuesOnBoth.run,
            True,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "last run"
    for name, backend in (("a", "redis"), ("b", "memory")):
        items = await read_all(two, handle, name, backend)
        assert isinstance(items[-1], streams.Closed)
        assert items[-1].result == "last run"


def test_two_plugins_with_one_backend_name(client: Client):
    config = client.config()
    config["plugins"] = [MemoryStreamPlugin(default=True), MemoryStreamPlugin()]
    with pytest.raises(ValueError, match="backend memory"):
        Worker(Client(**config), task_queue="q", workflows=[CreatesDefault])
