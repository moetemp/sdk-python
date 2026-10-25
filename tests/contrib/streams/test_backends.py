import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, common, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from tests.helpers import assert_task_fail_eventually, new_worker


@common.defn(name="events")
class Events:
    tokens = common.topic(str)


def two_backend_client(
    client: Client, redis_url: str, *, memory_default: bool = False
) -> Client:
    config = client.config()
    config["stream_backends"] = [
        RedisStreamBackend(
            redis_url, key_prefix=f"t-{uuid.uuid4()}", default=not memory_default
        ),
        MemoryStreamBackend(default=memory_default),
    ]
    return Client(**config)


def data(items: Sequence[object]) -> list[object]:
    return [item.data for item in items if isinstance(item, common.Record)]


@dataclass
class WriteBothInput:
    on_redis: common.ActivityStream[Events]
    on_memory: common.ActivityStream[Events]


@activity.defn
async def write_both(input: WriteBothInput) -> None:
    attempt = activity.info().attempt
    await input.on_redis.topic(Events.tokens).publish(f"redis activity {attempt}")
    await input.on_memory.topic(Events.tokens).publish(f"memory activity {attempt}")
    if attempt == 1:
        raise RuntimeError("first attempt fails")


@activity.defn
async def write_memory_locally(stream: common.ActivityStream[Events]) -> None:
    await stream.topic(Events.tokens).publish("memory local")


@workflow.defn
class WritesBoth:
    @workflow.run
    async def run(self) -> list[str | None]:
        on_redis = await workflow.create_stream(common.with_name(Events, "a"))
        on_memory = await workflow.create_stream(
            common.with_name(Events, "b"), backend="memory"
        )
        await on_redis.topic(Events.tokens).publish("redis workflow")
        await on_memory.topic(Events.tokens).publish("memory workflow")
        await workflow.execute_activity(
            write_both,
            WriteBothInput(
                common.ActivityStream.from_handle(on_redis),
                common.ActivityStream.from_handle(on_memory),
            ),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=10)),
        )
        await workflow.execute_local_activity(
            write_memory_locally,
            common.ActivityStream.from_handle(on_memory),
            start_to_close_timeout=timedelta(seconds=10),
        )
        backends = [on_redis.backend, on_memory.backend]
        await on_redis.close(result=backends)
        await on_memory.close(result=backends)
        return backends


async def read_all(
    handle: WorkflowHandle[Any, Any], name: str, backend: str
) -> list[object]:
    stream = handle.stream(common.with_name(Events, name), backend=backend)
    return [item async for item in stream.subscribe(wait_new_records=False)]


async def test_one_worker_serves_two_backends(client: Client, redis_url: str):
    # Each call goes to the backend that the handle names: Workflow buffers, Activity
    # writes, a Local Activity's flush, retry withdrawals and the closes.
    two = two_backend_client(client, redis_url)
    async with new_worker(
        two, WritesBoth, activities=[write_both, write_memory_locally]
    ) as worker:
        handle = await two.start_workflow(
            WritesBoth.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == ["redis", "memory"]
    on_redis = await read_all(handle, "a", "redis")
    on_memory = await read_all(handle, "b", "memory")
    assert any(isinstance(item, common.Superseded) for item in on_redis)
    assert any(isinstance(item, common.Superseded) for item in on_memory)
    assert data(on_redis)[-1] == "redis activity 2"
    assert data(on_memory)[-2:] == ["memory activity 2", "memory local"]
    assert "redis workflow" in data(on_redis)
    assert "memory workflow" in data(on_memory)
    for items in (on_redis, on_memory):
        closed = items[-1]
        assert isinstance(closed, common.Closed)
        assert closed.result == ["redis", "memory"]
    # Each Stream is only in its own backend.
    with pytest.raises(common.StreamNotFoundError):
        await read_all(handle, "b", "redis")


@workflow.defn
class CreatesDefault:
    @workflow.run
    async def run(self) -> str | None:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("default")
        return stream.backend


async def test_default_marks_the_worker_default(client: Client, redis_url: str):
    two = two_backend_client(client, redis_url, memory_default=True)
    async with new_worker(two, CreatesDefault) as worker:
        handle = await two.start_workflow(
            CreatesDefault.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "memory"
    assert data(await read_all(handle, "events", "memory")) == ["default"]


@activity.defn
async def write_elsewhere(stream: common.ActivityStream[Events]) -> str:
    try:
        await stream.topic(Events.tokens).publish("lost")
    except common.StreamConfigError as err:
        return str(err)
    return "written"


@workflow.defn
class PassesElsewhere:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            write_elsewhere,
            common.ActivityStream[Events](id="s", backend="elsewhere"),
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn
class CreatesElsewhere:
    @workflow.run
    async def run(self) -> None:
        await workflow.create_stream(Events, backend="elsewhere")


async def test_a_backend_that_nobody_registered(client: Client, redis_url: str):
    two = two_backend_client(client, redis_url)
    with pytest.raises(common.StreamConfigError):
        two.stream(Events, id="s", backend="elsewhere")
    async with new_worker(
        two, PassesElsewhere, CreatesElsewhere, activities=[write_elsewhere]
    ) as worker:
        message = await two.execute_workflow(
            PassesElsewhere.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert "The Worker has" in message and "redis, memory" in message
        handle = await two.start_workflow(
            CreatesElsewhere.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await assert_task_fail_eventually(handle, message_contains="redis, memory")
        await handle.terminate()


@workflow.defn
class ContinuesOnBoth:
    @workflow.run
    async def run(self, again: bool) -> str:
        if again:
            await workflow.create_stream(common.with_name(Events, "a"))
            await workflow.create_stream(
                common.with_name(Events, "b"), backend="memory"
            )
            workflow.continue_as_new(False)
        for name in ("a", "b"):
            await workflow.stream_handle(common.with_name(Events, name)).close(
                result="last run"
            )
        return "last run"


async def test_continued_run_closes_each_backend(client: Client, redis_url: str):
    # The next run learns the backend of each Stream that the chain created.
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
        items = await read_all(handle, name, backend)
        assert isinstance(items[-1], common.Closed)
        assert items[-1].result == "last run"
