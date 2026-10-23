"""The native lane: the same calls against a server that serves the Stream service.

Set ``TEMPORAL_STREAMS_NATIVE_TARGET`` to its address to run it.
"""

import concurrent.futures
import uuid
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.contrib.streams.native import NativeStreamPlugin
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.worker import Replayer
from tests.contrib.streams.test_workflow_reads import (
    Events,
    Reader,
    ReadInput,
    Writer,
)
from tests.helpers import new_worker


@activity.defn
async def write_native(stream: streams.ActivityStream[Events]) -> None:
    await stream.topic(Events.tokens).publish("activity")


@activity.defn
def write_native_sync(stream: streams.SyncActivityStream[Events]) -> None:
    stream.topic(Events.tokens).publish("sync activity")


@workflow.defn
class WritesNativeAndRedis:
    @workflow.run
    async def run(self) -> list[str | None]:
        on_redis = await stream_workflow.create_stream(streams.with_name(Events, "old"))
        on_native = await stream_workflow.create_stream(
            streams.with_name(Events, "new"), backend="native"
        )
        await on_redis.topic(Events.tokens).publish("redis")
        await on_native.topic(Events.tokens).publish("workflow 1", "workflow 2")
        # The Activities write after the run's own writes, as on Redis.
        await workflow.execute_activity(
            write_native,
            streams.ActivityStream.from_handle(on_native),
            start_to_close_timeout=timedelta(seconds=10),
        )
        await workflow.execute_activity(
            write_native_sync,
            streams.SyncActivityStream.from_handle(on_native),
            start_to_close_timeout=timedelta(seconds=10),
        )
        await on_native.topic(Events.tokens).publish("last")
        return [on_redis.backend, on_native.backend]


async def test_native_client_round_trip(native_client: Client):
    stream = await streams.StreamClient(native_client).create(
        Events, id=f"native-{uuid.uuid4()}", auto_close=timedelta(minutes=5)
    )
    assert await stream.topic(Events.tokens).publish("a", "b")
    await stream.close(result="done")
    items = [item async for item in stream.subscribe()]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "a",
        "b",
    ]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"


async def test_native_workflow_reads_and_replays(native_client: Client):
    async with new_worker(native_client, Writer, Reader) as worker:
        writer = await native_client.start_workflow(
            Writer.run, None, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = streams.StreamClient(native_client).workflow_stream(writer, Events)
        reader = await native_client.start_workflow(
            Reader.run,
            ReadInput(stream_workflow.WorkflowStream.from_handle(stream)),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await reader.result() == [
            "turn user hi",
            "a",
            "b",
            "c",
            "closed written",
        ]
    replayer = Replayer(workflows=[Writer, Reader], plugins=[NativeStreamPlugin()])
    await replayer.replay_workflow(await writer.fetch_history())
    await replayer.replay_workflow(await reader.fetch_history())


async def test_redis_and_native_on_a_native_server(
    native_client: Client, redis_url: str
):
    config = native_client.config()
    config["plugins"] = [
        RedisStreamPlugin(redis_url, key_prefix=f"t-{uuid.uuid4()}", default=True),
        NativeStreamPlugin(),
    ]
    two = Client(**config)
    async with new_worker(
        two,
        WritesNativeAndRedis,
        activities=[write_native, write_native_sync],
        activity_executor=concurrent.futures.ThreadPoolExecutor(max_workers=2),
    ) as worker:
        handle = await two.start_workflow(
            WritesNativeAndRedis.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == ["redis", "native"]
    new = streams.StreamClient(two).workflow_stream(
        handle, streams.with_name(Events, "new"), backend="native"
    )
    items = [item async for item in new.subscribe()]
    assert [item.data for item in items if isinstance(item, streams.Record)] == [
        "workflow 1",
        "workflow 2",
        "activity",
        "sync activity",
        "last",
    ]
    assert isinstance(items[-1], streams.Closed)
    assert items[-1].result == ["redis", "native"]
