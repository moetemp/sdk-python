"""Brook API proposal, "Worker setup".

The Stream plugin goes on the client, and the Worker takes the client's plugins. Each plugin
registers a backend name. Tests use ``MemoryStreamPlugin`` with no change to Workflow code.
Sync Workflow code, such as ``@workflow.init``, uses the ``_nowait`` calls.
"""

from __future__ import annotations

import uuid

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.contrib.streams.testing import MemoryStreamPlugin
from temporalio.worker import Worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


@workflow.defn
class Greeter:
    @workflow.init
    def __init__(self, name: str) -> None:
        # The init can't await, so it creates and publishes with the _nowait calls.
        self.stream = stream_workflow.create_stream_nowait(Events)
        self.stream.topic(Events.tokens).publish_nowait(f"hello {name}")

    @workflow.run
    async def run(self, name: str) -> str:
        await self.stream.topic(Events.tokens).publish(f"bye {name}")
        return name


async def connect_with_redis(target_host: str, redis_url: str) -> Client:
    return await Client.connect(target_host, plugins=[RedisStreamPlugin(redis_url)])


async def connect_for_tests(target_host: str) -> Client:
    return await Client.connect(target_host, plugins=[MemoryStreamPlugin()])


async def connect_while_migrating(target_host: str, url_a: str, url_b: str) -> Client:
    # Two backends during a move. Creates with no backend= go to the default one.
    return await Client.connect(
        target_host,
        plugins=[
            RedisStreamPlugin(url_a, name="redis-a", default=True),
            RedisStreamPlugin(url_b, name="redis-b"),
        ],
    )


def run_worker(client: Client, task_queue: str) -> Worker:
    # The Worker takes the client's plugins, so it needs no Stream setup of its own.
    return Worker(client, task_queue=task_queue, workflows=[Greeter])


async def main(client: Client, task_queue: str) -> list[str]:
    async with run_worker(client, task_queue):
        handle = await client.start_workflow(
            Greeter.run, "ada", id=f"greeter-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()
    stream = streams.StreamClient(client).workflow_stream(handle, Events)
    return [
        item.data
        async for item in stream.subscribe()
        if isinstance(item, streams.Record)
    ]


async def create_on_second_backend(client: Client) -> str | None:
    stream = await streams.StreamClient(client).create(
        Events, id=f"chat-{uuid.uuid4()}", backend="redis-b"
    )
    return stream.backend
