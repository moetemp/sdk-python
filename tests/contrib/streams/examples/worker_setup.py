"""Brook API proposal, "Worker setup".

The Stream backends go on the client with ``stream_backends=``, and the Worker takes the
client's backends. Each backend registers a name. A create with no backend name goes to the
one with ``default=True``. Tests use ``MemoryStreamBackend`` with no change to Workflow code.
Sync Workflow code, such as ``@workflow.init``, uses the ``_nowait`` calls.
"""

from __future__ import annotations

import uuid

from temporalio import common, workflow
from temporalio.client import Client
from temporalio.contrib import stream as redis
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.worker import Worker


@common.defn(name="events")
class Events:
    tokens = common.topic(str)


@workflow.defn
class Greeter:
    @workflow.init
    def __init__(self, name: str) -> None:
        # The init can't await, so it creates and publishes with the _nowait calls.
        self.stream = workflow.create_stream_nowait(Events)
        self.stream.topic(Events.tokens).publish_nowait(f"hello {name}")

    @workflow.run
    async def run(self, name: str) -> str:
        await self.stream.topic(Events.tokens).publish(f"bye {name}")
        await self.stream.close()
        return name


async def connect_with_redis(target_host: str, redis_url: str) -> Client:
    # With no default=True, the default is native, and Workflow code would name Redis with
    # redis.workflow.create_stream.
    return await Client.connect(
        target_host,
        stream_backends=[redis.RedisStreamBackend(redis_url, default=True)],
    )


async def connect_for_tests(target_host: str) -> Client:
    return await Client.connect(
        target_host, stream_backends=[MemoryStreamBackend(default=True)]
    )


async def connect_while_migrating(target_host: str, url_a: str, url_b: str) -> Client:
    # Two backends during a move. Creates with no backend= go to the default one.
    return await Client.connect(
        target_host,
        stream_backends=[
            redis.RedisStreamBackend(url_a, name="redis-a", default=True),
            redis.RedisStreamBackend(url_b, name="redis-b"),
        ],
    )


def run_worker(client: Client, task_queue: str) -> Worker:
    # The Worker takes the client's backends, so it needs no Stream setup of its own.
    return Worker(client, task_queue=task_queue, workflows=[Greeter])


async def main(client: Client, task_queue: str) -> list[str]:
    async with run_worker(client, task_queue):
        handle = await client.start_workflow(
            Greeter.run, "ada", id=f"greeter-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()
    stream = handle.stream(Events)
    return [
        item.data
        async for item in stream.subscribe()
        if isinstance(item, common.Record)
    ]


async def create_on_second_backend(client: Client) -> str | None:
    stream = await redis.create(
        client, Events, id=f"chat-{uuid.uuid4()}", backend="redis-b"
    )
    return stream.backend
