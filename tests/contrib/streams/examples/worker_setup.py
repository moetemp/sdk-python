"""Streams API proposal, "Worker setup".

Register the Stream backends on the client with ``stream_backends=``. The Worker uses its
client's backend, and serves only one. Each backend registers a name. A create with no backend
name uses the backend with ``default=True``, and ``backend=`` picks another. Tests use
``MemoryStreamBackend`` without changing Workflow code. Sync Workflow code, such as
``@workflow.init``, uses the ``_nowait`` calls.
"""

from __future__ import annotations

import uuid

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.worker import Worker


@workflow.defn
class Greeter:
    @workflow.init
    def __init__(self, name: str) -> None:
        # The init can't await, so it creates and publishes with the _nowait calls.
        self.tokens = workflow.create_stream_nowait(
            f"{workflow.info().workflow_id}-tokens", payload_type=str
        )
        self.tokens.append_nowait(f"hello {name}")

    @workflow.query
    def stream(self) -> streams.StreamRef[str]:
        return self.tokens.ref

    @workflow.run
    async def run(self, name: str) -> str:
        await self.tokens.append(f"bye {name}")
        await self.tokens.close()
        return name


async def connect_with_redis(target_host: str, redis_url: str) -> Client:
    # This SDK has no native backend, so its one Redis backend is the default.
    return await Client.connect(
        target_host, stream_backends=[RedisStreamBackend(redis_url, default=True)]
    )


async def connect_for_tests(target_host: str) -> Client:
    return await Client.connect(
        target_host, stream_backends=[MemoryStreamBackend(default=True)]
    )


async def connect_while_migrating(target_host: str, url_a: str, url_b: str) -> Client:
    # Two backends during a migration. A create with no backend= uses the default one. A
    # Worker serves only one backend, so a Worker gets a client with one backend.
    return await Client.connect(
        target_host,
        stream_backends=[
            RedisStreamBackend(url_a, name="redis-a", default=True),
            RedisStreamBackend(url_b, name="redis-b"),
        ],
    )


def run_worker(client: Client, task_queue: str) -> Worker:
    # The Worker uses its client's one backend, so it needs no Stream setup of its own.
    return Worker(client, task_queue=task_queue, workflows=[Greeter])


async def main(client: Client, task_queue: str) -> list[str]:
    async with run_worker(client, task_queue):
        handle = await client.start_workflow(
            Greeter.run, "ada", id=f"greeter-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()
        stream = client.get_stream(await handle.query(Greeter.stream))
    return [r.data async for r in stream.read() if isinstance(r, streams.Record)]


async def create_on_second_backend(client: Client) -> str | None:
    stream = await client.create_stream(
        f"chat-{uuid.uuid4()}", backend="redis-b", payload_type=str
    )
    return stream.ref.backend
