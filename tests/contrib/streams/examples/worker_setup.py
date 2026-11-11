"""Streams API proposal, "Worker setup".

The client registers its Stream backends in a ``StreamConfig``, as ``ExternalStorage`` registers
its drivers. The Worker takes its client's config, and serves only one backend. One backend needs
no selector. With more than one, a selector picks the backend of each new Stream. Tests swap
Redis for ``MemoryStreamBackend`` without changing Workflow code. Sync Workflow code, such as
``@workflow.init``, uses the ``_nowait`` calls.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.testing import WorkflowEnvironment
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


async def connect(target_host: str, redis_url: str) -> Client:
    # Retention is an option of the backend, not of a create. One backend needs no selector.
    backend = RedisStreamBackend(redis_url, retention=timedelta(days=7))
    return await Client.connect(
        target_host, stream_config=streams.StreamConfig(backends=[backend])
    )


def migration_config(
    redis_a: streams.StreamBackend, redis_b: streams.StreamBackend
) -> streams.StreamConfig:
    # With more than one backend, a selector picks the backend of each new Stream, as
    # ExternalStorage.driver_selector picks a driver. It returns one of the registered backends.
    return streams.StreamConfig(
        backends=[redis_a, redis_b],
        # redis-b is the migration target.
        backend_selector=lambda ctx: (
            redis_a if ctx.id.startswith("legacy-") else redis_b
        ),
    )


async def connect_while_migrating(target_host: str, url_a: str, url_b: str) -> Client:
    redis_a = RedisStreamBackend(url_a, name="redis-a", retention=timedelta(days=1))
    redis_b = RedisStreamBackend(url_b, name="redis-b", retention=timedelta(days=1))
    return await Client.connect(
        target_host, stream_config=migration_config(redis_a, redis_b)
    )


async def start_test_environment() -> WorkflowEnvironment:
    # A test swaps Redis for memory with no code change, since the client registers the backends.
    return await WorkflowEnvironment.start_time_skipping(
        stream_config=streams.StreamConfig(backends=[MemoryStreamBackend()])
    )


def run_worker(client: Client, task_queue: str) -> Worker:
    # The Worker takes its client's config, so it needs no Stream setup of its own.
    return Worker(client, task_queue=task_queue, workflows=[Greeter])


async def main(client: Client, task_queue: str) -> list[str]:
    async with run_worker(client, task_queue):
        handle = await client.start_workflow(
            Greeter.run, "ada", id=f"greeter-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()
        stream = client.get_stream(await handle.query(Greeter.stream))
    return [r.data async for r in stream.read() if isinstance(r, streams.Record)]


async def create_while_migrating(client: Client) -> tuple[str | None, str | None]:
    suffix = uuid.uuid4()
    new = await client.create_stream(f"chat-{suffix}", payload_type=str)
    legacy = await client.create_stream(f"legacy-{suffix}", payload_type=str)
    return new.ref.backend, legacy.ref.backend
