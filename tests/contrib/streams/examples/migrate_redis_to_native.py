"""Jetty v2, "Move from Redis to native Streams".

Old Streams stay on Redis, and new ones go to the server. Step 1 runs the old code on a Worker
with only ``RedisStreamPlugin``. Step 2 deploys new code on a Worker with both plugins. The new
code picks ``native`` behind ``workflow.patched``, so a run that started on the old code replays
the same and goes on with its Redis Stream. Each handle carries its backend, so the client reads
each Stream where it is. Redis stays the default, since a change of the default changes the
commands of a replayed create.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.contrib.streams.native import NativeStreamPlugin
from temporalio.contrib.streams.redis import RedisStreamPlugin

from temporalio import workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib import streams
from temporalio.worker import Worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


class _Chat:
    def __init__(self) -> None:
        self._more: list[str] = []

    @workflow.signal
    def more(self, text: str) -> None:
        self._more.append(text)

    async def chat(self, stream: stream_workflow.WorkflowStream[Events]) -> None:
        await stream.topic(Events.tokens).publish("hello")
        await workflow.wait_condition(lambda: bool(self._more))
        await stream.topic(Events.tokens).publish(*self._more)


@workflow.defn(name="Chat")
class ChatBeforeTheMove(_Chat):
    @workflow.run
    async def run(self) -> None:
        await self.chat(await stream_workflow.create_stream(Events))


@workflow.defn(name="Chat")
class Chat(_Chat):
    @workflow.run
    async def run(self) -> None:
        # A run from before the move has no patch marker, so it keeps its Redis Stream.
        backend = "native" if workflow.patched("streams-on-native") else None
        await self.chat(await stream_workflow.create_stream(Events, backend=backend))


async def connect_before(target_host: str, redis_url: str) -> Client:
    return await Client.connect(target_host, plugins=[RedisStreamPlugin(redis_url)])


async def connect_during(target_host: str, redis_url: str) -> Client:
    return await Client.connect(
        target_host,
        plugins=[RedisStreamPlugin(redis_url, default=True), NativeStreamPlugin()],
    )


async def read(client: Client, handle: WorkflowHandle[Any, None]) -> list[str]:
    await handle.result()
    # A UI that reads chats from both sides of the move looks on native first, then Redis.
    stream = streams.StreamClient(client).workflow_stream(
        handle, Events, backend=("native", "redis")
    )
    tokens = [
        item.data
        async for item in stream.subscribe()
        if isinstance(item, streams.Record)
    ]
    return [f"{stream.backend}: {token}" for token in tokens]


async def main(before: Client, during: Client, task_queue: str) -> list[str]:
    # Step 1: the old code starts a chat on Redis.
    async with Worker(before, task_queue=task_queue, workflows=[ChatBeforeTheMove]):
        old = await before.start_workflow(
            ChatBeforeTheMove.run, id=f"chat-{uuid.uuid4()}", task_queue=task_queue
        )
        stream = streams.StreamClient(before).workflow_stream(old, Events)
        async for _ in stream.subscribe(create_wait=timedelta(seconds=30)):
            # The old chat wrote its first Record, so it runs on the old code.
            break
    # Step 2: the new code goes on with the old chat, and starts a new one on the server.
    async with Worker(during, task_queue=task_queue, workflows=[Chat]):
        await old.signal(ChatBeforeTheMove.more, "old chat goes on")
        new = await during.start_workflow(
            Chat.run, id=f"chat-{uuid.uuid4()}", task_queue=task_queue
        )
        await new.signal(Chat.more, "new chat")
        return [*await read(during, old), *await read(during, new)]
