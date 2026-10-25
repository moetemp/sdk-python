"""Jetty v2, "Move from Redis to native Streams".

Old Streams stay on Redis, and new ones go to the server. The client registers
``RedisStreamBackend`` with ``default=True``, so Workflow code that names no backend keeps
creating on Redis, and the native backend is there with no registration. Step 1 runs the old
code. Step 2 deploys new code that picks ``native`` behind ``workflow.patched``, so a run that
started on the old code replays the same and goes on with its Redis Stream. Redis stays the
default, since a change of the default changes the commands of a replayed create.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from temporalio import common, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.worker import Worker


@common.defn(name="events")
class Events:
    tokens = common.topic(str)


class _Chat:
    def __init__(self) -> None:
        self._more: list[str] = []

    @workflow.signal
    def more(self, text: str) -> None:
        self._more.append(text)

    async def chat(self, stream: common.WorkflowStream[Events]) -> None:
        await stream.topic(Events.tokens).publish("hello")
        await workflow.wait_condition(lambda: bool(self._more))
        await stream.topic(Events.tokens).publish(*self._more)


@workflow.defn(name="Chat")
class ChatBeforeTheMove(_Chat):
    @workflow.run
    async def run(self) -> None:
        await self.chat(await workflow.create_stream(Events))


@workflow.defn(name="Chat")
class Chat(_Chat):
    @workflow.run
    async def run(self) -> None:
        # A run from before the move has no patch marker, so it keeps its Redis Stream.
        backend = "native" if workflow.patched("streams-on-native") else None
        await self.chat(await workflow.create_stream(Events, backend=backend))


async def connect(target_host: str, redis_url: str) -> Client:
    return await Client.connect(
        target_host, stream_backends=[RedisStreamBackend(redis_url, default=True)]
    )


async def read(handle: WorkflowHandle[Any, None]) -> list[str]:
    await handle.result()
    # A UI that reads chats from both sides of the move looks on native first, then Redis.
    for backend in ("native", "redis"):
        stream = handle.stream(Events, backend=backend)
        try:
            tokens = [
                item.data
                async for item in stream.subscribe(wait_new_records=False)
                if isinstance(item, common.Record)
            ]
        except common.StreamNotFoundError:
            continue
        return [f"{backend}: {token}" for token in tokens]
    raise common.StreamNotFoundError(
        f"The chat {handle.id} has no Stream", reason="stream_not_found"
    )


async def main(client: Client, task_queue: str) -> list[str]:
    # Step 1: the old code starts a chat on Redis.
    async with Worker(client, task_queue=task_queue, workflows=[ChatBeforeTheMove]):
        old = await client.start_workflow(
            ChatBeforeTheMove.run, id=f"chat-{uuid.uuid4()}", task_queue=task_queue
        )
        async for _ in old.stream(Events).subscribe(create_wait=timedelta(seconds=30)):
            # The old chat wrote its first Record, so it runs on the old code.
            break
    # Step 2: the new code goes on with the old chat, and starts a new one on the server.
    async with Worker(client, task_queue=task_queue, workflows=[Chat]):
        await old.signal(ChatBeforeTheMove.more, "old chat goes on")
        new = await client.start_workflow(
            Chat.run, id=f"chat-{uuid.uuid4()}", task_queue=task_queue
        )
        await new.signal(Chat.more, "new chat")
        return [*await read(old), *await read(new)]
