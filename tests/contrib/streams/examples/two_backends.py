"""Jetty v2, "Two backends in one Worker".

The client has ``RedisStreamPlugin`` and ``NativeStreamPlugin``, and the Worker takes both from
it. Each Stream handle carries its backend name, so every call goes to the backend that keeps
the Stream. A create with no ``backend=`` goes to the default one, here Redis. Tests swap a
plugin for ``MemoryStreamPlugin(name="native")`` with no change to Workflow code.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.contrib.streams.native import NativeStreamPlugin
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.worker import Worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


# Two Streams of one definition, under their own names.
Audit = streams.with_name(Events, "audit")
Live = streams.with_name(Events, "live")


@dataclass
class SummarizeInput:
    audit: streams.ActivityStream[Events]
    live: streams.ActivityStream[Events]


@activity.defn
async def summarize(input: SummarizeInput) -> str:
    # One Activity writes to both Streams. Each write goes to its own backend.
    await input.live.topic(Events.tokens).publish("summary")
    await input.audit.topic(Events.tokens).publish("summarized")
    return "done"


@workflow.defn
class TwoStreams:
    @workflow.run
    async def run(self) -> str:
        # The audit log stays on the default backend, Redis. The live view is on the server.
        audit = await stream_workflow.create_stream(Audit)
        live = await stream_workflow.create_stream(Live, backend="native")
        await audit.topic(Events.tokens).publish("started")
        await live.topic(Events.tokens).publish("hello", "there")
        return await workflow.execute_activity(
            summarize,
            SummarizeInput(
                streams.ActivityStream.from_handle(audit),
                streams.ActivityStream.from_handle(live),
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )


async def connect(target_host: str, redis_url: str) -> Client:
    return await Client.connect(
        target_host, plugins=[RedisStreamPlugin(redis_url), NativeStreamPlugin()]
    )


async def main(client: Client, task_queue: str) -> dict[str, list[str]]:
    async with Worker(
        client, task_queue=task_queue, workflows=[TwoStreams], activities=[summarize]
    ):
        handle = await client.start_workflow(
            TwoStreams.run, id=f"two-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()
    stream_client = streams.StreamClient(client)
    read: dict[str, list[str]] = {}
    for name, definition, backend in (
        ("audit", Audit, "redis"),
        ("live", Live, "native"),
    ):
        stream = stream_client.workflow_stream(handle, definition, backend=backend)
        read[name] = [
            item.data
            async for item in stream.subscribe()
            if isinstance(item, streams.Record)
        ]
    return read
