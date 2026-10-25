"""Jetty v2, "Two backends in one Worker".

The client registers ``RedisStreamBackend``, and has the native backend with no registration.
The Worker takes both from it. Each Stream handle carries its backend name, so every call goes
to the backend that keeps the Stream. A create with no ``backend=`` goes to the default one,
here native, since the Redis backend has no ``default=True``. Tests swap Redis for a
``MemoryStreamBackend(name="redis")`` with no change to Workflow code.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, common, workflow
from temporalio.client import Client
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.worker import Worker


@common.defn(name="events")
class Events:
    tokens = common.topic(str)


# Two Streams of one definition, under their own names.
Audit = common.with_name(Events, "audit")
Live = common.with_name(Events, "live")


@dataclass
class SummarizeInput:
    audit: common.ActivityStream[Events]
    live: common.ActivityStream[Events]


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
        # The audit log is on the server, the default backend. The live view is on Redis.
        audit = await workflow.create_stream(Audit)
        live = await workflow.create_stream(Live, backend="redis")
        await audit.topic(Events.tokens).publish("started")
        await live.topic(Events.tokens).publish("hello", "there")
        result = await workflow.execute_activity(
            summarize,
            SummarizeInput(
                common.ActivityStream.from_handle(audit),
                common.ActivityStream.from_handle(live),
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )
        # The server closes the audit log when the run chain ends. The end of a run closes
        # nothing on Redis, so the run closes the live view.
        await live.close(result=result)
        return result


async def connect(target_host: str, redis_url: str) -> Client:
    return await Client.connect(
        target_host, stream_backends=[RedisStreamBackend(redis_url)]
    )


async def main(client: Client, task_queue: str) -> dict[str, list[str]]:
    async with Worker(
        client, task_queue=task_queue, workflows=[TwoStreams], activities=[summarize]
    ):
        handle = await client.start_workflow(
            TwoStreams.run, id=f"two-{uuid.uuid4()}", task_queue=task_queue
        )
        await handle.result()
    read: dict[str, list[str]] = {}
    for name, definition, backend in (
        ("audit", Audit, None),
        ("live", Live, "redis"),
    ):
        stream = handle.stream(definition, backend=backend)
        read[name] = [
            item.data
            async for item in stream.subscribe()
            if isinstance(item, common.Record)
        ]
    return read
