"""An agent Workflow links each tool call's stream, and a client follows them all.

The agent writes its own events and, for each tool call, a link to the
tool's stream. ``StreamHandle.follow`` reads the agent's stream and each
linked tool stream in one order. A tool's records come after the link that
opens it, and an agent record after a ``join`` waits for the tool's end.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    activity_handle,
    get_stream_handle,
    start_activity_with_stream,
    topic,
    workflow_writer,
)
from temporalio.worker import Worker

EVENTS = topic("events", str)
TOKENS = topic("tokens", str)


@activity.defn
async def lookup(query: str) -> str:
    out = activity_handle().producer(topic=TOKENS)
    for word in f"facts about {query}".split():
        await out.append(word)
    return "done"


@workflow.defn
class LinkingAgent:
    @workflow.run
    async def run(self, queries: list[str]) -> None:
        events = workflow_writer(EVENTS)
        events.publish("plan")
        for index, query in enumerate(queries):
            call_id = f"lookup-{index}"
            handle, ref = start_activity_with_stream(
                lookup,
                query,
                activity_id=call_id,
                start_to_close_timeout=timedelta(seconds=30),
                topic=TOKENS,
            )
            events.link(ref, name=call_id)
            await handle
            events.join(name=call_id)
        events.publish("answer")


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "agent-1"
) -> list[str]:
    """Run the agent and return what a client follower saw, as ``<source>:<value>``.

    ``client`` must carry the stream store, for example
    ``Client.connect(..., plugins=[RedisStreams("redis://localhost:6379")])``.
    """
    async with Worker(
        client, task_queue=task_queue, workflows=[LinkingAgent], activities=[lookup]
    ):
        await client.execute_workflow(
            LinkingAgent.run,
            ["streams", "links"],
            id=workflow_id,
            task_queue=task_queue,
        )
        seen: list[str] = []
        stream = get_stream_handle(client, workflow_id, topic=EVENTS)
        async for item in stream.follow():
            source = item.source.activity_id or "agent"
            if item.record is not None and item.record.kind is RecordKind.DATA:
                seen.append(f"{source}:{item.record.value}")
        return seen
