"""An agent Workflow runs a tool call that streams to a stream of its own.

The Workflow starts the tool with ``start_activity_with_stream``, which
returns the tool's stream ref with the handle. ``activity_handle()`` in the
tool is then the tool's own stream, and every attempt writes to it. A retry
is reported to readers as ``SUPERSEDED``. The tool does not close the
stream. The agent's Worker closes it when the agent sees the tool end.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    RecordKind,
    StreamRef,
    activity_handle,
    get_stream_handle,
    start_activity_with_stream,
    topic,
)
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

TOKENS = topic("tokens", str)


@activity.defn
async def search(query: str) -> str:
    out = activity_handle().producer(topic=TOKENS)
    words = f"results for {query}".split()
    if activity.info().attempt == 1:
        # The first attempt fails halfway, as a remote call might.
        await out.append(*words[:2])
        raise ApplicationError("the search call dropped")
    await out.append(*words)
    return " ".join(words)


@workflow.defn
class SearchAgent:
    @workflow.run
    async def run(self, query: str) -> str:
        handle, _ = start_activity_with_stream(
            search,
            query,
            activity_id="search-1",
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
            topic=TOKENS,
        )
        return await handle


async def main(client: Client, task_queue: str, *, workflow_id: str = "agent-1") -> str:
    """Run one search in an agent Workflow and return the text the client read.

    ``client`` must carry the stream store, for example
    ``Client.connect(..., plugins=[RedisStreams("redis://localhost:6379")])``.
    """
    async with Worker(
        client, task_queue=task_queue, workflows=[SearchAgent], activities=[search]
    ):
        agent = await client.start_workflow(
            SearchAgent.run, "streams", id=workflow_id, task_queue=task_queue
        )
        # The ref names the agent run and the tool's id, so the client can open
        # it before the tool starts.
        ref = StreamRef.for_workflow_activity(
            agent.id,
            run_id=agent.result_run_id or "",
            activity_id="search-1",
            topic=TOKENS,
        )
        text: list[str] = []
        async for record in get_stream_handle(client, ref).read():
            if record.kind is RecordKind.SUPERSEDED:
                text.clear()
            elif record.kind is RecordKind.DATA and record.value is not None:
                text.append(record.value)
        await agent.result()
        return " ".join(text)
