"""Scenario 4 with a Worker follower: a Slack connector follows an agent in another namespace.

The agent runs in namespace ``streams-b``, as in ``n4``: it publishes a
message, idles 10 s, publishes another, and closes its stream. The connector
lives in ``default``. It is a standalone Nexus operation that follows the
agent's ``message`` topic, and a Worker that posts each message from
``on_records``. The Worker in ``default`` holds no store of ``streams-b``,
so it reads through the stream service on the agent's endpoint, over the
Nexus HTTP ingress. Posts are at least once, so the connector drops repeats
by cursor.

Left out: a topic filter. One callback per record.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.u4_sano_connector``. The
HTTP ingress is at the gRPC port plus one, or at ``TEMPORAL_HTTP_ADDRESS``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from temporalio.client import Client
from temporalio.contrib.streams.nexus import (
    FollowBatch,
    FollowOutcome,
    StreamFollowerService,
    TemporalStreamsHandler,
    TemporalStreamsHttpClient,
    follow_stream,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NAMESPACE_B,
    NewStore,
    connect,
    endpoint,
    run,
    stream_service_url,
    two_namespaces,
)
from tests.contrib.streams.samples.scenarios.n4_slack_connector import (
    Agent,
    AgentHandler,
    AgentService,
)
from tests.helpers.nexus import end_standalone_operation


@dataclass
class SlackConnector:
    """Posts each message once, by its cursor."""

    posted: dict[str, str] = field(default_factory=dict)
    outcome: FollowOutcome | None = None
    closed: asyncio.Event = field(default_factory=asyncio.Event)

    async def on_records(self, batch: FollowBatch[str]) -> None:
        for record in batch.records:
            if record.cursor.token in self.posted:
                continue
            print(f"[slack] {record.value}")
            self.posted[record.cursor.token] = record.value or ""

    async def on_close(self, outcome: FollowOutcome) -> None:
        self.outcome = outcome
        self.closed.set()


@dataclass
class Outcome:
    posts: list[str]
    close_result: Any


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "connector-u4",
    new_store: NewStore,
    idle_seconds: float = 10.0,
) -> Outcome:
    """Run the agent and the standalone connector, and return the posts.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier, NexusHandler callbacks on standalone operations, and the
    namespace ``streams-b``.
    """
    b = await two_namespaces(client, new_store().notify_on_append())
    a = await connect(client, None)
    b_queue = f"{task_queue}-b"
    name = await endpoint(client, b_queue, namespace=NAMESPACE_B)
    connector = SlackConnector()
    followers = StreamFollowerService(
        {"slack": connector},
        client=a,
        transport=TemporalStreamsHttpClient(await stream_service_url(client, name)),
    )
    async with (
        Worker(
            b,
            task_queue=b_queue,
            workflows=[Agent],
            nexus_service_handlers=[AgentHandler(), TemporalStreamsHandler()],
        ),
        Worker(a, task_queue=task_queue, nexus_service_handlers=[followers]),
    ):
        followed = await follow_stream(
            a,
            endpoint=name,
            service=AgentService,
            operation=AgentService.follow,
            arg=idle_seconds,
            follower="slack",
            task_queue=task_queue,
            id=workflow_id,
        )
        try:
            await asyncio.wait_for(connector.closed.wait(), idle_seconds + 30)
        finally:
            # Ends the operation while the Worker runs, so a failed run leaves
            # no callback retried against a task queue no one polls.
            await end_standalone_operation(a, followed, connector.closed.is_set)
    assert connector.outcome is not None
    return Outcome(
        posts=list(connector.posted.values()),
        close_result=connector.outcome.result,
    )


if __name__ == "__main__":
    run(main)
