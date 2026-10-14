"""Scenario 4 on Bridge: a Slack connector follows an agent in another namespace.

The agent runs in namespace ``streams-b``. It publishes a message, idles,
publishes another, and closes its stream. A ``Connector`` Workflow in
``default`` follows the agent's ``message`` topic through a stream operation
and posts each message through an Activity, as the ``nexus_slack_router``
sample does. While the agent idles, the connector gets no progress, so after
one read that waits its idle wait and arms the stream, its History has no
Workflow Task in the gap.

Left out: a standalone Nexus operation as the caller, and a Worker Callback
per delivery. Neither is built, so the connector is a small Workflow.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7961
python -m tests.contrib.streams.samples.scenarios.n4_slack_connector``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.contrib.streams import (
    StreamRef,
    close_workflow_stream,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NAMESPACE_B,
    NewStore,
    connect,
    endpoint,
    run,
    two_namespaces,
)

MESSAGE = topic("message", str)


@nexusrpc.service
class AgentService:
    follow: nexusrpc.Operation[float, str]


@workflow.defn
class Agent:
    """Publishes two messages with an idle gap between them."""

    @workflow.run
    async def run(self, idle_seconds: float) -> None:
        messages = workflow_writer(MESSAGE)
        messages.publish("the build is green")
        await workflow.sleep(timedelta(seconds=idle_seconds))
        messages.publish("the release is out")
        await close_workflow_stream(MESSAGE, result="2 messages")


async def open_agent(ctx: Any, idle_seconds: float) -> StreamRef:
    workflow_id = f"agent-{ctx.request_id}"
    await temporalio.nexus.client().start_workflow(
        Agent.run,
        idle_seconds,
        id=workflow_id,
        task_queue=temporalio.nexus.info().task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return StreamRef.for_workflow(workflow_id, topic=MESSAGE.name)


@nexusrpc.handler.service_handler(service=AgentService)
class AgentHandler:
    @nexusrpc.handler.operation_handler
    def follow(self) -> nexusrpc.handler.OperationHandler[float, str]:
        return StreamOperationHandler(open_agent)


#: What the Slack Activity posted, in this process.
posted: list[str] = []


@activity.defn
async def post(text: str) -> None:
    print(f"[slack] {text}")
    posted.append(text)


@workflow.defn
class Connector:
    """Posts each of the agent's messages to Slack."""

    @workflow.run
    async def run(self, endpoint: str, idle_seconds: float) -> str:
        client = workflow.create_nexus_client(service=AgentService, endpoint=endpoint)
        handle = await client.start_operation(AgentService.follow, idle_seconds)
        reader = StreamReader(handle, item_type=str, endpoint=endpoint)
        while (batch := await reader.next()) is not None:
            for text in batch:
                await workflow.execute_activity(
                    post, text, start_to_close_timeout=timedelta(seconds=10)
                )
        return await handle


@dataclass
class Outcome:
    posts: list[str]
    close_result: str
    #: The longest time between two Workflow Tasks the connector's History scheduled.
    longest_quiet_seconds: float


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "connector-n4",
    new_store: NewStore,
    idle_seconds: float = 10.0,
) -> Outcome:
    """Run the agent and the connector, and return the posts and the quiet gap.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier and the namespace ``streams-b``.
    """
    posted.clear()
    b = await two_namespaces(client, new_store().notify_on_append())
    a = await connect(client, None)
    b_queue = f"{task_queue}-b"
    name = await endpoint(client, b_queue, namespace=NAMESPACE_B)
    async with (
        Worker(
            b,
            task_queue=b_queue,
            workflows=[Agent],
            nexus_service_handlers=[AgentHandler(), TemporalStreamsHandler()],
        ),
        Worker(a, task_queue=task_queue, workflows=[Connector], activities=[post]),
    ):
        handle = await a.start_workflow(
            Connector.run,
            args=[name, idle_seconds],
            id=workflow_id,
            task_queue=task_queue,
        )
        result = await asyncio.wait_for(handle.result(), idle_seconds + 30)
        times = [
            event.event_time.ToDatetime()
            for event in (await handle.fetch_history()).events
            if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        ]
        quiet = max(
            (later - earlier).total_seconds()
            for earlier, later in zip(times, times[1:])
        )
        print(f"[connector] the longest gap between Workflow Tasks was {quiet:.1f} s")
        return Outcome(
            posts=list(posted), close_result=result, longest_quiet_seconds=quiet
        )


if __name__ == "__main__":
    run(main)
