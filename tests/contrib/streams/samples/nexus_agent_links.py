"""A caller in another namespace follows an agent and its tool calls over Nexus.

The agent runs as a Workflow in namespace ``streams-b``. Each tool call is an
Activity that owns its stream, and the agent writes a link to it in its own
stream, then joins it once the tool returned. The operation ``Agents.run_agent``
starts the agent, or finds it, and hands the caller the agent's stream. The
caller's ``StreamReader`` with ``follow_links=True`` reads the agent's stream
and every tool stream in one order, through the stream service's ``follow``.

Workers in ``streams-b``: one serves ``Agents`` and one the stream service,
and one runs the agent and its tools. The caller's Workflow runs in the
client's namespace and follows directly through ``StreamDirectReads``.

Run against the Bridge live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7961
python -m tests.contrib.streams.samples.nexus_agent_links``.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.contrib.streams import (
    FollowedItem,
    RecordKind,
    StreamRef,
    activity_handle,
    start_activity_with_stream,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.nexus import (
    ClientTransport,
    ReadPosition,
    StreamDirectReads,
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.worker import Replayer, Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NAMESPACE_B,
    NewStore,
    endpoint,
    run,
    two_namespaces,
)

EVENTS = topic("events", str)
TOKENS = topic("tokens", str)


@nexusrpc.service
class Agents:
    run_agent: nexusrpc.Operation[str, str]


@activity.defn
async def search(query: str) -> str:
    out = activity_handle().producer(topic=TOKENS)
    for word in f"{query} results".split():
        await out.append(word)
        await asyncio.sleep(0.2)
    return "ok"


@workflow.defn
class Agent:
    """Plans, calls two tools that stream, and answers."""

    @workflow.run
    async def run(self, queries: list[str]) -> str:
        events = workflow_writer(EVENTS)
        events.publish("plan")
        for index, query in enumerate(queries):
            call = f"tool-{index}"
            handle, ref = start_activity_with_stream(
                search,
                query,
                activity_id=call,
                start_to_close_timeout=timedelta(seconds=30),
                topic=TOKENS,
            )
            events.link(ref, name=call)
            await handle
            events.join(name=call)
        events.publish("answer")
        return "answered"


# When the slow tool appended each token, by agent id. The tool runs in the
# caller's process in the lane, so the test reads this directly.
appended_at: dict[str, dict[str, float]] = {}


@activity.defn
async def slow_search(query: str) -> str:
    """Writes one token every 3 seconds, four in all."""
    out = activity_handle().producer(topic=TOKENS)
    times = appended_at.setdefault(activity.info().workflow_id or "", {})
    for step in range(1, 5):
        await out.append(f"{query}-{step}")
        times[f"tool-0:{query}-{step}"] = time.monotonic()
        await asyncio.sleep(3)
    return "ok"


@workflow.defn
class SlowAgent:
    """Links one slow tool, and writes nothing on its own stream while it runs."""

    @workflow.run
    async def run(self, query: str) -> str:
        events = workflow_writer(EVENTS)
        events.publish("plan")
        handle, ref = start_activity_with_stream(
            slow_search,
            query,
            activity_id="tool-0",
            start_to_close_timeout=timedelta(minutes=2),
            topic=TOKENS,
        )
        events.link(ref, name="tool-0")
        await handle
        events.join(name="tool-0")
        events.publish("answer")
        return "answered"


class AgentsHandler:
    """Starts the agent, or finds the one with the id, and hands its stream over."""

    def __init__(self, agents_queue: str) -> None:
        self._agents_queue = agents_queue

    async def open_agent_stream(
        self, _ctx: nexusrpc.handler.StartOperationContext, agent_id: str
    ) -> StreamRef:
        try:
            # A caller that goes on in a new run starts the operation again, and
            # gets the same agent, running or done.
            client = temporalio.nexus.client()
            if agent_id.startswith("slow-"):
                await client.start_workflow(
                    SlowAgent.run,
                    "slow",
                    id=agent_id,
                    task_queue=self._agents_queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                )
            else:
                await client.start_workflow(
                    Agent.run,
                    ["streams", "links"],
                    id=agent_id,
                    task_queue=self._agents_queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                )
        except WorkflowAlreadyStartedError:
            pass
        return StreamRef.for_workflow(agent_id, topic=EVENTS)

    def service(self) -> Any:
        open_agent_stream = self.open_agent_stream

        @nexusrpc.handler.service_handler(service=Agents)
        class AgentsService:
            @nexusrpc.handler.operation_handler
            def run_agent(self) -> nexusrpc.handler.OperationHandler[str, str]:
                return StreamOperationHandler(open_agent_stream)

        return AgentsService()


def describe(item: FollowedItem[Any]) -> str:
    """One item as ``<source>:<what>``."""
    source = item.source.activity_id or "agent"
    if item.link_state is not None:
        return f"{source}:{item.link_state.kind.name}"
    record = item.record
    assert record is not None
    if record.kind is RecordKind.LINK:
        assert record.link is not None
        return f"{source}:{record.link.event.name}"
    if record.kind is RecordKind.DATA:
        return f"{source}:{record.value}"
    return f"{source}:{record.kind.name}"


@dataclass
class FollowOutput:
    items: list[str] = field(default_factory=list)
    runs: int = 1


@workflow.defn
class FollowsAgent:
    """Follows the agent, and goes on in a new run after ``stop_after`` batches."""

    @workflow.run
    async def run(
        self,
        agents: str,
        streams: str,
        agent_id: str,
        stop_after: int,
        items: list[str],
        runs: int = 1,
        after: ReadPosition | None = None,
    ) -> FollowOutput:
        client = workflow.create_nexus_client(service=Agents, endpoint=agents)
        handle = await client.start_operation(Agents.run_agent, agent_id)
        reader = StreamReader(
            handle, item_type=str, endpoint=streams, follow_links=True, after=after
        )
        batches = 0
        while (batch := await reader.next_followed()) is not None:
            items.extend(describe(item) for item in batch)
            batches += 1
            if batches == stop_after:
                workflow.continue_as_new(
                    args=[
                        agents,
                        streams,
                        agent_id,
                        0,
                        items,
                        runs + 1,
                        reader.position,
                    ]
                )
        await handle
        return FollowOutput(items, runs)


@dataclass
class Lane:
    caller: Client
    owner: Client
    agents: str
    streams: str
    queue: str


async def lane(client: Client, task_queue: str, new_store: NewStore) -> Lane:
    """Endpoints ``agents`` and ``streams`` that target the Workers in ``streams-b``."""
    owner = await two_namespaces(client, new_store().notify_on_append())
    queue = f"{task_queue}-b"
    agents = await endpoint(client, queue, namespace=NAMESPACE_B)
    streams = await endpoint(client, f"{queue}-streams", namespace=NAMESPACE_B)
    return Lane(client, owner, agents, streams, queue)


def owner_workers(lane: Lane) -> tuple[Worker, Worker, Worker]:
    handler = AgentsHandler(f"{lane.queue}-agents")
    return (
        Worker(
            lane.owner,
            task_queue=lane.queue,
            nexus_service_handlers=[handler.service()],
        ),
        Worker(
            lane.owner,
            task_queue=f"{lane.queue}-streams",
            nexus_service_handlers=[TemporalStreamsHandler()],
        ),
        Worker(
            lane.owner,
            task_queue=f"{lane.queue}-agents",
            workflows=[Agent, SlowAgent],
            activities=[search, slow_search],
        ),
    )


def direct_reads(lane: Lane) -> StreamDirectReads:
    return StreamDirectReads({lane.streams: ClientTransport(lane.caller, lane.streams)})


async def main(
    client: Client,
    task_queue: str,
    *,
    new_store: NewStore,
    stop_after: int = 0,
    workflow_id: str = "follows-agent",
    agent_id: str = "agent-1",
) -> FollowOutput:
    """Follow the agent from a Workflow in ``client``'s namespace, and replay each run.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier, Nexus operation progress and the namespace ``streams-b``.
    With ``stop_after``, the caller goes on in a new run after that many
    batches, from the reader's position.
    """
    the_lane = await lane(client, task_queue, new_store)
    caller = await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        plugins=[direct_reads(the_lane)],
    )
    agents_worker, streams_worker, agent_worker = owner_workers(the_lane)
    async with agents_worker, streams_worker, agent_worker:
        async with Worker(caller, task_queue=task_queue, workflows=[FollowsAgent]):
            handle = await caller.start_workflow(
                FollowsAgent.run,
                args=[
                    the_lane.agents,
                    the_lane.streams,
                    agent_id,
                    stop_after,
                    [],
                ],
                id=workflow_id,
                task_queue=task_queue,
            )
            output = await handle.result()
        # A new plugin has no batch cache, as in another process, so each
        # follow replays through the transport.
        run_ids = {handle.first_execution_run_id, handle.result_run_id}
        latest = await caller.get_workflow_handle(workflow_id).describe()
        run_ids.add(latest.run_id)
        for run_id in run_ids:
            run_history = await caller.get_workflow_handle(
                workflow_id, run_id=run_id
            ).fetch_history()
            await Replayer(
                workflows=[FollowsAgent], plugins=[direct_reads(the_lane)]
            ).replay_workflow(run_history)
    return output


if __name__ == "__main__":
    run(main)
