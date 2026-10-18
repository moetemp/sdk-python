"""A Nexus tool call streams its output from a standalone Activity.

The tool runs as a standalone Activity in namespace ``streams-b``, and the
Activity owns the stream it writes. The operation ``Tools.call_tool`` starts
the Activity and hands the caller the Activity's stream, so the caller reads
the tool's output while it runs, and the operation completes with the
stream's close. A retry of the tool is reported to the reader as a
supersession.

Workers in ``streams-b``: one serves ``Tools`` and the stream service on
``<task queue>-b``, and one runs the ``run_tool`` Activity on
``<task queue>-tools``. The caller's Workflow runs in the client's namespace
and reads directly through ``StreamDirectReads``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Literal

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import ActivityIDConflictPolicy, RetryPolicy
from temporalio.contrib.streams import StreamRef, activity_handle, topic
from temporalio.contrib.streams.nexus import (
    ClientTransport,
    HttpTransport,
    StreamDirectReads,
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, Worker
from tests.contrib.streams._nexus_lane import (
    NAMESPACE_B,
    NewStore,
    endpoint,
    stream_service_url,
    two_namespaces,
)

TOKENS = topic("tokens", str)


@nexusrpc.service
class Tools:
    call_tool: nexusrpc.Operation[str, str]


@activity.defn
async def run_tool(call: str) -> str:
    """Streams the tool's output. The first attempt fails halfway."""
    stream = activity_handle()
    out = stream.producer(topic=TOKENS)
    if call == "boom":
        await out.append("x1")
        # No close: the stream's notifier closes it with this failure.
        raise ApplicationError("boom", type="Boom", non_retryable=True)
    if activity.info().attempt == 1:
        await out.append("a1", "a2", "a3")
        raise ApplicationError("the tool dropped")
    await out.append("b1", "b2")
    await stream.close(topic=TOKENS, result="done")
    return "ok"


class ToolsHandler:
    """Starts the tool's Activity and hands the caller its stream."""

    def __init__(self, tools_queue: str) -> None:
        self._tools_queue = tools_queue

    async def open_tool_stream(
        self, ctx: nexusrpc.handler.StartOperationContext, call: str
    ) -> StreamRef:
        # The request id stays the same across retried starts, so a retry
        # reaches the same Activity run and attaches once.
        handle = await temporalio.nexus.client().start_activity(
            run_tool,
            call,
            id=f"tool-{ctx.request_id}",
            task_queue=self._tools_queue,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), maximum_attempts=2
            ),
            id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
        )
        return StreamRef.for_activity(handle.id, run_id=handle.run_id, topic=TOKENS)

    def service(self) -> Any:
        open_tool_stream = self.open_tool_stream

        @nexusrpc.handler.service_handler(service=Tools)
        class ToolsService:
            @nexusrpc.handler.operation_handler
            def call_tool(self) -> nexusrpc.handler.OperationHandler[str, str]:
                return StreamOperationHandler(open_tool_stream)

        return ToolsService()


@dataclass
class ToolOutput:
    records: list[str] = field(default_factory=list)
    supersessions: list[str] = field(default_factory=list)
    result: str = ""


@workflow.defn
class CallsTool:
    """Calls the tool and reads its stream to the end."""

    @workflow.run
    async def run(
        self, tools: str, streams: str, call: str, recorded: bool
    ) -> ToolOutput:
        client = workflow.create_nexus_client(service=Tools, endpoint=tools)
        handle = await client.start_operation(Tools.call_tool, call)
        reader = StreamReader(
            handle, item_type=str, endpoint=streams, replay_from_store=not recorded
        )
        output = ToolOutput()
        while (batch := await reader.next()) is not None:
            output.records.extend(batch)
        output.supersessions = [
            # The low 11 bits of an Activity's attempt are its Temporal attempt.
            f"{mark.supersession.previous_attempt & 0x7FF}"
            f" -> {mark.supersession.attempt & 0x7FF}"
            for mark in reader.supersessions
        ]
        output.result = await handle
        return output


@dataclass
class Lane:
    """The two namespaces, their Workers' task queues and the endpoints."""

    caller: Client
    owner: Client
    tools: str
    streams: str
    streams_url: str
    queue: str


async def lane(client: Client, task_queue: str, new_store: NewStore) -> Lane:
    """Endpoints ``tools`` and ``streams`` that target the Worker in ``streams-b``."""
    owner = await two_namespaces(client, new_store().notify_on_append())
    queue = f"{task_queue}-b"
    tools = await endpoint(client, queue, namespace=NAMESPACE_B)
    streams = await endpoint(client, f"{queue}-streams", namespace=NAMESPACE_B)
    url = await stream_service_url(client, streams)
    return Lane(client, owner, tools, streams, url, queue)


def owner_workers(
    lane: Lane, *, authorize: Any = None
) -> tuple[Worker, Worker, Worker]:
    """The Workers in ``streams-b``: the handlers on both endpoints and the tool."""
    tools = ToolsHandler(f"{lane.queue}-tools")
    return (
        Worker(
            lane.owner, task_queue=lane.queue, nexus_service_handlers=[tools.service()]
        ),
        Worker(
            lane.owner,
            task_queue=f"{lane.queue}-streams",
            nexus_service_handlers=[TemporalStreamsHandler(authorize=authorize)],
        ),
        Worker(lane.owner, task_queue=f"{lane.queue}-tools", activities=[run_tool]),
    )


def direct_reads(lane: Lane, transport: Literal["client", "http"]) -> StreamDirectReads:
    if transport == "client":
        return StreamDirectReads(
            {lane.streams: ClientTransport(lane.caller, lane.streams)}
        )
    return StreamDirectReads({lane.streams: HttpTransport(lane.streams_url)})


async def main(
    client: Client,
    task_queue: str,
    *,
    new_store: NewStore,
    transport: Literal["client", "http", "recorded"] = "client",
    call: str = "hello",
    workflow_id: str = "calls-tool",
) -> ToolOutput:
    """Call the tool from a Workflow in ``client``'s namespace, and replay its History.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier, Nexus operation progress and the namespace ``streams-b``.
    ``transport`` picks how the reader reads: directly through a
    ``ClientTransport`` or an ``HttpTransport``, or recorded in History.
    """
    the_lane = await lane(client, task_queue, new_store)
    plugins: list[Any] = []
    if transport != "recorded":
        plugins.append(direct_reads(the_lane, transport))
    caller = await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        plugins=plugins,
    )
    tools_worker, streams_worker, tool_worker = owner_workers(the_lane)
    async with tools_worker, streams_worker, tool_worker:
        async with Worker(caller, task_queue=task_queue, workflows=[CallsTool]):
            handle = await caller.start_workflow(
                CallsTool.run,
                args=[the_lane.tools, the_lane.streams, call, transport == "recorded"],
                id=workflow_id,
                task_queue=task_queue,
            )
            output = await handle.result()
        history = await handle.fetch_history()
        # A new plugin has no batch cache, as in another process, so
        # positions-only reads replay through the transport. Recorded reads
        # replay with no server.
        replay_plugins: list[Any] = []
        if transport != "recorded":
            replay_plugins.append(direct_reads(the_lane, transport))
        await Replayer(workflows=[CallsTool], plugins=replay_plugins).replay_workflow(
            history
        )
    return output
