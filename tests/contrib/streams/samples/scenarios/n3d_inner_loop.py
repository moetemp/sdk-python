"""Scenarios 3d and 3e on Bridge: an agent follows an inner loop in another namespace.

``TurnWorkflow`` runs in namespace ``streams-b``. It publishes its
messages on ``message`` and its token deltas on ``delta``. ``AgentWorkflow``
in ``default`` starts the ``run_turn`` stream operation, which follows the
``message`` topic only, since one operation follows one topic. The agent
gets every message and the turn's close result.

3d and 3e differ only in B's store, memory or Redis. The test runs both,
and the agent can't tell them apart.

Left out: a backend that polls ``delta`` across namespaces. That needs
Nexus direct calls, which don't exist yet.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7961
python -m tests.contrib.streams.samples.scenarios.n3d_inner_loop``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import workflow
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
DELTA = topic("delta", str)
MESSAGES = ["looking up the order", "the order shipped yesterday", "anything else?"]


@nexusrpc.service
class TurnService:
    run_turn: nexusrpc.Operation[str, str]


@workflow.defn
class TurnWorkflow:
    """One turn of the inner loop: deltas as they come, then each whole message."""

    @workflow.run
    async def run(self, prompt: str) -> None:
        messages = workflow_writer(MESSAGE)
        deltas = workflow_writer(DELTA)
        for text in MESSAGES:
            for word in text.split():
                deltas.publish(word)
            messages.publish(text)
            await workflow.sleep(timedelta(milliseconds=100))
        await close_workflow_stream(MESSAGE, result=f"answered {prompt!r}")


async def open_turn(ctx: Any, prompt: str) -> StreamRef:
    workflow_id = f"turn-{ctx.request_id}"
    await temporalio.nexus.client().start_workflow(
        TurnWorkflow.run,
        prompt,
        id=workflow_id,
        task_queue=temporalio.nexus.info().task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return StreamRef.for_workflow(workflow_id, topic=MESSAGE.name)


@nexusrpc.handler.service_handler(service=TurnService)
class TurnHandler:
    @nexusrpc.handler.operation_handler
    def run_turn(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(open_turn)


@dataclass
class Heard:
    messages: list[str] = field(default_factory=list)
    close_result: str = ""


@workflow.defn
class AgentWorkflow:
    """Follows one turn's messages in another namespace."""

    @workflow.run
    async def run(self, endpoint: str, prompt: str) -> Heard:
        client = workflow.create_nexus_client(service=TurnService, endpoint=endpoint)
        handle = await client.start_operation(TurnService.run_turn, prompt)
        reader = StreamReader(handle, item_type=str, endpoint=endpoint)
        heard = Heard()
        while (batch := await reader.next()) is not None:
            heard.messages.extend(batch)
        heard.close_result = await handle
        return heard


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "agent-n3d",
    new_store: NewStore,
) -> Heard:
    """Run one turn in B with a store from ``new_store`` and return what the agent heard.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier and the namespace ``streams-b``.
    """
    b = await two_namespaces(client, new_store().notify_on_append())
    a = await connect(client, None)
    b_queue = f"{task_queue}-b"
    name = await endpoint(client, b_queue, namespace=NAMESPACE_B)
    async with (
        Worker(
            b,
            task_queue=b_queue,
            workflows=[TurnWorkflow],
            nexus_service_handlers=[TurnHandler(), TemporalStreamsHandler()],
        ),
        Worker(a, task_queue=task_queue, workflows=[AgentWorkflow]),
    ):
        handle = await a.start_workflow(
            AgentWorkflow.run,
            args=[name, "where is my order"],
            id=workflow_id,
            task_queue=task_queue,
        )
        heard = await asyncio.wait_for(handle.result(), 30)
        for message in heard.messages:
            print(f"[agent] {message}")
        return heard


if __name__ == "__main__":
    run(main)
