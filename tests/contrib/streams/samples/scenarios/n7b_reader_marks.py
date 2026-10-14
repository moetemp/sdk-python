"""Scenario 7b on Bridge: a caller Workflow sees the retry and the stale write.

s7b's courier Activity runs behind a stream operation. Attempt 1 appends and
stalls past its timeout, attempt 2 appends, and attempt 1 then appends late.
A ``Watcher`` Workflow reads the stream with ``StreamReader``. The reader
hands over both attempts' current records, reports the takeover in
``reader.supersessions``, and keeps the late record out of the batches and
in ``reader.stale_records``.

Left out: fencing and a retraction at the timeout, as in s7b.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7961
python -m tests.contrib.streams.samples.scenarios.n7b_reader_marks``.
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
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy
from temporalio.contrib.streams import StreamRef, close_workflow_stream
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios import s7b_retried_courier
from tests.contrib.streams.samples.scenarios._nexus import (
    NewStore,
    connect,
    endpoint,
    run,
)
from tests.contrib.streams.samples.scenarios.s7b_retried_courier import (
    POSITIONS,
    track,
)


@nexusrpc.service
class CourierService:
    watch: nexusrpc.Operation[str, str]


@workflow.defn
class Courier:
    """Runs the courier Activity, then closes its stream with the Activity's result."""

    @workflow.run
    async def run(self) -> None:
        result = await workflow.execute_activity(
            track,
            start_to_close_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
        )
        await close_workflow_stream(POSITIONS, result=result)


async def open_courier(_ctx: Any, courier_id: str) -> StreamRef:
    await temporalio.nexus.client().start_workflow(
        Courier.run,
        id=courier_id,
        task_queue=temporalio.nexus.info().task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return StreamRef.for_workflow(courier_id, topic=POSITIONS.name)


@nexusrpc.handler.service_handler(service=CourierService)
class CourierHandler:
    @nexusrpc.handler.operation_handler
    def watch(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(open_courier)


@dataclass
class Watched:
    positions: list[str] = field(default_factory=list)
    #: Each takeover, as "producer attempt -> attempt".
    supersessions: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    close_result: str = ""


@workflow.defn
class Watcher:
    @workflow.run
    async def run(self, endpoint: str, courier_id: str) -> Watched:
        client = workflow.create_nexus_client(service=CourierService, endpoint=endpoint)
        handle = await client.start_operation(CourierService.watch, courier_id)
        reader = StreamReader(handle, item_type=str, endpoint=endpoint)
        watched = Watched()
        while (batch := await reader.next()) is not None:
            watched.positions.extend(batch)
        watched.close_result = await handle
        watched.supersessions = [
            f"{mark.supersession.previous_attempt} -> {mark.supersession.attempt}"
            for mark in reader.supersessions
        ]
        watched.stale = [str(record.value) for record in reader.stale_records]
        return watched


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "watcher-n7b",
    new_store: NewStore,
) -> Watched:
    """Run the courier behind the operation and return what the watcher's reader marked.

    ``client`` is a client of a server with the stream notifier.
    """
    s7b_retried_courier._gate = s7b_retried_courier._Gate()
    streams = await connect(client, new_store().notify_on_append())
    name = await endpoint(client, task_queue)
    async with Worker(
        streams,
        task_queue=task_queue,
        workflows=[Courier, Watcher],
        activities=[track],
        nexus_service_handlers=[CourierHandler(), TemporalStreamsHandler()],
    ):
        handle = await streams.start_workflow(
            Watcher.run,
            args=[name, f"{workflow_id}-courier"],
            id=workflow_id,
            task_queue=task_queue,
        )
        watched = await asyncio.wait_for(handle.result(), 30)
        print(f"[watcher] {watched}")
        return watched


if __name__ == "__main__":
    run(main)
