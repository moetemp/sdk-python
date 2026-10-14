"""Scenario 9 on Bridge: a caller in another namespace reads directly.

As in n1c, ``Ledger`` and the stream operation's handler run in namespace
``streams-b``, and the caller ``Bookkeeper`` runs in ``default``. The
bookkeeper's reader opts in with ``replay_from_store=True``. So each read runs
in a Local Activity, which reaches B's stream service through
``HttpTransport`` on the endpoint's HTTP ingress, and the bookkeeper's History
keeps the range and digest of each read, not the records.

After the first entries the bookkeeper's Worker stops. The ledger goes on
posting while nobody reads. Then a new Worker, with an empty batch cache,
picks the run up. It replays the run and reads each recorded range again from
B's store, through the endpoint. The store keeps those records for the
reader's floor holder while the floor lease lasts, even past the store's
retention. Once the lease lapses with no read, a trim can drop them, and the
replay fails its Workflow Task with ``StreamReplayRangeLostError``.

Both namespaces can share an encryption codec. The handler decodes each body
with B's codec and B's Worker encodes the read's answer, so the transport
takes the same codec as ``payload_codec``.

Left out: credentials. The live lane's server takes no auth. A real
deployment gives ``HttpTransport`` a write credential on ``streams-b``.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7961
python -m tests.contrib.streams.samples.scenarios.n9_direct_reads``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio import workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowHistory
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.contrib.streams import (
    StreamRef,
    close_workflow_stream,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.nexus import (
    HttpTransport,
    StreamDirectReads,
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.contrib.streams.nexus._direct import DIRECT_READ_ACTIVITY
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.worker import Replayer, Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NAMESPACE_B,
    NewStore,
    endpoint,
    run,
    stream_service_url,
    two_namespaces,
    until,
)

ENTRIES = topic("entries", str)


@nexusrpc.service
class LedgerService:
    follow: nexusrpc.Operation[str, str]


@workflow.defn
class Ledger:
    """Publishes the entries it is sent, then closes its stream on request."""

    def __init__(self) -> None:
        self._pending: list[str] = []
        self._closing = False

    @workflow.run
    async def run(self) -> None:
        entries = workflow_writer(ENTRIES)
        posted = 0
        while True:
            await workflow.wait_condition(lambda: bool(self._pending) or self._closing)
            for entry in self._pending:
                entries.publish(entry)
                posted += 1
            self._pending.clear()
            if self._closing:
                break
        await close_workflow_stream(ENTRIES, result=f"{posted} entries")

    @workflow.signal
    def post(self, entries: list[str]) -> None:
        self._pending.extend(entries)

    @workflow.signal
    def close(self) -> None:
        self._closing = True


async def open_ledger(_ctx: Any, ledger_id: str) -> StreamRef:
    """Names ``Ledger``'s stream, and starts ``Ledger`` if nobody did."""
    await temporalio.nexus.client().start_workflow(
        Ledger.run,
        id=ledger_id,
        task_queue=temporalio.nexus.info().task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return StreamRef.for_workflow(ledger_id, topic=ENTRIES.name)


@nexusrpc.handler.service_handler(service=LedgerService)
class LedgerHandler:
    @nexusrpc.handler.operation_handler
    def follow(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(open_ledger)


@workflow.defn
class Bookkeeper:
    """Reads a ledger in another namespace, keeping the records out of History."""

    def __init__(self) -> None:
        self._entries: list[str] = []

    @workflow.run
    async def run(self, endpoint: str, ledger_id: str) -> list[str]:
        client = workflow.create_nexus_client(service=LedgerService, endpoint=endpoint)
        handle = await client.start_operation(LedgerService.follow, ledger_id)
        reader = StreamReader(
            handle, item_type=str, endpoint=endpoint, replay_from_store=True
        )
        while (batch := await reader.next()) is not None:
            self._entries.extend(batch)
        return self._entries

    @workflow.query
    def count(self) -> int:
        return len(self._entries)


@dataclass
class Outcome:
    entries: list[str] | None
    """What the bookkeeper read. ``None`` when its replay failed."""
    replay_failure: str | None
    """The failure of the bookkeeper's first failed Workflow Task, if one failed."""
    marker_bytes: list[int] = field(default_factory=list)
    """The size of each read's result in the bookkeeper's History."""
    recorded_reads: int = 0
    """How many reads the bookkeeper's History holds as Nexus operations."""


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "bookkeeper-n9",
    new_store: NewStore,
    codec: PayloadCodec | None = None,
    gap: timedelta = timedelta(0),
) -> Outcome:
    """Read a ledger directly across namespaces, with a Worker restart in the middle.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier and the namespace ``streams-b``. While the bookkeeper's
    Worker is down, the ledger posts an entry every half second for ``gap``.
    """
    converter = dataclasses.replace(DataConverter.default, payload_codec=codec)
    base = await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        data_converter=converter,
    )
    b = await two_namespaces(base, new_store().notify_on_append())
    b_queue = f"{task_queue}-b"
    name = await endpoint(base, b_queue, namespace=NAMESPACE_B)
    url = await stream_service_url(base, name)

    async def caller() -> Client:
        # A new plugin is a new batch cache, as in a new process.
        return await Client.connect(
            base.service_client.config.target_host,
            namespace=base.namespace,
            data_converter=converter,
            plugins=[reads()],
        )

    def reads() -> StreamDirectReads:
        return StreamDirectReads({name: HttpTransport(url, payload_codec=codec)})

    ledger_id = f"{workflow_id}-ledger"
    async with Worker(
        b,
        task_queue=b_queue,
        workflows=[Ledger],
        nexus_service_handlers=[LedgerHandler(), TemporalStreamsHandler()],
    ):
        ledger = await b.start_workflow(Ledger.run, id=ledger_id, task_queue=b_queue)
        a = await caller()
        async with Worker(a, task_queue=task_queue, workflows=[Bookkeeper]):
            bookkeeper = await a.start_workflow(
                Bookkeeper.run,
                args=[name, ledger_id],
                id=workflow_id,
                task_queue=task_queue,
            )
            await ledger.signal(Ledger.post, ["a", "b"])

            async def read_two() -> bool:
                return await bookkeeper.query(Bookkeeper.count) >= 2

            await until(read_two)
        # Nobody reads while the ledger goes on.
        posted = 0
        loop = asyncio.get_running_loop()
        end = loop.time() + gap.total_seconds()
        while True:
            posted += 1
            await ledger.signal(Ledger.post, [f"later {posted}"])
            if loop.time() >= end:
                break
            await asyncio.sleep(0.5)
        await ledger.signal(Ledger.close)
        a = await caller()
        async with Worker(a, task_queue=task_queue, workflows=[Bookkeeper]):
            failure = await _first_task_failure_or_result(a, workflow_id)
        if failure is not None:
            await a.get_workflow_handle(workflow_id).terminate("the replay failed")
            return Outcome(entries=None, replay_failure=failure)
        entries = await a.get_workflow_handle(workflow_id).result()
        history = await a.get_workflow_handle(workflow_id).fetch_history()
        # A Replayer in yet another process reads every range again.
        await Replayer(
            workflows=[Bookkeeper], data_converter=converter, plugins=[reads()]
        ).replay_workflow(history)
        return Outcome(
            entries=entries,
            replay_failure=None,
            marker_bytes=_marker_bytes(history),
            recorded_reads=sum(
                1
                for event in history.events
                if event.event_type == EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
                and event.nexus_operation_scheduled_event_attributes.operation == "read"
            ),
        )


async def _first_task_failure_or_result(client: Client, workflow_id: str) -> str | None:
    """Wait for the run to end or for a Workflow Task to fail, and return that failure."""
    handle = client.get_workflow_handle(workflow_id)
    result = asyncio.ensure_future(handle.result())
    try:
        for _ in range(300):
            if result.done():
                result.result()
                return None
            async for event in handle.fetch_history_events():
                if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
                    return str(event.workflow_task_failed_event_attributes.failure)
            await asyncio.sleep(0.1)
        raise TimeoutError("the bookkeeper neither ended nor failed a task")
    finally:
        result.cancel()


def _marker_bytes(history: WorkflowHistory) -> list[int]:
    sizes: list[int] = []
    for event in history.events:
        if event.event_type != EventType.EVENT_TYPE_MARKER_RECORDED:
            continue
        details = event.marker_recorded_event_attributes.details
        data = json.loads(details["data"].payloads[0].data)
        if data.get("activity_type") == DIRECT_READ_ACTIVITY:
            sizes.append(details["result"].payloads[0].ByteSize())
    return sizes


if __name__ == "__main__":
    run(main)
