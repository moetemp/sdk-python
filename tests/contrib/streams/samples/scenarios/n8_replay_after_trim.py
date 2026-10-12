"""Scenario 8 on Bridge: a caller replays after the owner's streams are gone.

A ``Bookkeeper`` Workflow follows a ``Ledger``'s stream through a stream
operation. After two entries it continues as new and hands
``reader.position`` to its next run. The ledger then continues as new
itself, and its new run writes one more entry and closes the stream. The
bookkeeper's new run starts the operation again, reads after the position,
and gets only the new entry.

Then the ledger's streams are deleted and its Worker stops. A ``Replayer``
replays both of the bookkeeper's runs. Every read answer is in the
bookkeeper's History, so the replay needs neither the store nor the
ledger's Worker.

Left out: a reset that retracts records. A reset run's records carry the new
run id, and nothing retracts the old run's records.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.n8_replay_after_trim``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import nexusrpc
import nexusrpc.handler

from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams import StreamRef, topic, workflow_writer
from temporalio.contrib.streams.nexus import (
    ReadPosition,
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
    close_workflow_stream,
)
from temporalio.worker import Replayer, Worker
from tests.contrib.streams.samples.scenarios._common import store_of
from tests.contrib.streams.samples.scenarios._nexus import (
    NewStore,
    connect,
    endpoint,
    run,
    until,
)

ENTRIES = topic("entries", str)


@nexusrpc.service
class LedgerService:
    follow: nexusrpc.Operation[str, str]


@workflow.defn
class Ledger:
    def __init__(self) -> None:
        self._more = False

    @workflow.run
    async def run(self, run_number: int) -> None:
        entries = workflow_writer(ENTRIES)
        if run_number == 1:
            entries.publish("one")
            entries.publish("two")
            await workflow.wait_condition(lambda: self._more)
            workflow.continue_as_new(2)
        entries.publish("three")
        close_workflow_stream("3 entries", topic=ENTRIES)

    @workflow.signal
    def more(self) -> None:
        self._more = True


async def attach_only(_ctx: Any, ledger_id: str) -> StreamRef:
    return StreamRef.for_workflow(ledger_id, topic=ENTRIES.name)


@nexusrpc.handler.service_handler(service=LedgerService)
class LedgerHandler:
    @nexusrpc.handler.operation_handler
    def follow(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(attach_only)


@dataclass
class Books:
    entries: list[str] = field(default_factory=list)
    close_result: str = ""


@workflow.defn
class Bookkeeper:
    """Reads the ledger and continues as new once its first run has two entries."""

    @workflow.run
    async def run(
        self, endpoint: str, ledger_id: str, position: ReadPosition | None, books: Books
    ) -> Books:
        client = workflow.create_nexus_client(service=LedgerService, endpoint=endpoint)
        handle = await client.start_operation(LedgerService.follow, ledger_id)
        reader = StreamReader(handle, item_type=str, endpoint=endpoint, after=position)
        while (batch := await reader.next()) is not None:
            books.entries.extend(batch)
            if position is None and len(books.entries) >= 2:
                workflow.continue_as_new(
                    args=[endpoint, ledger_id, reader.position, books]
                )
        books.close_result = await handle
        return books


@dataclass
class Outcome:
    books: Books
    #: What the first run had read when it continued as new.
    first_run_entries: list[str]
    keys_deleted: int
    runs_replayed: int


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "bookkeeper-n8",
    new_store: NewStore,
) -> Outcome:
    """Run, delete the ledger's streams, stop its Worker, and replay the bookkeeper.

    ``client`` is a client of a server with the stream notifier. The replay
    raises if it fails.
    """
    streams = await connect(client, new_store().notify_on_append())
    plain = await connect(client, None)
    ledger_queue = f"{task_queue}-ledger"
    name = await endpoint(client, ledger_queue)
    ledger_id = f"{workflow_id}-ledger"
    async with Worker(plain, task_queue=task_queue, workflows=[Bookkeeper]):
        async with Worker(
            streams,
            task_queue=ledger_queue,
            workflows=[Ledger],
            nexus_service_handlers=[LedgerHandler(), TemporalStreamsHandler()],
        ):
            ledger = await streams.start_workflow(
                Ledger.run, 1, id=ledger_id, task_queue=ledger_queue
            )
            bookkeeper = await plain.start_workflow(
                Bookkeeper.run,
                args=[name, ledger_id, None, Books()],
                id=workflow_id,
                task_queue=task_queue,
            )
            first_run = plain.get_workflow_handle(
                workflow_id, run_id=bookkeeper.result_run_id
            )

            async def continued() -> bool:
                status = (await first_run.describe()).status
                return status == WorkflowExecutionStatus.CONTINUED_AS_NEW

            await until(continued)
            await ledger.signal(Ledger.more)
            books = await asyncio.wait_for(bookkeeper.result(), 30)
        deleted = await store_of(streams).delete_workflow_streams(
            client.namespace, ledger_id
        )
        print(f"[admin] deleted {deleted} store keys, and the ledger's Worker stopped")
        histories = [
            await first_run.fetch_history(),
            await plain.get_workflow_handle(workflow_id).fetch_history(),
        ]
        replayer = Replayer(workflows=[Bookkeeper])
        for history in histories:
            await replayer.replay_workflow(history)
        print(
            f"[replay] {len(histories)} runs replayed with no store and no ledger Worker"
        )
        second_input = (
            histories[1].events[0].workflow_execution_started_event_attributes
        )
        carried = await plain.data_converter.decode(
            second_input.input.payloads, [str, str, ReadPosition, Books]
        )
        return Outcome(
            books=books,
            first_run_entries=carried[3].entries,
            keys_deleted=deleted,
            runs_replayed=len(histories),
        )


if __name__ == "__main__":
    run(main)
