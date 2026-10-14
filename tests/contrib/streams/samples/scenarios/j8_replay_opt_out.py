"""Scenario 8 for a Workflow reader: replay its reads from the store, or from History.

``Ledger`` publishes two events, waits, publishes one more and closes its
stream. Two ``Audit`` Workflows read it with ``workflow_reader``:

- ``store``, with the default ``replay_from_store=True``. History keeps
  each read's range and a digest, and replay reads the range from the store
  again.
- ``history``, with ``replay_from_store=False``. History keeps each batch.

Both read the first two events on one Worker. That Worker stops, and a new
Worker with no Workflow cache takes over, so each of its Workflow Tasks
replays the reads before it. Both readers get every event. Last, a
``Replayer`` replays each History with a store plugin that no client
connected. ``history`` replays. ``store`` can't, since its replay needs the
store.

Left out: a run that outlives the store's hold on its records, which
should Continue-as-New first.

It needs a server with the stream notifier and the Workflow wake target,
and the store's ``notify_on_append()``.

Run with ``python -m tests.contrib.streams.samples.scenarios.j8_replay_opt_out``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from temporalio import workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.streams import (
    RecordKind,
    close_workflow_stream,
    topic,
    workflow_reader,
    workflow_writer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.worker import Replayer, Worker
from tests.contrib.streams.samples.scenarios._common import run

EVENTS = topic("events", str)
MODES = {"store": True, "history": False}


@dataclass
class Outcome:
    read: dict[str, list[str]]
    replays_with_no_store: dict[str, bool]


@workflow.defn
class Ledger:
    def __init__(self) -> None:
        self._more = False

    @workflow.run
    async def run(self) -> None:
        events = workflow_writer(EVENTS)
        events.publish("opened")
        events.publish("credited 10")
        await events.flush()
        await workflow.wait_condition(lambda: self._more)
        events.publish("debited 4")
        await close_workflow_stream(EVENTS, result="closed")

    @workflow.signal
    def more(self) -> None:
        self._more = True


@workflow.defn
class Audit:
    def __init__(self) -> None:
        self._seen: list[str] = []

    @workflow.run
    async def run(self, owner: str, replay_from_store: bool) -> list[str]:
        async with workflow_reader(
            owner, topic=EVENTS, replay_from_store=replay_from_store
        ) as reader:
            async for batch in reader:
                for record in batch.records:
                    if record.kind is RecordKind.DATA:
                        self._seen.append(str(record.value))
        return self._seen

    @workflow.query
    def seen(self) -> list[str]:
        return self._seen


async def _until_read(handle: WorkflowHandle[Audit, list[str]], count: int) -> None:
    for _ in range(200):
        if len(await handle.query(Audit.seen)) >= count:
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"{handle.id} did not read {count} events")


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "ledger-j8"
) -> Outcome:
    """Read across a Worker restart, then replay each reader with no store."""
    workflows = [Ledger, Audit]
    async with Worker(client, task_queue=task_queue, workflows=workflows):
        owner = await client.start_workflow(
            Ledger.run, id=workflow_id, task_queue=task_queue
        )
        audits = {
            mode: await client.start_workflow(
                Audit.run,
                args=[workflow_id, replay_from_store],
                id=f"{workflow_id}-{mode}",
                task_queue=task_queue,
            )
            for mode, replay_from_store in MODES.items()
        }
        for handle in audits.values():
            await _until_read(handle, 2)
    print("[worker] stopped, and a new Worker with no cache takes over")
    async with Worker(
        client, task_queue=task_queue, workflows=workflows, max_cached_workflows=0
    ):
        await owner.signal(Ledger.more)
        read = {mode: await handle.result() for mode, handle in audits.items()}
        await owner.result()
    for mode, events in read.items():
        print(f"[{mode}] read {events}")

    replays: dict[str, bool] = {}
    for mode, handle in audits.items():
        # A plugin that no client connected, as in a process with no store.
        replayer = Replayer(workflows=[Audit], plugins=[MemoryStreams()])
        try:
            await replayer.replay_workflow(await handle.fetch_history())
            replays[mode] = True
        except Exception as error:
            print(f"[{mode}] the replay needs the store: {type(error).__name__}")
            replays[mode] = False
        else:
            print(f"[{mode}] replayed with no store")
    return Outcome(read=read, replays_with_no_store=replays)


if __name__ == "__main__":
    run(main, notify=True)
