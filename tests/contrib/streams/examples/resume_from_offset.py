"""Brook API proposal, "A client resumes a scoped Stream from an offset".

The Workflow writes one Stream per turn, a renamed definition from ``common.with_name``. The
Reader saves the read's ``next_offset`` after each Record, stops, and later resumes from it with
``offset``. A view that wants only new Records starts at ``common.END``.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from temporalio import common, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker


@common.defn(name="events")
class Events:
    tokens = common.topic(str)


Turn3 = common.with_name(Events, "turn-3")


@workflow.defn
class Turns:
    def __init__(self) -> None:
        self.more: list[str] = []
        self.done = False

    @workflow.run
    async def run(self, first: list[str]) -> None:
        turn = await workflow.create_stream(Turn3)
        tokens = turn.topic(Events.tokens)
        await tokens.publish(*first)
        await workflow.wait_condition(lambda: bool(self.more) or self.done)
        await tokens.publish(*self.more)

    @workflow.signal
    def add(self, words: list[str]) -> None:
        self.more = words


class OffsetStore:
    """Where the Reader keeps its place, such as a database row."""

    def __init__(self) -> None:
        self.offsets: dict[str, int] = {}

    def load(self, key: str) -> int | None:
        return self.offsets.get(key)

    def save(self, key: str, offset: int) -> None:
        self.offsets[key] = offset


async def read_some(
    handle: WorkflowHandle[Turns, None], store: OffsetStore, count: int
) -> list[str]:
    turn3 = handle.stream(Turn3)
    seen = []
    read = turn3.subscribe(
        Events.tokens, offset=store.load("turn-3"), create_wait=timedelta(seconds=10)
    )
    async for item in read:
        if isinstance(item, common.Closed):
            break
        if isinstance(item, common.Superseded):
            continue
        seen.append(item.data)
        # The read's next offset is after this Record, so the next read starts there.
        store.save("turn-3", read.next_offset or 0)
        if len(seen) == count:
            break
    await read.aclose()
    return seen


async def read_new(handle: WorkflowHandle[Turns, None]) -> list[str]:
    # A view that shows only what comes next starts after the newest Record.
    turn3 = handle.stream(Turn3)
    return [
        item.data
        async for item in turn3.subscribe(
            Events.tokens, offset=common.END, wait_new_records=False
        )
        if isinstance(item, common.Record)
    ]


async def main(
    client: Client, task_queue: str
) -> tuple[list[str], list[str], list[str]]:
    async with Worker(client, task_queue=task_queue, workflows=[Turns]):
        handle = await client.start_workflow(
            Turns.run, ["a", "b"], id=f"turns-{uuid.uuid4()}", task_queue=task_queue
        )
        store = OffsetStore()
        before = await read_some(handle, store, 2)
        new = await read_new(handle)
        await handle.signal(Turns.add, ["c", "d"])
        after = await read_some(handle, store, 2)
        await handle.result()
        return before, new, after
