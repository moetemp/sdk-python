"""Brook API proposal, "A client resumes a scoped Stream from an offset".

The Workflow writes one Stream per turn with a ``name=`` override. The Reader saves the offset
after each Record itself, stops, and later resumes from it with ``from_offset``. A view that
wants only new Records starts at ``streams.END``.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib import streams
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.worker import Worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


@workflow.defn
class Turns:
    def __init__(self) -> None:
        self.more: list[str] = []
        self.done = False

    @workflow.run
    async def run(self, first: list[str]) -> None:
        turn = await stream_workflow.create_stream(Events, name="turn-3")
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
    client: Client, handle: WorkflowHandle[Turns, None], store: OffsetStore, count: int
) -> list[str]:
    turn3 = streams.StreamClient(client).workflow_stream(handle, Events, name="turn-3")
    tokens = turn3.topic(Events.tokens)
    seen = []
    async for item in tokens.subscribe(
        from_offset=store.load("turn-3"), create_wait=timedelta(seconds=10)
    ):
        if isinstance(item, streams.Closed):
            break
        if isinstance(item, streams.Superseded):
            continue
        seen.append(item.data)
        # from_offset is inclusive, so the next read starts after this Record.
        store.save("turn-3", item.offset + 1)
        if len(seen) == count:
            break
    return seen


async def read_new(client: Client, handle: WorkflowHandle[Turns, None]) -> list[str]:
    # A view that shows only what comes next starts after the newest Record.
    turn3 = streams.StreamClient(client).workflow_stream(handle, Events, name="turn-3")
    return [
        item.data
        async for item in turn3.topic(Events.tokens).subscribe(
            from_offset=streams.END, wait_new_records=False
        )
        if isinstance(item, streams.Record)
    ]


async def main(
    client: Client, task_queue: str
) -> tuple[list[str], list[str], list[str]]:
    async with Worker(client, task_queue=task_queue, workflows=[Turns]):
        handle = await client.start_workflow(
            Turns.run, ["a", "b"], id=f"turns-{uuid.uuid4()}", task_queue=task_queue
        )
        store = OffsetStore()
        before = await read_some(client, handle, store, 2)
        new = await read_new(client, handle)
        await handle.signal(Turns.add, ["c", "d"])
        after = await read_some(client, handle, store, 2)
        await handle.result()
        return before, new, after
