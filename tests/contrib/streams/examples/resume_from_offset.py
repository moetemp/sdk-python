"""Brook API proposal, "A client resumes a Stream from an offset".

The Workflow hands out its ``turns`` Stream through a ``stream`` query. The Reader saves the
read's ``next_offset`` after each Record, stops, and later resumes from it with ``offset``. A
view that wants only new Records starts at ``common.END``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from temporalio import common, workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker


@dataclass
class Turn:
    text: str


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@workflow.defn
class Chat:
    def __init__(self) -> None:
        self.more: list[str] = []
        self.done = False

    @workflow.query
    def stream(self) -> common.WorkflowStream[Turn]:
        return workflow.stream_handle(Turns)

    @workflow.run
    async def run(self, first: list[str]) -> None:
        turns = await workflow.create_stream(Turns)
        await turns.publish(*map(Turn, first))
        await workflow.wait_condition(lambda: bool(self.more) or self.done)
        await turns.publish(*map(Turn, self.more))

    @workflow.signal
    def add(self, texts: list[str]) -> None:
        self.more = texts


class OffsetStore:
    """Where the Reader keeps its place, such as a database row."""

    def __init__(self) -> None:
        self.offsets: dict[str, int] = {}

    def load_offset(self, key: str) -> int | None:
        return self.offsets.get(key)

    def save_offset(self, key: str, offset: int) -> None:
        self.offsets[key] = offset


async def read_some(
    handle: WorkflowHandle[Chat, None], store: OffsetStore, count: int
) -> list[str]:
    stream = await handle.query("stream", result_type=common.ClientStream[Turn])
    seen = []
    read = stream.subscribe(
        offset=store.load_offset("turns"), create_wait=timedelta(seconds=10)
    )
    async for r in read:
        if isinstance(r, common.Closed):
            break
        if isinstance(r, common.Record):
            seen.append(r.data.text)
            # The read's next offset is after this Record, so the next read starts there.
            store.save_offset("turns", read.next_offset or 0)
            if len(seen) == count:
                break
    await read.aclose()
    return seen


async def read_new(handle: WorkflowHandle[Chat, None]) -> list[str]:
    # A view that shows only what comes next starts after the newest Record.
    stream = await handle.query("stream", result_type=common.ClientStream[Turn])
    return [
        r.data.text
        async for r in stream.subscribe(offset=common.END, wait_new_records=False)
        if isinstance(r, common.Record)
    ]


async def main(
    client: Client, task_queue: str
) -> tuple[list[str], list[str], list[str]]:
    async with Worker(client, task_queue=task_queue, workflows=[Chat]):
        handle = await client.start_workflow(
            Chat.run, ["a", "b"], id=f"chat-{uuid.uuid4()}", task_queue=task_queue
        )
        store = OffsetStore()
        before = await read_some(handle, store, 2)
        new = await read_new(handle)
        await handle.signal(Chat.add, ["c", "d"])
        after = await read_some(handle, store, 2)
        await handle.result()
        return before, new, after
