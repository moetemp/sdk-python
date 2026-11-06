"""Streams API proposal, the resume notes of "A UI backend reads a Stream by its id".

The chat server creates the Stream and passes its ref to the Workflow. The Reader gets the
Stream by its id, saves the read's ``next_offset`` after each Record, stops, and later resumes
from it with ``offset``. A view that wants only new Records starts at ``streams.END``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@dataclass
class Turn:
    text: str


@workflow.defn
class Chat:
    def __init__(self) -> None:
        self.more: list[str] = []

    @workflow.run
    async def run(self, ref: streams.StreamRef[Turn], first: list[str]) -> None:
        turns = workflow.to_stream(ref)
        await turns.publish(*map(Turn, first))
        await workflow.wait_condition(lambda: bool(self.more))
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

    def save_offset(self, key: str, offset: int | None) -> None:
        if offset is not None:
            self.offsets[key] = offset


async def read_some(
    client: Client, chat_id: str, store: OffsetStore, count: int
) -> list[str]:
    stream = client.get_stream[Turn](chat_id)
    seen = []
    read = stream.subscribe(offset=store.load_offset("turns"))
    async for r in read:
        if r.kind != streams.ItemKind.RECORD:
            continue
        seen.append(r.data.text)
        store.save_offset("turns", read.next_offset)  # the next read starts after it
        if len(seen) == count:
            break
    await read.aclose()
    return seen


async def read_new(client: Client, chat_id: str) -> list[str]:
    # A view that shows only what comes next starts after the newest Record.
    stream = client.get_stream[Turn](chat_id)
    return [
        r.data.text
        async for r in stream.subscribe(offset=streams.END, wait_new_records=False)
        if isinstance(r, streams.Record)
    ]


async def main(
    client: Client, task_queue: str
) -> tuple[list[str], list[str], list[str]]:
    async with Worker(client, task_queue=task_queue, workflows=[Chat]):
        chat_id = f"chat-{uuid.uuid4()}"
        async with await client.create_stream[Turn](chat_id) as turns:
            handle = await client.start_workflow(
                Chat.run,
                args=[turns.ref, ["a", "b"]],
                id=chat_id,
                task_queue=task_queue,
            )
            store = OffsetStore()
            before = await read_some(client, chat_id, store, 2)
            new = await read_new(client, chat_id)
            await handle.signal(Chat.add, ["c", "d"])
            after = await read_some(client, chat_id, store, 2)
            await handle.result()
        return before, new, after
