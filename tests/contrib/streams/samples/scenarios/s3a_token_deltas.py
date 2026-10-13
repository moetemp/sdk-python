"""Scenario 3a: a UI follows a chat turn and its token deltas.

The Workflow publishes ``turn started`` and ``turn finished`` on ``turns``.
The model Activity appends the deltas on ``deltas`` as its own producer.
The backend reads both topics at once. The ``turns`` records become visible
when the activation that publishes them completes, so the Activity, which
that activation scheduled, already sees ``turn started``.

Left out: the native stream. Redis or memory stands in for it, so appends
never contend with the Workflow's own writes.

Run with ``python -m tests.contrib.streams.samples.scenarios.s3a_token_deltas``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    RecordKind,
    StreamRecord,
    activity_handle,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

TURNS = topic("turns", str)
DELTAS = topic("deltas", str)


@dataclass
class Turn:
    text: str
    saw_turn_started: bool


@activity.defn
async def model(prompt: str) -> Turn:
    # The activation that scheduled this Activity published the turn, and Core
    # appended it when that activation completed, so the record is there.
    newest = await activity_handle().latest(topic=TURNS)
    deltas = activity_handle().producer(topic=DELTAS)
    words = ["Here", " is", " an", " answer", " to ", prompt]
    for word in words:
        await deltas.append(word)
    return Turn(text="".join(words), saw_turn_started=newest != BEGINNING)


@workflow.defn
class ChatWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> Turn:
        turns = workflow_writer(TURNS)
        turns.publish("turn started")
        turn = await workflow.execute_activity(
            model, prompt, start_to_close_timeout=timedelta(seconds=30)
        )
        turns.publish("turn finished")
        return turn


@dataclass
class Seen:
    turns: list[str]
    text: str
    answer: Turn


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "chat-3a"
) -> Seen:
    """Run one turn and return what the backend saw on each topic.

    ``client`` must carry the stream store.
    """
    async with Worker(
        client, task_queue=task_queue, workflows=[ChatWorkflow], activities=[model]
    ):
        handle = await client.start_workflow(
            ChatWorkflow.run, "streams", id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, workflow_id)
        turns: list[StreamRecord[str]] = []
        deltas: list[StreamRecord[str]] = []
        await asyncio.gather(
            follow("turns", stream.read(topic=TURNS), turns),
            follow("deltas", stream.read(topic=DELTAS), deltas),
        )
        answer = await handle.result()
        return Seen(
            turns=[r.value for r in turns if r.kind is RecordKind.DATA and r.value],
            text="".join(values(deltas)),
            answer=answer,
        )


if __name__ == "__main__":
    run(main)
