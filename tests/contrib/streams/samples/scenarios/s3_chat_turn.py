"""Scenarios 3a to 3c: a chat turn streams its tokens, and a retry takes back a partial answer.

The ``Chat`` Workflow publishes ``turn started`` on ``turns`` with
``workflow_writer``, runs the ``generate`` Activity, then publishes ``turn
ended``. The Activity appends the model's tokens on a second topic,
``tokens``. Its first attempt appends two tokens and fails, as a dropped
model connection does. Its second attempt appends the whole answer.

The chat UI reads both topics. It reads ``tokens`` through
``track_supersession``. So the ``SUPERSEDED`` record before attempt 2 tells
the UI which tokens of attempt 1 to take back, and the UI shows only the
answer of attempt 2.

Left out: a model, which a list of words stands in for. Server fencing and
retraction, which need the native stream.

Run with ``python -m tests.contrib.streams.samples.scenarios.s3_chat_turn``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    RecordKind,
    StreamRecord,
    activity_handle,
    get_stream_handle,
    topic,
    track_supersession,
    workflow_writer,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import (
    FOLLOW_TIMEOUT,
    describe,
    follow,
    run,
    values,
)

TURNS = topic("turns", str)
TOKENS = topic("tokens", str)

ANSWER = ["Your", "parcel", "is", "on", "its", "way."]


@dataclass
class Outcome:
    turns: list[str]
    shown: list[str]
    withdrawn: list[str]


@activity.defn
async def generate(prompt: str) -> str:
    print(f"[model] attempt {activity.info().attempt} answers {prompt!r}")
    producer = activity_handle().producer(topic=TOKENS)
    if activity.info().attempt == 1:
        await producer.append(*ANSWER[:2])
        raise RuntimeError("the model connection dropped")
    await producer.append(*ANSWER)
    await producer.finish()
    return " ".join(ANSWER)


@workflow.defn
class Chat:
    @workflow.run
    async def run(self, prompt: str) -> str:
        turns = workflow_writer(TURNS)
        turns.publish("turn started")
        answer = await workflow.execute_activity(
            generate,
            prompt,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
        )
        turns.publish("turn ended")
        await turns.finish()
        return answer


async def show_tokens(
    records: AsyncGenerator[StreamRecord[str], None],
) -> tuple[list[str], list[str]]:
    """Show each token as it comes, and take back the ones a retry withdraws.

    Returns the tokens shown at the end, and the tokens taken back.
    """
    shown: list[StreamRecord[str]] = []
    taken_back: list[str] = []
    async for record, withdrawn in track_supersession(records):
        print(f"[ui] {describe(record)}")
        for old in withdrawn:
            print(f"[ui] took back {old.value!r}")
            taken_back.append(str(old.value))
        gone = {id(old) for old in withdrawn}
        shown = [r for r in shown if id(r) not in gone]
        if record.kind is RecordKind.DATA and not record.stale:
            shown.append(record)
    print("[ui] the read ended")
    return values(shown), taken_back


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "chat-3"
) -> Outcome:
    """Run one chat turn and return what the UI showed and took back.

    ``client`` must carry the stream store.
    """
    async with Worker(
        client, task_queue=task_queue, workflows=[Chat], activities=[generate]
    ):
        handle = await client.start_workflow(
            Chat.run, "where is my parcel?", id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, workflow_id)
        turns: list[StreamRecord[str]] = []
        turn_reader = asyncio.create_task(
            follow("turns", stream.read(topic=TURNS), turns)
        )
        shown, taken_back = await asyncio.wait_for(
            show_tokens(stream.read(topic=TOKENS)), FOLLOW_TIMEOUT
        )
        await turn_reader
        print(f"[ui] the Workflow answered {await handle.result()!r}")
        return Outcome(turns=values(turns), shown=shown, withdrawn=taken_back)


if __name__ == "__main__":
    run(main)
