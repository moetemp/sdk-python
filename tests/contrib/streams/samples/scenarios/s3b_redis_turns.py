"""Scenario 3b: a chat whose whole stream is in Redis.

Two turns share one topic, so the backend sees one order. The Workflow
publishes each turn's start and end directly. The model Activity appends
the deltas as its own producer. Turn 1's Activity fails once and is
retried, so the backend sees ``SUPERSEDED`` before the retry's deltas. Turn
2 fails on every attempt. Nothing withdraws its partial deltas, so the
Workflow publishes ``turn 2 failed`` after them.

The page lists this as Redis only, so the test runs it on Redis only.

Run with ``STREAMS_REDIS_URL=redis://localhost:6379 python -m
tests.contrib.streams.samples.scenarios.s3b_redis_turns``.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    StreamRecord,
    activity_handle,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, label, run

CHAT = topic("chat", str)

#: The deltas each turn's model call writes, and how many of them an attempt
#: writes before it fails.
REPLIES = {1: (["Hel", "lo"], {1: 1}), 2: (["Wor", "ld"], {1: 1, 2: 1})}


@activity.defn
async def reply(turn: int) -> str:
    words, fails_after = REPLIES[turn]
    attempt = activity.info().attempt
    deltas = activity_handle().producer(topic=CHAT)
    if attempt in fails_after:
        await deltas.append(*words[: fails_after[attempt]])
        raise ApplicationError(f"the model call of turn {turn} dropped")
    await deltas.append(*words)
    return "".join(words)


@workflow.defn
class ChatWorkflow:
    @workflow.run
    async def run(self) -> None:
        chat = workflow_writer(CHAT)
        for turn in (1, 2):
            chat.publish(f"turn {turn} started")
            try:
                await workflow.execute_activity(
                    reply,
                    turn,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(milliseconds=100), maximum_attempts=2
                    ),
                )
            except ActivityError:
                chat.publish(f"turn {turn} failed")
                continue
            chat.publish(f"turn {turn} finished")


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "chat-3b"
) -> list[str]:
    """Run both turns and return a label for each record the backend saw.

    ``client`` must carry a Redis stream store.
    """
    async with Worker(
        client, task_queue=task_queue, workflows=[ChatWorkflow], activities=[reply]
    ):
        handle = await client.start_workflow(
            ChatWorkflow.run, id=workflow_id, task_queue=task_queue
        )
        seen: list[StreamRecord[str]] = []
        await follow(
            "backend", get_stream_handle(client, workflow_id).read(topic=CHAT), seen
        )
        await handle.result()
        return [label(record) for record in seen]


if __name__ == "__main__":
    run(main)
