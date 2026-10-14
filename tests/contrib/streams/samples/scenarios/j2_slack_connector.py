"""A Slack connector Workflow follows a channel's stream and posts each message.

A channel Workflow owns the stream, and people append messages from client
code. The connector Workflow in the same namespace reads the stream with
``workflow_reader`` and posts each message through an Activity. Between
messages it sleeps on the stream's notifier, with no Local Activity and no
timer, and the first append wakes it on a Workflow Task.

Left out: Slack itself, which a list stands in for, and a channel in
another namespace, which needs Nexus.

It needs a server with the stream notifier and the Workflow wake target,
and the store's ``notify_on_append()``.

Run with ``python -m tests.contrib.streams.samples.scenarios.j2_slack_connector``.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    close_workflow_stream,
    get_stream_handle,
    topic,
    workflow_reader,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import run

MESSAGES = topic("messages", str)

# What the connector posted, in this process, as Slack would show it.
posted: list[str] = []


@activity.defn
async def post_to_slack(message: str) -> None:
    posted.append(message)
    print(f"[slack] {message}")


@workflow.defn
class Channel:
    """Owns the channel's stream until someone archives it."""

    def __init__(self) -> None:
        self.archived = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.archived)
        await close_workflow_stream(MESSAGES, result="archived")

    @workflow.signal
    def archive(self) -> None:
        self.archived = True


@workflow.defn
class Connector:
    """Posts every message of a channel, in order, until the channel closes."""

    @workflow.run
    async def run(self, channel: str) -> int:
        count = 0
        async with workflow_reader(channel, topic=MESSAGES) as reader:
            async for batch in reader:
                for record in batch.records:
                    if record.kind is RecordKind.DATA and record.value is not None:
                        await workflow.execute_activity(
                            post_to_slack,
                            record.value,
                            start_to_close_timeout=timedelta(seconds=10),
                        )
                        count += 1
        return count


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "channel-j2"
) -> list[str]:
    """Run a channel and its connector, send messages with gaps, and return what was posted."""
    posted.clear()
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Channel, Connector],
        activities=[post_to_slack],
    ):
        channel = await client.start_workflow(
            Channel.run, id=workflow_id, task_queue=task_queue
        )
        connector = await client.start_workflow(
            Connector.run,
            workflow_id,
            id=f"{workflow_id}-connector",
            task_queue=task_queue,
        )
        handle = get_stream_handle(client, workflow_id, topic=MESSAGES)
        # One producer per person, which numbers that person's messages.
        people = {
            person: handle.producer(producer_id=person, attempt=1)
            for person in ("ana", "bo")
        }
        for person, text in [("ana", "hello"), ("bo", "hi ana"), ("ana", "lunch?")]:
            # A gap longer than the idle wait, so the connector sleeps between messages.
            await asyncio.sleep(3)
            await people[person].append(f"{person}: {text}")
        await channel.signal(Channel.archive)
        count = await connector.result()
        print(f"[connector] posted {count} messages")
    return list(posted)


if __name__ == "__main__":
    run(main, notify=True)
