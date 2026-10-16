"""A CI tail follows a build's sparse log and wakes only when a line lands.

A build Workflow runs its steps as Activities. Each step appends one log
line when it ends, and the steps take longer than the reader's idle wait,
so the stream is idle most of the time. The tail is a client reader. It
sleeps on the stream's notifier between lines and reads at the poll limit
too, so it would also find a line whose notification was lost. The build
closes its stream with its status.

It needs a server with the stream notifier, and the store's
``notify_on_append()``.

Run with ``python -m tests.contrib.streams.samples.scenarios.j3_ci_tail``.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRecord,
    activity_handle,
    close_workflow_stream,
    get_stream_handle,
    topic,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

LOG = topic("log", str)


@activity.defn
async def step(name: str) -> None:
    # A real step compiles or tests. This one takes longer than the idle wait.
    await asyncio.sleep(3)
    await activity_handle().producer(topic=LOG).append(f"{name}: ok")


@workflow.defn
class Build:
    @workflow.run
    async def run(self) -> str:
        for name in ["checkout", "compile", "test"]:
            await workflow.execute_activity(
                step, name, start_to_close_timeout=timedelta(seconds=30)
            )
        await close_workflow_stream(LOG, result="passed")
        return "passed"


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "build-j3"
) -> list[str]:
    """Run a build and tail its log from a client, and return the lines."""
    seen: list[StreamRecord[str]] = []
    async with Worker(
        client, task_queue=task_queue, workflows=[Build], activities=[step]
    ):
        build = await client.start_workflow(
            Build.run, id=workflow_id, task_queue=task_queue
        )
        await follow(
            "tail", get_stream_handle(client, workflow_id, topic=LOG).read(), seen
        )
        print(f"[tail] the build {await build.result()}")
    return values(seen)


if __name__ == "__main__":
    run(main, notify=True)
