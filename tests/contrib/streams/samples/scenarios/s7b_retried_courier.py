"""Scenario 7b: a courier Activity stalls, is retried, and the stale attempt writes late.

Attempt 1 appends a record, then stalls past its 2 s start-to-close
timeout. Attempt 2 appends its own record. The backend sees ``SUPERSEDED``
before it. An in-process event then releases attempt 1, which appends one
more record. Core doesn't refuse it, and the backend sees it marked
``stale``.

Left out: fencing and a retraction at the timeout. Both need the native
stream, which is out of scope for this release.

Run with ``python -m tests.contrib.streams.samples.scenarios.s7b_retried_courier``.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.contrib.streams import (
    StreamRecord,
    activity_handle,
    get_stream_handle,
    topic,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, label, run

POSITIONS = topic("positions", str)


class _Gate:
    """Lets the stalled attempt 1 go on once attempt 2 wrote, in this process."""

    def __init__(self) -> None:
        self.released = asyncio.Event()
        self.late_written = asyncio.Event()


# Made by each run of ``main``, since an event belongs to one event loop.
_gate = _Gate()


@activity.defn
async def track() -> str:
    gate = _gate
    positions = activity_handle().producer(topic=POSITIONS)
    if activity.info().attempt == 1:
        await positions.append("attempt 1 at the depot")
        while not gate.released.is_set():
            try:
                await gate.released.wait()
            except asyncio.CancelledError:
                # A timed-out attempt can keep running, as a stuck process does.
                continue
        await positions.append("attempt 1 late")
        gate.late_written.set()
        return "attempt 1"
    await positions.append("attempt 2 at the depot")
    gate.released.set()
    await asyncio.wait_for(gate.late_written.wait(), 10)
    return "attempt 2"


@workflow.defn
class Courier:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            track,
            start_to_close_timeout=timedelta(seconds=2),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
        )


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "courier-7b"
) -> list[str]:
    """Run the courier and return a label for each record the backend saw.

    ``client`` must carry the stream store.
    """
    global _gate
    _gate = _Gate()
    async with Worker(
        client, task_queue=task_queue, workflows=[Courier], activities=[track]
    ):
        handle = await client.start_workflow(
            Courier.run, id=workflow_id, task_queue=task_queue
        )
        seen: list[StreamRecord[str]] = []
        await follow(
            "backend",
            get_stream_handle(client, workflow_id).read(topic=POSITIONS),
            seen,
        )
        print(f"[backend] the Activity's result came from {await handle.result()}")
        return [label(record) for record in seen]


if __name__ == "__main__":
    run(main)
