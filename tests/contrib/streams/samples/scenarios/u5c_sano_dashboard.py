"""Scenario 5c with U5: dashboards follow a delivery as standalone operations.

``Delivery`` publishes 50 stages, waits, then publishes 5 more and closes its
stream. A first dashboard starts following after the 50. It reads the
backlog in pages of 10, and its follower is slow, so a call runs out of time
before the backlog ends. It then answers ``behind``, and the server sends the
same progress again at once, until the dashboard caught up. It then follows
the last stages live. A second dashboard starts following after the close.
It gets the backlog and the close at once, since the server answers a late
attach with the close.

Left out: the backlog as data in the callbacks. The follower reads it.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.u5c_sano_dashboard``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.nexus import (
    FollowBatch,
    FollowOutcome,
    StreamFollowerService,
    close_workflow_stream,
    follow_stream,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._nexus import (
    NewStore,
    connect,
    endpoint,
    run,
    until,
)
from tests.contrib.streams.samples.scenarios.n5c_late_attach import (
    DeliveryHandler,
    DeliveryService,
)

STAGES = topic("stages", str)
BACKLOG = [f"stage {n}" for n in range(50)]
LIVE = [f"stage {n}" for n in range(50, 55)]
PAGE = 10


@workflow.defn
class Delivery:
    def __init__(self) -> None:
        self._go_on = False

    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        for stage in BACKLOG:
            stages.publish(stage)
        await workflow.wait_condition(lambda: self._go_on)
        for stage in LIVE:
            stages.publish(stage)
        close_workflow_stream(f"{len(BACKLOG) + len(LIVE)} stages", topic=STAGES)

    @workflow.signal
    def go_on(self) -> None:
        self._go_on = True


@dataclass
class Dashboard:
    """Shows each stage once, by cursor. ``delay`` makes it slow per page."""

    delay: float = 0.0
    stages: dict[str, str] = field(default_factory=dict)
    pages: int = 0
    outcome: FollowOutcome | None = None
    closed: asyncio.Event = field(default_factory=asyncio.Event)

    async def on_records(self, batch: FollowBatch[str]) -> None:
        self.pages += 1
        for record in batch.records:
            self.stages.setdefault(record.cursor.token, record.value or "")
        await asyncio.sleep(self.delay)

    async def on_close(self, outcome: FollowOutcome) -> None:
        self.outcome = outcome
        self.closed.set()


@dataclass
class Followed:
    stages: list[str]
    pages: int
    close_result: Any


@dataclass
class Outcome:
    live: Followed
    after_close: Followed
    #: How long the dashboard that followed after the close took, in seconds.
    after_close_seconds: float


def _followed(dashboard: Dashboard) -> Followed:
    assert dashboard.outcome is not None
    return Followed(
        stages=list(dashboard.stages.values()),
        pages=dashboard.pages,
        close_result=dashboard.outcome.result,
    )


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "delivery-u5c",
    new_store: NewStore,
) -> Outcome:
    """Run the delivery with two dashboards and return what each one got.

    ``client`` is a client of a server with the stream notifier and NexusHandler
    callbacks on standalone operations.
    """
    streams = await connect(client, new_store().notify_on_append())
    name = await endpoint(client, task_queue)
    # Each page takes 1 s, so a call's budget of half the 10 s request time
    # ends before the backlog of 5 pages.
    live = Dashboard(delay=1.0)
    after_close = Dashboard()
    followers = StreamFollowerService(
        {"live": live, "after-close": after_close}, client=streams, max_records=PAGE
    )
    async with Worker(
        streams,
        task_queue=task_queue,
        workflows=[Delivery],
        nexus_service_handlers=[DeliveryHandler(), followers],
    ):
        delivery = await streams.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )

        async def backlog_written() -> bool:
            # The Workflow publishes the backlog in one task, so it lands at once.
            latest = await get_stream_handle(streams, workflow_id).latest(topic=STAGES)
            return latest != BEGINNING

        await until(backlog_written)

        async def dashboard(key: str) -> Any:
            return await follow_stream(
                streams,
                endpoint=name,
                service=DeliveryService,
                operation=DeliveryService.follow,
                arg=workflow_id,
                follower=key,
                task_queue=task_queue,
                id=f"{workflow_id}-{key}",
            )

        await dashboard("live")

        async def caught_up() -> bool:
            return len(live.stages) >= len(BACKLOG)

        await until(caught_up, timeout=60)
        await delivery.signal(Delivery.go_on)
        await asyncio.wait_for(live.closed.wait(), 30)
        await delivery.result()
        started = asyncio.get_running_loop().time()
        await dashboard("after-close")
        await asyncio.wait_for(after_close.closed.wait(), 30)
        took = asyncio.get_running_loop().time() - started
    return Outcome(
        live=_followed(live),
        after_close=_followed(after_close),
        after_close_seconds=took,
    )


if __name__ == "__main__":
    run(main)
