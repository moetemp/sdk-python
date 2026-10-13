"""Scenario 5a: a support dashboard opens a stream late and reads it from the start.

The app follows the stream live. After three records, a dashboard client
reads from ``BEGINNING``, catches up and then follows live too. Both reads
end when the Workflow closes. A dashboard that comes back after retention
dropped the records gets ``StreamExpiredError``.

Left out: a push to a late reader. The dashboard long-polls.

Run with ``python -m tests.contrib.streams.samples.scenarios.s5a_late_dashboard``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    StreamRecord,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

STATUS = topic("status", str)


@workflow.defn
class SupportCase:
    def __init__(self) -> None:
        self._go_on = False

    @workflow.run
    async def run(self) -> None:
        status = workflow_writer(STATUS)
        for step in ("opened", "assigned", "investigating"):
            status.publish(step)
        await workflow.wait_condition(lambda: self._go_on)
        for step in ("fix found", "fix shipped", "closed"):
            status.publish(step)

    @workflow.signal
    def go_on(self) -> None:
        self._go_on = True


@dataclass
class Seen:
    app: list[str]
    dashboard: list[str]


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "case-5a"
) -> Seen:
    """Run one case and return what the app and the late dashboard saw.

    ``client`` must carry the stream store.
    """
    async with Worker(client, task_queue=task_queue, workflows=[SupportCase]):
        handle = await client.start_workflow(
            SupportCase.run, id=workflow_id, task_queue=task_queue
        )
        stream = get_stream_handle(client, workflow_id)
        app_has_three = asyncio.Event()
        dashboard_caught_up = asyncio.Event()

        def count(seen: list[StreamRecord[str]], event: asyncio.Event):
            def on_record(_: StreamRecord[str]) -> None:
                if len(values(seen)) >= 3:
                    event.set()

            return on_record

        app: list[StreamRecord[str]] = []
        app_read = asyncio.create_task(
            follow(
                "app",
                stream.read(topic=STATUS),
                app,
                on_record=count(app, app_has_three),
            )
        )
        await app_has_three.wait()
        dashboard: list[StreamRecord[str]] = []
        dashboard_read = asyncio.create_task(
            follow(
                "dashboard",
                stream.read(topic=STATUS, after=BEGINNING),
                dashboard,
                on_record=count(dashboard, dashboard_caught_up),
            )
        )
        await dashboard_caught_up.wait()
        await handle.signal(SupportCase.go_on)
        await asyncio.gather(app_read, dashboard_read)
        await handle.result()
        return Seen(app=values(app), dashboard=values(dashboard))


if __name__ == "__main__":
    run(main)
