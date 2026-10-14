"""Scenario 1b in one namespace: a parent Workflow relays its child's stream.

The ``Order`` Workflow starts its ``Delivery`` child, reads the child's
``stages`` with ``workflow_reader``, and publishes each stage again on its
own ``relay`` topic with ``workflow_writer``. Between stages it sleeps on
the stream's notifier, with no Local Activity and no timer. The child closes
its stream when the delivery ends, which ends the parent's read. The app
reads only the parent's stream.

Left out: a child in another namespace (1c), which needs Nexus.

It needs a server with the stream notifier and the Workflow wake target,
and the store's ``notify_on_append()``.

Run with ``python -m tests.contrib.streams.samples.scenarios.j1b_parent_relay``.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    StreamRecord,
    close_workflow_stream,
    get_stream_handle,
    topic,
    workflow_reader,
    workflow_writer,
)
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

STAGES = topic("stages", str)
RELAY = topic("relay", str)


@workflow.defn
class Delivery:
    """Publishes its stages with gaps longer than the reader's idle wait, then closes."""

    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        for stage in ("picked up", "at the hub", "delivered"):
            stages.publish(stage)
            await stages.flush()
            await workflow.sleep(timedelta(seconds=3))
        await close_workflow_stream(STAGES, result="delivered")


@workflow.defn
class Order:
    """Relays its delivery's stages on its own stream."""

    @workflow.run
    async def run(self, delivery_id: str) -> int:
        child = await workflow.start_child_workflow(Delivery.run, id=delivery_id)
        out = workflow_writer(RELAY)
        relayed = 0
        async with workflow_reader(delivery_id, topic=STAGES) as reader:
            async for batch in reader:
                for record in batch.records:
                    if record.kind is RecordKind.DATA:
                        out.publish(f"delivery {record.value}")
                        relayed += 1
        await child
        await close_workflow_stream(RELAY, result=f"{relayed} stages")
        return relayed


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "order-j1b"
) -> list[str]:
    """Run one order and return what the app read on the parent's stream."""
    seen: list[StreamRecord[str]] = []
    async with Worker(client, task_queue=task_queue, workflows=[Order, Delivery]):
        handle = await client.start_workflow(
            Order.run,
            f"{workflow_id}-delivery",
            id=workflow_id,
            task_queue=task_queue,
        )
        records = get_stream_handle(client, workflow_id, topic=RELAY).read()
        await follow("app", records, seen)
        print(f"[app] the order relayed {await handle.result()} stages")
    return values(seen)


if __name__ == "__main__":
    run(main, notify=True)
