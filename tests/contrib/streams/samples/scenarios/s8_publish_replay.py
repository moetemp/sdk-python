"""Scenario 8: a publishing Workflow replays after its streams are gone.

The Workflow publishes, then completes. An admin deletes every stream of
the Workflow with ``delete_workflow_streams``. A ``Replayer`` with no stream
store then replays the History. Replay sends the publishes to Core without
bodies, and Core checks them against the recorded markers, so it needs no
store and writes nothing.

Left out: a Workflow that reads a stream and replays its reads. Brook has
no Workflow reader, so that half is the Bridge demo n8. A reset run's
records carry the new run id. Nothing retracts the old run's records.

Run with ``python -m tests.contrib.streams.samples.scenarios.s8_publish_replay``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRecord,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.worker import Replayer, Worker
from tests.contrib.streams.samples.scenarios._common import (
    follow,
    run,
    store_of,
    values,
)

EVENTS = topic("events", str)


@workflow.defn
class Publisher:
    @workflow.run
    async def run(self) -> None:
        events = workflow_writer(EVENTS)
        events.publish("one")
        await workflow.sleep(timedelta(milliseconds=50))
        events.publish("two")
        events.finish()


@dataclass
class Outcome:
    before_delete: list[str]
    keys_deleted: int
    after_replay: list[str]


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "publisher-8"
) -> Outcome:
    """Run, delete, replay, and return what the stream held at each step.

    ``client`` must carry the stream store. The replay raises if it fails.
    """
    async with Worker(client, task_queue=task_queue, workflows=[Publisher]):
        handle = await client.start_workflow(
            Publisher.run, id=workflow_id, task_queue=task_queue
        )
        await handle.result()
    stream = get_stream_handle(client, workflow_id)
    before: list[StreamRecord[str]] = []
    await follow("before", stream.read(topic=EVENTS), before)
    deleted = await store_of(client).delete_workflow_streams(
        client.namespace, workflow_id
    )
    print(f"[admin] deleted {deleted} store keys")
    await Replayer(workflows=[Publisher]).replay_workflow(await handle.fetch_history())
    print("[replay] the History replayed with no store")
    after: list[StreamRecord[str]] = []
    await follow("after", stream.read(topic=EVENTS), after)
    return Outcome(
        before_delete=values(before), keys_deleted=deleted, after_replay=values(after)
    )


if __name__ == "__main__":
    run(main)
