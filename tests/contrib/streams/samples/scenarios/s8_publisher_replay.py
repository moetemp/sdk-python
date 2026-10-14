"""Scenario 8: a publisher Workflow replays with no store I/O.

``Publisher`` publishes three stages with ``workflow_writer`` and finishes
its topic. Each flush is one Local Activity, and History keeps its result
in a marker. The demo then replays the History with a ``Replayer``. The
Replayer needs the store plugin, which adds the interceptor that
``workflow_writer`` replays with. The plugin connects nothing, so the
demo gives the Replayer a Redis store at a port where nothing listens.
The replay takes each flush's result from its marker and passes.

Left out: a Workflow that reads a stream.

Run with ``python -m tests.contrib.streams.samples.scenarios.s8_publisher_replay``.
"""

from __future__ import annotations

from dataclasses import dataclass

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRecord,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.contrib.streams.redis import RedisStreams
from temporalio.worker import Replayer, Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

STAGES = topic("stages", str)

# Nothing listens on port 1, so a replay that read the store would fail.
NO_STORE = "redis://127.0.0.1:1"


@dataclass
class Outcome:
    stages: list[str]
    markers: int


@workflow.defn
class Publisher:
    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        stages.publish("placed")
        await stages.flush()
        stages.publish("packed")
        stages.publish("shipped")
        await stages.finish()


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "publisher-8"
) -> Outcome:
    """Run the publisher, replay its History with no store, and return what it published.

    ``client`` must carry the stream store.
    """
    async with Worker(client, task_queue=task_queue, workflows=[Publisher]):
        handle = await client.start_workflow(
            Publisher.run, id=workflow_id, task_queue=task_queue
        )
        await handle.result()
    seen: list[StreamRecord[str]] = []
    await follow(
        "backend", get_stream_handle(client, workflow_id).read(topic=STAGES), seen
    )
    history = await handle.fetch_history()
    markers = sum(
        event.HasField("marker_recorded_event_attributes") for event in history.events
    )
    replayer = Replayer(
        workflows=[Publisher],
        plugins=[RedisStreams(NO_STORE, key_prefix="replay")],
    )
    await replayer.replay_workflow(history)
    print(f"[replayer] replayed {markers} flush markers with no store")
    return Outcome(stages=values(seen), markers=markers)


if __name__ == "__main__":
    run(main)
