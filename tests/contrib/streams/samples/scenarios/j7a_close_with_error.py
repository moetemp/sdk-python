"""Scenario 7a: a delivery fails, and its stream's close carries the failure.

``Delivery`` publishes two stages with ``workflow_writer``. Then the
courier stops answering, and it closes its stream with
``close_workflow_stream(error=...)``, a ``CourierLost`` failure. The store
keeps the close's outcome. Two readers follow the stream:

- a backend's client read, which raises the failure after its last record,
- a ``Dispatcher`` Workflow's ``workflow_reader``, which raises it after its
  last batch.

Neither reader asks the Workflow. Both end on the close while ``Delivery``
still runs.

Left out: an owner that fails with no close. The notifier's owner check
closes its stream with no outcome from History.

It needs a server with the stream notifier and the Workflow wake target,
and the store's ``notify_on_append()``.

Run with ``python -m tests.contrib.streams.samples.scenarios.j7a_close_with_error``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams import (
    RecordKind,
    StreamRecord,
    close_workflow_stream,
    get_stream_handle,
    topic,
    workflow_reader,
    workflow_writer,
)
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

STAGES = topic("stages", str)


@dataclass
class Seen:
    stages: list[str] = field(default_factory=list)
    failure_type: str | None = None


@dataclass
class Outcome:
    backend: Seen
    dispatcher: Seen
    owner_ran_on: bool


@workflow.defn
class Delivery:
    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        for stage in ("picked up", "at the hub"):
            stages.publish(stage)
            await stages.flush()
            await workflow.sleep(timedelta(seconds=3))
        lost = ApplicationError("the courier stopped answering", type="CourierLost")
        await close_workflow_stream(STAGES, error=lost)
        # It runs on, so the readers end on the close and not on the run's end.
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


@workflow.defn
class Dispatcher:
    """Follows a delivery's stages in Workflow code, and learns how it failed."""

    @workflow.run
    async def run(self, delivery_id: str) -> Seen:
        seen = Seen()
        try:
            async with workflow_reader(delivery_id, topic=STAGES) as reader:
                async for batch in reader:
                    for record in batch.records:
                        if record.kind is RecordKind.DATA:
                            seen.stages.append(str(record.value))
        except ApplicationError as error:
            seen.failure_type = error.type
        return seen


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "delivery-j7a"
) -> Outcome:
    """Run one failed delivery and return what each reader saw."""
    async with Worker(client, task_queue=task_queue, workflows=[Delivery, Dispatcher]):
        delivery = await client.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )
        dispatcher = await client.start_workflow(
            Dispatcher.run,
            workflow_id,
            id=f"{workflow_id}-dispatcher",
            task_queue=task_queue,
        )
        records: list[StreamRecord[str]] = []
        backend = Seen()
        try:
            await follow(
                "backend",
                get_stream_handle(client, workflow_id, topic=STAGES).read(),
                records,
            )
        except ApplicationError as error:
            print(f"[backend] the read raised {error.type}: {error.message}")
            backend.failure_type = error.type
        backend.stages = values(records)
        seen = await dispatcher.result()
        print(f"[dispatcher] read {seen.stages} and raised {seen.failure_type}")
        running = WorkflowExecutionStatus.RUNNING
        owner_ran_on = (await delivery.describe()).status == running
        await delivery.signal(Delivery.done)
        await delivery.result()
    return Outcome(backend=backend, dispatcher=seen, owner_ran_on=owner_ran_on)


if __name__ == "__main__":
    run(main, notify=True)
