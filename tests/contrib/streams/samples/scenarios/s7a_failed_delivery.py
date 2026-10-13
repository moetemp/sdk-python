"""Scenario 7a: a delivery fails, and the backend learns why.

``Delivery`` publishes two stages, then raises
``ApplicationError(type="CourierLost")``. The backend's read ends when the
Workflow's run chain closes, failed or not. The stream says nothing of the
outcome, so the backend describes the Workflow and reads the failure from
its result.

Left out: a close that carries the failure to stream readers. That is
Bridge's fix for 7a, which completes a Nexus caller with the failure.

Run with ``python -m tests.contrib.streams.samples.scenarios.s7a_failed_delivery``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporalio.contrib.streams import (
    StreamRecord,
    get_stream_handle,
    topic,
    workflow_writer,
)
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

STAGES = topic("stages", str)


@workflow.defn
class Delivery:
    @workflow.run
    async def run(self) -> None:
        stages = workflow_writer(STAGES)
        stages.publish("assigned")
        await workflow.sleep(timedelta(milliseconds=100))
        stages.publish("picked up")
        await workflow.sleep(timedelta(milliseconds=100))
        raise ApplicationError("the courier stopped answering", type="CourierLost")


@dataclass
class Outcome:
    stages: list[str]
    status: WorkflowExecutionStatus | None
    failure_type: str | None


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "delivery-7a"
) -> Outcome:
    """Run the failing delivery and return what the backend learned.

    ``client`` must carry the stream store.
    """
    async with Worker(client, task_queue=task_queue, workflows=[Delivery]):
        handle = await client.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )
        seen: list[StreamRecord[str]] = []
        await follow(
            "backend", get_stream_handle(client, workflow_id).read(topic=STAGES), seen
        )
        status = (await handle.describe()).status
        print(f"[backend] the Workflow's status is {status.name if status else None}")
        failure_type = None
        try:
            await handle.result()
        except WorkflowFailureError as error:
            if isinstance(error.cause, ApplicationError):
                failure_type = error.cause.type
            print(f"[backend] the delivery failed: {error.cause}")
        return Outcome(stages=values(seen), status=status, failure_type=failure_type)


if __name__ == "__main__":
    run(main)
