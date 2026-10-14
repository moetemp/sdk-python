"""Scenario 7a: a delivery fails, and the backend asks the Workflow why.

The courier Activity appends two stages, then fails with no retry. The
Workflow fails with a ``CourierLost`` error. The backend's read ends when
the Workflow ends, the same as for a success, since the stream does not
carry the outcome. So the backend then asks the Workflow for its result,
which raises the failure.

Left out: a close that carries the failure. ``j7a_close_with_error`` shows
it.

Run with ``python -m tests.contrib.streams.samples.scenarios.s7a_failed_delivery``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.streams import (
    StreamRecord,
    activity_handle,
    get_stream_handle,
    topic,
)
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, run, values

STAGES = topic("stages", str)


@dataclass
class Outcome:
    stages: list[str]
    failure_type: str | None


@activity.defn
async def deliver() -> None:
    stages = activity_handle().producer(topic=STAGES)
    await stages.append("picked up", "at the hub")
    raise ApplicationError(
        "the courier stopped answering", type="CourierLost", non_retryable=True
    )


@workflow.defn
class Delivery:
    @workflow.run
    async def run(self) -> None:
        try:
            await workflow.execute_activity(
                deliver, start_to_close_timeout=timedelta(seconds=10)
            )
        except ActivityError as error:
            # A reader asks the Workflow, so its failure names the cause.
            raise ApplicationError(
                "the courier stopped answering", type="CourierLost"
            ) from error


async def main(
    client: Client, task_queue: str, *, workflow_id: str = "delivery-7a"
) -> Outcome:
    """Run one failed delivery and return the stages and the failure the backend saw.

    ``client`` must carry the stream store.
    """
    async with Worker(
        client, task_queue=task_queue, workflows=[Delivery], activities=[deliver]
    ):
        handle = await client.start_workflow(
            Delivery.run, id=workflow_id, task_queue=task_queue
        )
        seen: list[StreamRecord[str]] = []
        await follow(
            "backend", get_stream_handle(client, workflow_id).read(topic=STAGES), seen
        )
        failure_type = None
        try:
            await handle.result()
        except WorkflowFailureError as error:
            if isinstance(error.cause, ApplicationError):
                failure_type = error.cause.type
        print(f"[backend] the delivery failed with {failure_type}")
        return Outcome(stages=values(seen), failure_type=failure_type)


if __name__ == "__main__":
    run(main)
