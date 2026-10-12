import asyncio
from collections.abc import Callable, Sequence
from typing import Any

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.history.v1
import temporalio.api.workflowservice.v1
import temporalio.client
import temporalio.service
from temporalio.client import WorkflowHistory


def make_nexus_endpoint_name(task_queue: str) -> str:
    # Create endpoints for different task queues without name collisions.
    return f"nexus-endpoint-{task_queue}"


def events_of_type(
    history: WorkflowHistory,
    event_type: temporalio.api.enums.v1.EventType.ValueType,
) -> list[temporalio.api.history.v1.HistoryEvent]:
    return [event for event in history.events if event.event_type == event_type]


def links_from_workflow_execution_started_event(
    event: temporalio.api.history.v1.HistoryEvent,
) -> list[temporalio.api.common.v1.Link]:
    callback_links = [
        link
        for callback in event.workflow_execution_started_event_attributes.completion_callbacks
        for link in callback.links
    ]
    if callback_links:
        return list(callback_links)
    return list(event.links)


def workflow_event_link_event_type(
    workflow_event: temporalio.api.common.v1.Link.WorkflowEvent,
) -> temporalio.api.enums.v1.EventType.ValueType:
    if workflow_event.HasField("request_id_ref"):
        return workflow_event.request_id_ref.event_type
    return workflow_event.event_ref.event_type


def expected_nexus_operation_link(
    *,
    namespace: str,
    operation_id: str,
    run_id: str,
) -> temporalio.api.common.v1.Link:
    return temporalio.api.common.v1.Link(
        nexus_operation=temporalio.api.common.v1.Link.NexusOperation(
            namespace=namespace,
            operation_id=operation_id,
            run_id=run_id,
        )
    )


def expected_workflow_event_link(
    *,
    namespace: str,
    workflow_id: str,
    run_id: str,
    event_type: temporalio.api.enums.v1.EventType.ValueType,
    event_id: int = 0,
    request_id: str | None = None,
) -> temporalio.api.common.v1.Link:
    if request_id is not None:
        return temporalio.api.common.v1.Link(
            workflow_event=temporalio.api.common.v1.Link.WorkflowEvent(
                namespace=namespace,
                workflow_id=workflow_id,
                run_id=run_id,
                request_id_ref=temporalio.api.common.v1.Link.WorkflowEvent.RequestIdReference(
                    request_id=request_id,
                    event_type=event_type,
                ),
            )
        )

    return temporalio.api.common.v1.Link(
        workflow_event=temporalio.api.common.v1.Link.WorkflowEvent(
            namespace=namespace,
            workflow_id=workflow_id,
            run_id=run_id,
            event_ref=temporalio.api.common.v1.Link.WorkflowEvent.EventReference(
                event_id=event_id,
                event_type=event_type,
            ),
        )
    )


def assert_links_match(
    links: Sequence[temporalio.api.common.v1.Link],
    *expected_links: temporalio.api.common.v1.Link,
) -> None:
    actual = sorted(list(links), key=_link_sort_key)
    expected = sorted(list(expected_links), key=_link_sort_key)
    assert actual == expected


def _link_sort_key(link: temporalio.api.common.v1.Link) -> bytes:
    return link.SerializeToString(deterministic=True)


async def end_standalone_operation(
    client: temporalio.client.Client,
    handle: temporalio.client.NexusOperationHandle[Any],
    served: Callable[[], bool],
    wait: float = 10.0,
) -> None:
    """Leave no standalone operation, nor a callback of one, behind a test.

    Call it while the Worker that takes the operation's callbacks still runs.
    A callback whose Worker is gone is retried against a task queue no one
    polls, which slows every later delivery on the server. So the operation is
    terminated, and if ``served`` doesn't turn true within ``wait`` seconds, as
    on a server that never calls back, the operation is deleted with its
    callbacks.
    """
    try:
        await handle.terminate(reason="the test ended")
    except temporalio.service.RPCError:
        pass
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait
    while not served() and loop.time() < deadline:
        await asyncio.sleep(0.1)
    if served():
        return
    try:
        await client.workflow_service.delete_nexus_operation_execution(
            temporalio.api.workflowservice.v1.DeleteNexusOperationExecutionRequest(
                namespace=client.namespace,
                operation_id=handle.operation_id,
                run_id=handle.run_id or "",
            )
        )
    except temporalio.service.RPCError:
        pass
