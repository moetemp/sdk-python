"""A producer identity for a Local Activity that survives its reruns.

A Local Activity's attempt is not durable. After a Worker crash or a failed
Workflow Task, the Workflow schedules the same call again and it starts at
attempt one, so the store would refuse its first append as stale. The
store plugin gives each call an identity that holds across reruns:

- **Producer id.** The plugin's Workflow interceptor numbers the run's Local
  Activity calls in the order Workflow code makes them, which replay
  repeats, and stamps the number on the call in a header. The producer id is
  ``__temporal_local_<number>@<run id>``. The Activity id is not used, since a retry
  that backs off longer than ``local_retry_threshold`` is scheduled again
  under a new one.
- **Producer attempt.** Core puts the Workflow Task facts of the task in
  which it dispatched the call on the Local Activity's start. It takes them
  from History. A rerun after a failed task is scheduled while Core replays
  the committed tasks, but Core dispatches it only in the live task, so the
  rerun always gets a later task than the first run. The task attempt is
  ``prefix << 16 | retry``, the same value the Workflow's own records use,
  and the producer attempt is ``task attempt << 14 | min(a, 16383)``, with
  ``a`` the Local Activity's own attempt. A rerun writes a higher attempt,
  and so does a local retry. The task attempt stays below 2**32 for a
  History under the event limit, so the producer attempt stays below 2**46
  and a reader that holds it in a double reads it exactly.
- **Task facts.** The call's records carry the started event id of its
  Workflow Task attempt and the run's last completed tasks, as the
  Workflow's records do. So a later record of the run tells a reader that
  the call's task never completed, and the reader withdraws the call's
  records. That covers a retried task that doesn't run the call again.
"""

from __future__ import annotations

import contextvars
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import temporalio.activity
import temporalio.worker
from temporalio import workflow
from temporalio.api.common.v1 import Payload

LOCAL_ACTIVITY_HEADER = "__temporal_streams_local_activity"
"""The header that carries a Local Activity call's number."""

# The bits of the task attempt that hold the retry, and of the producer attempt that hold the
# Local Activity's own attempt.
_RETRY_BITS = 16
_LOCAL_BITS = 14
_ENCODING = b"json/plain"


def task_attempt(
    completed_started_event_ids: Sequence[int], failed_attempts: int
) -> int:
    """The attempt a Workflow Task's records write, from its History facts."""
    prefix = completed_started_event_ids[0] if completed_started_event_ids else 0
    retry = min(1 + failed_attempts, (1 << _RETRY_BITS) - 1)
    return prefix << _RETRY_BITS | retry


def producer_attempt(task_attempt: int, local_attempt: int) -> int:
    """Encode a Local Activity's producer attempt."""
    return task_attempt << _LOCAL_BITS | min(local_attempt, (1 << _LOCAL_BITS) - 1)


@dataclass(frozen=True)
class LocalCall:
    """One Local Activity call, with the Workflow Task that Core dispatched it in."""

    number: int
    task_started_event_id: int
    failed_attempts: int
    completed_started_event_ids: tuple[int, ...]

    @property
    def producer_id_prefix(self) -> str:
        """What comes before ``@<run id>`` in the producer id."""
        return f"__temporal_local_{self.number}"

    def attempt(self, local_attempt: int) -> int:
        """The producer attempt of the call's Local Activity attempt ``local_attempt``."""
        return producer_attempt(
            task_attempt(self.completed_started_event_ids, self.failed_attempts),
            local_attempt,
        )


_local_call: contextvars.ContextVar[LocalCall | None] = contextvars.ContextVar(
    "temporal_streams_local_call", default=None
)


def current_local_call() -> LocalCall | None:
    """The running Local Activity call, if its Workflow numbered it."""
    return _local_call.get()


class StreamsInterceptor(temporalio.worker.Interceptor):
    """Numbers each Local Activity call and reads the number in the Activity.

    Its Workflow interceptor also tells ``stream_producer`` that the Worker
    has a store.
    """

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        """Read the number and the Workflow Task of a Local Activity call."""
        return _ActivityInbound(next)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor]:
        """Number the Workflow's Local Activity calls."""
        return _WorkflowInbound


class _ActivityInbound(temporalio.worker.ActivityInboundInterceptor):
    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        stamp = input.headers.get(LOCAL_ACTIVITY_HEADER)
        task = temporalio.activity._Context.current().workflow_task
        if stamp is None or task is None or not temporalio.activity.info().is_local:
            return await self.next.execute_activity(input)
        call = LocalCall(
            number=json.loads(stamp.data)["call"],
            task_started_event_id=task.started_event_id,
            failed_attempts=task.failed_attempts,
            completed_started_event_ids=tuple(task.completed_started_event_ids),
        )
        token = _local_call.set(call)
        try:
            return await self.next.execute_activity(input)
        finally:
            _local_call.reset(token)


class _WorkflowInbound(temporalio.worker.WorkflowInboundInterceptor):
    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        super().init(_WorkflowOutbound(outbound))


class _WorkflowOutbound(temporalio.worker.WorkflowOutboundInterceptor):
    def __init__(self, next: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        super().__init__(next)
        # One instance per run, made again on replay, which repeats the calls in order.
        self._calls = 0

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> workflow.ActivityHandle[Any]:
        self._calls += 1
        input.headers = _stamped(input.headers, self._calls)
        return super().start_local_activity(input)


def _stamped(headers: Mapping[str, Payload], call: int) -> Mapping[str, Payload]:
    stamp = Payload(
        metadata={"encoding": _ENCODING},
        data=json.dumps({"call": call}, separators=(",", ":")).encode(),
    )
    return {**headers, LOCAL_ACTIVITY_HEADER: stamp}
