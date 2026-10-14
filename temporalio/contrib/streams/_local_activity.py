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
- **Producer attempt.** Before its first append, the Local Activity asks
  the server for its run's History length and the attempt of the pending
  Workflow Task. A rerun runs in a later Workflow Task attempt, which either
  added an event to History or, while the task keeps failing, raised the
  task's attempt with History unchanged. So that pair grows from one run of
  the call to the next, and the producer attempt puts it in front of the
  Local Activity's own attempt, which grows across local retries.

The Workflow can't stamp the History length itself. Replay repeats the
length a task had, so a call rescheduled by replaying a task that completed
(for example a Workflow Task heartbeat while the call ran) gets the length
of its first run. And a Workflow Task that fails again runs at the same
length, since the server records only its first failure.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import temporalio.activity
import temporalio.worker
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.contrib.streams._errors import StreamUnsupportedError

LOCAL_ACTIVITY_HEADER = "__temporal_streams_local_activity"
"""The header that carries a Local Activity call's number."""

LOCAL_ATTEMPTS = 10_000
"""How many Local Activity attempts a producer attempt has room for."""

TASK_ATTEMPTS = 100_000
"""How many attempts of one Workflow Task a producer attempt has room for."""

# Kept below 2**53, so a reader that holds the attempt in a double reads it exactly.
_MAX_HISTORY_LENGTH = (2**53 - 1) // (TASK_ATTEMPTS * LOCAL_ATTEMPTS)
_ENCODING = b"json/plain"


def producer_attempt(history_length: int, task_attempt: int, local_attempt: int) -> int:
    """Encode a Local Activity's producer attempt.

    Raises:
        StreamUnsupportedError: A part is too large to encode.
    """
    for name, value, limit in (
        ("History length", history_length, _MAX_HISTORY_LENGTH + 1),
        ("Workflow Task attempt", task_attempt, TASK_ATTEMPTS),
        ("Local Activity attempt", local_attempt, LOCAL_ATTEMPTS),
    ):
        if value >= limit:
            raise StreamUnsupportedError(
                f"a Local Activity at {name} {value} has no stream producer, since "
                f"a producer attempt holds values below {limit}"
            )
    return (history_length * TASK_ATTEMPTS + task_attempt) * LOCAL_ATTEMPTS + (
        local_attempt
    )


@dataclass
class LocalCall:
    """One Local Activity call, as the running attempt sees it."""

    number: int
    _attempt: int | None = None
    _resolving: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def producer_id_prefix(self) -> str:
        """What comes before ``@<run id>`` in the producer id."""
        return f"__temporal_local_{self.number}"

    async def attempt(self, client: Client) -> int:
        """The producer attempt of the running Local Activity attempt.

        Asks the server once per Local Activity attempt.

        Raises:
            StreamUnsupportedError: A part is too large to encode.
        """
        async with self._resolving:
            if self._attempt is None:
                info = temporalio.activity.info()
                assert info.workflow_id and info.workflow_run_id
                described = await client.get_workflow_handle(
                    info.workflow_id, run_id=info.workflow_run_id
                ).describe()
                task = described.raw_description.pending_workflow_task
                self._attempt = producer_attempt(
                    described.history_length, task.attempt, info.attempt
                )
            return self._attempt


_local_call: contextvars.ContextVar[LocalCall | None] = contextvars.ContextVar(
    "temporal_streams_local_call", default=None
)


def current_local_call() -> LocalCall | None:
    """The running Local Activity call, if its Workflow stamped it."""
    return _local_call.get()


class StreamsInterceptor(temporalio.worker.Interceptor):
    """Numbers each Local Activity call and reads the number in the Activity.

    It also sends what the run's Workflow writers buffered before the run
    ends.
    """

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        """Read the number of a Local Activity call."""
        return _ActivityInbound(next)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor]:
        """Number the Workflow's Local Activity calls and drain its writers."""
        return _WorkflowInbound


class _ActivityInbound(temporalio.worker.ActivityInboundInterceptor):
    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        stamp = input.headers.get(LOCAL_ACTIVITY_HEADER)
        if stamp is None or not temporalio.activity.info().is_local:
            return await self.next.execute_activity(input)
        token = _local_call.set(LocalCall(number=json.loads(stamp.data)["call"]))
        try:
            return await self.next.execute_activity(input)
        finally:
            _local_call.reset(token)


class _WorkflowInbound(temporalio.worker.WorkflowInboundInterceptor):
    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        super().init(_WorkflowOutbound(outbound))

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        # Imported here, since the writer module imports this one.
        from temporalio.contrib.streams._workflow_writer import run_then_drain

        return await run_then_drain(super().execute_workflow(input))


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
