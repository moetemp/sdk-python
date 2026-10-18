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
- **Producer attempt.** The interceptor also stamps the Workflow Task facts
  of the activation that schedules the call. Core takes them from History.
  The task attempt is ``prefix << 16 | retry``, the same value the
  Workflow's own records use, and the producer attempt is ``task attempt <<
  14 | min(a, 16383)``, with ``a`` the Local Activity's own attempt. A rerun
  in a later task attempt writes a higher attempt, and so does a local
  retry. The task attempt stays below 2**32 for a History under the event
  limit, so the producer attempt stays below 2**46 and a reader that holds it
  in a double reads it exactly.
- **Task facts.** The call's records carry the started event id of its
  Workflow Task attempt and the run's last completed tasks, as the
  Workflow's records do. So a later record of the run tells a reader that
  the call's task never completed, and the reader withdraws the call's
  records. That covers a retried task that doesn't run the call again.
"""

from __future__ import annotations

import contextvars
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import temporalio.activity
import temporalio.worker
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.bridge.streams import STREAM_OWNER_HEADER
from temporalio.contrib.streams._errors import StreamUnsupportedError

LOCAL_ACTIVITY_HEADER = "__temporal_streams_local_activity"
"""The header that carries a Local Activity call's number and task facts."""

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


def require_local_activity(name: str) -> None:
    """Refuse to run a stream Activity that a Workflow scheduled as a normal one.

    The plugin registers the stream Activities on every Worker, so Workflow
    code can name them in a normal Activity call. That call would write an
    Activity's events in History in place of the reader's or closer's marker,
    and its retries would not follow the Local Activity rules.

    Raises:
        StreamUnsupportedError: The running Activity is not a Local Activity.
    """
    if not temporalio.activity.info().is_local:
        raise StreamUnsupportedError(
            f"{name} runs only as the Local Activity that the stream API "
            "schedules, not as a normal Activity"
        )


@dataclass(frozen=True)
class LocalCall:
    """One Local Activity call, as its Workflow stamped it."""

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
    """The running Local Activity call, if its Workflow stamped it."""
    return _local_call.get()


_owns_stream: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "temporal_streams_owns_stream", default=False
)


def owns_stream() -> bool:
    """Whether the Workflow scheduled the running Activity to own its stream."""
    return _owns_stream.get()


@dataclass
class _OwnerRequest:
    activity_id: str | None = None


_owner_request: contextvars.ContextVar[_OwnerRequest | None] = contextvars.ContextVar(
    "temporal_streams_owner_request", default=None
)


def start_owning_activity(
    start: Callable[[], workflow.ActivityHandle[Any]],
) -> tuple[workflow.ActivityHandle[Any], str]:
    """Run ``start`` with the stream owner header on the Activity it schedules.

    Returns the handle and the Activity id the command carries.
    """
    request = _OwnerRequest()
    token = _owner_request.set(request)
    try:
        handle = start()
    finally:
        _owner_request.reset(token)
    if request.activity_id is None:
        raise StreamUnsupportedError(
            "the stream owner header was not set, since the Workflow runs without "
            "the stream store's Worker interceptor; build the Worker from a client "
            "that carries the store"
        )
    return handle, request.activity_id


class StreamsInterceptor(temporalio.worker.Interceptor):
    """Stamps each Local Activity call and reads the stamp in the Activity.

    Its Workflow interceptor also tells ``stream_producer`` that the Worker
    has a store.
    """

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        """Read the stamp of a Local Activity call."""
        return _ActivityInbound(next)

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor]:
        """Stamp the Workflow's Local Activity calls."""
        return _WorkflowInbound


class _ActivityInbound(temporalio.worker.ActivityInboundInterceptor):
    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        if temporalio.activity.info().is_local:
            stamp = input.headers.get(LOCAL_ACTIVITY_HEADER)
            if stamp is None:
                return await self.next.execute_activity(input)
            fields = json.loads(stamp.data)
            call = LocalCall(
                number=fields["call"],
                task_started_event_id=fields["started"],
                failed_attempts=fields["failed"],
                completed_started_event_ids=tuple(fields["completed"]),
            )
            token = _local_call.set(call)
            try:
                return await self.next.execute_activity(input)
            finally:
                _local_call.reset(token)
        if STREAM_OWNER_HEADER not in input.headers:
            return await self.next.execute_activity(input)
        owner_token = _owns_stream.set(True)
        try:
            return await self.next.execute_activity(input)
        finally:
            _owns_stream.reset(owner_token)


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

    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> workflow.ActivityHandle[Any]:
        request = _owner_request.get()
        if request is None:
            return super().start_activity(input)
        # Only the helper's own call takes the header, not one an inner
        # interceptor starts.
        _owner_request.set(None)
        input.headers = {**input.headers, STREAM_OWNER_HEADER: _OWNER_MARK}
        handle = super().start_activity(input)
        activity_id = input.activity_id
        if activity_id is None:
            # The default id is the call's sequence, which only the SDK's handle holds.
            seq = getattr(handle, "_seq", None)
            if seq is None:
                raise StreamUnsupportedError(
                    "an interceptor replaced the Activity handle, so the default "
                    "Activity id is unknown; pass activity_id"
                )
            activity_id = str(seq)
        request.activity_id = activity_id
        return handle


_OWNER_MARK = Payload(metadata={"encoding": _ENCODING}, data=b"true")


def _stamped(headers: Mapping[str, Payload], call: int) -> Mapping[str, Payload]:
    task = workflow._Runtime.current().workflow_task_history_info()
    fields = {
        "call": call,
        "started": task.started_event_id,
        "failed": task.failed_attempts,
        "completed": list(task.completed_started_event_ids),
    }
    stamp = Payload(
        metadata={"encoding": _ENCODING},
        data=json.dumps(fields, separators=(",", ":")).encode(),
    )
    return {**headers, LOCAL_ACTIVITY_HEADER: stamp}
