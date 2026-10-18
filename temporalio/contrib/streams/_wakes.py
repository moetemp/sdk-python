"""The stream wakes a Workflow run has seen, folded per reader.

The server attaches the wakes of a Workflow's stream readers to the Workflow
Task scheduled event, each one an ``Any`` of ``StreamWake``. Core hands the
attachments of a task to lang in one ``WorkflowTaskAttachments`` job, and the
Workflow instance queues the wakes in event order. Replay delivers the same
job at the same point, so a reader that waits on this state waits
deterministically.

A wake says that the stream moved, not what it holds. The fold keeps the
highest counter per reader, and the flags until the reader takes them. A
close failure in a wake was encoded in the stream owner's context, so each
reader registers that context, and the Worker decodes the failure in it.
:func:`fold_stream_wakes` is the fold rule that every SDK shares, and the
sdk-streams vectors ``testdata/wake_folds.json`` check it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from temporalio import workflow
from temporalio.api.failure.v1 import Failure
from temporalio.api.stream.v1 import StreamWake
from temporalio.converter import SerializationContext

__all__ = [
    "WakeState",
    "fold_stream_wakes",
    "fold_wake",
    "register_wake_context",
    "wake_state",
]

# Held on the run's runtime, so the state goes with the run at eviction and
# replay builds it again from the same jobs.
_WAKES_ATTRIBUTE = "_temporal_stream_wake_states"


@dataclass
class WakeState:
    """What the wakes of one reader said so far."""

    counter: int = 0
    """The highest counter of any wake."""
    recheck: bool = False
    """The notifier asked for a read with no new counter, and no read took it yet."""
    resubscribe: bool = False
    """The notifier dropped the reader, and the reader has not attached again yet."""
    closed: bool = False
    close_failure: Failure | None = None


def fold_stream_wakes(wakes: list[StreamWake]) -> list[StreamWake]:
    """Fold ``wakes`` to one wake per subscription, in order of first appearance.

    The wake with the highest counter gives the counter, position and
    metadata. A recheck, a resubscribe or a close on any wake stays, since each
    one asks the reader to act whatever the counter. The first close gives the
    outcome.
    """
    folded: dict[str, StreamWake] = {}
    for wake in wakes:
        held = folded.get(wake.subscription_id)
        if held is None:
            held = StreamWake()
            held.CopyFrom(wake)
            folded[wake.subscription_id] = held
            continue
        if wake.counter > held.counter:
            held.counter = wake.counter
            held.position = wake.position
            held.ClearField("metadata")
            held.metadata.update(wake.metadata)
        held.recheck = held.recheck or wake.recheck
        held.resubscribe = held.resubscribe or wake.resubscribe
        if wake.closed and not held.closed:
            held.closed = True
            held.ClearField("close_result")
            held.ClearField("close_failure")
            if wake.HasField("close_result"):
                held.close_result.CopyFrom(wake.close_result)
            if wake.HasField("close_failure"):
                held.close_failure.CopyFrom(wake.close_failure)
    return list(folded.values())


def fold_wake(runtime: Any, wake: StreamWake) -> None:
    """Fold ``wake`` into its reader's state."""
    state = _states(runtime).setdefault(wake.subscription_id, WakeState())
    state.counter = max(state.counter, wake.counter)
    state.recheck = state.recheck or wake.recheck
    state.resubscribe = state.resubscribe or wake.resubscribe
    if wake.closed and not state.closed:
        # The notifier keeps the first close, so later wakes repeat it.
        state.closed = True
        if wake.HasField("close_failure"):
            state.close_failure = wake.close_failure


def wake_state(subscription_id: str) -> WakeState | None:
    """The folded wakes of ``subscription_id`` in the running Workflow."""
    runtime = workflow._Runtime.current()
    queued: list[StreamWake] = getattr(runtime, "_stream_wakes", [])
    for wake in fold_stream_wakes(queued):
        fold_wake(runtime, wake)
    queued.clear()
    return _states(runtime).get(subscription_id)


def register_wake_context(subscription_id: str, context: SerializationContext) -> None:
    """Have the Worker decode the wakes of ``subscription_id`` in ``context``."""
    runtime: Any = workflow._Runtime.current()
    runtime._stream_wake_contexts[subscription_id] = context


def _states(runtime: Any) -> dict[str, WakeState]:
    states: dict[str, WakeState] | None = getattr(runtime, _WAKES_ATTRIBUTE, None)
    if states is None:
        states = {}
        setattr(runtime, _WAKES_ATTRIBUTE, states)
    return states
