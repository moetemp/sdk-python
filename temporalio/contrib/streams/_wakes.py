"""The stream wakes a Workflow run has seen, folded per reader.

The server records the wakes of a Workflow's stream readers on the Workflow
Task scheduled event, and Core hands each one to lang as a
``ResolveStreamWake`` job. Replay delivers the same wakes at the same
points, so a reader that waits on this state waits deterministically.

A wake says that the stream moved, not what it holds. The fold keeps the
highest counter per reader, and the flags until the reader takes them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure
from temporalio.api.stream.v1 import StreamWake

__all__ = ["WakeState", "fold_wake", "wake_state"]

# Held on the run's runtime, so the state goes with the run at eviction and
# replay builds it again from the same jobs.
_WAKES_ATTRIBUTE = "_temporal_stream_wakes"


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
    close_result: Payload | None = None
    close_failure: Failure | None = None


def fold_wake(runtime: Any, wake: StreamWake) -> None:
    """Fold ``wake`` into the state of its reader on ``runtime``."""
    states: dict[str, WakeState] | None = getattr(runtime, _WAKES_ATTRIBUTE, None)
    if states is None:
        states = {}
        setattr(runtime, _WAKES_ATTRIBUTE, states)
    state = states.setdefault(wake.subscription_id, WakeState())
    state.counter = max(state.counter, wake.counter)
    state.recheck = state.recheck or wake.recheck
    state.resubscribe = state.resubscribe or wake.resubscribe
    if wake.closed and not state.closed:
        # The notifier keeps the first close, so later wakes repeat it.
        state.closed = True
        if wake.HasField("close_result"):
            state.close_result = wake.close_result
        if wake.HasField("close_failure"):
            state.close_failure = wake.close_failure


def wake_state(subscription_id: str) -> WakeState | None:
    """The folded wakes of ``subscription_id`` in the running Workflow."""
    states: dict[str, WakeState] = getattr(
        workflow._Runtime.current(), _WAKES_ATTRIBUTE, {}
    )
    return states.get(subscription_id)
