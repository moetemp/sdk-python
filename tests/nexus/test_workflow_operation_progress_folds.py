"""Nexus operation progress as the server attaches it to a Workflow Task.

Each progress is an ``Any`` of ``NexusOperationProgress`` in a
``WorkflowTaskAttachments`` job, keyed by endpoint, service, operation and
token. The instance folds the progress of a task to one per key and gives it
to each started, unresolved operation with that key. The fold and the
deliveries must give the results of the sdk-streams vectors.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import pytest
from google.protobuf.any_pb2 import Any as AnyProto

from temporalio import workflow
from temporalio.api.nexus.v1 import NexusOperationProgress
from temporalio.bridge.proto.workflow_activation import WorkflowTaskAttachments
from temporalio.worker._workflow_instance import (
    _NexusOperationHandle,
    _WorkflowInstanceImpl,
    fold_nexus_operation_progress,
)
from tests.contrib.streams._support import streams_crate_dir


def _vectors() -> dict[str, Any]:
    with open(streams_crate_dir() / "testdata" / "nexus_progress_folds.json") as f:
        return json.load(f)


def _progress(fields: dict[str, Any]) -> NexusOperationProgress:
    fields = dict(fields)
    key = fields.pop("key", None)
    progress = NexusOperationProgress(**fields)
    if key is not None:
        progress.operation_key.CopyFrom(NexusOperationProgress.OperationKey(**key))
    return progress


@pytest.mark.parametrize("case", _vectors()["folds"], ids=lambda case: case["name"])
def test_the_fold_gives_the_shared_vectors(case: dict[str, Any]):
    folded = fold_nexus_operation_progress([_progress(p) for p in case["progress"]])
    assert folded == [_progress(p) for p in case["folded"]]


class _Handle:
    """An operation handle with the real progress step, which records what it took."""

    def __init__(
        self,
        seq: int,
        endpoint: str,
        service: str,
        operation: str,
        delivered: list[dict[str, Any]],
    ):
        self.seq = seq
        self._delivered = delivered
        self._input = SimpleNamespace(
            endpoint=endpoint, service=service, operation_name=operation
        )
        self.operation_token: str | None = None
        self._latest_progress: workflow.NexusOperationProgress | None = None
        self._progress_waiters: list[Any] = []

    def _resolve_progress(self, progress: workflow.NexusOperationProgress) -> None:
        before = self._latest_progress
        _NexusOperationHandle._resolve_progress(self, progress)  # type: ignore[arg-type]
        if self._latest_progress is not before:
            self._delivered.append(
                {
                    "seq": self.seq,
                    "counter": progress.counter,
                    "position": progress.position,
                }
            )


class _Instance:
    """The instance's state that the attachments job reads, with its real steps."""

    _apply_nexus_operation_progress = (
        _WorkflowInstanceImpl._apply_nexus_operation_progress
    )

    def __init__(self) -> None:
        self._pending_nexus_operations: dict[int, _Handle] = {}
        self._stream_wakes: list[Any] = []

    def call_soon(self, callback: Any) -> None:
        del callback


def _packed(progress: NexusOperationProgress) -> AnyProto:
    attachment = AnyProto()
    attachment.Pack(progress)
    return attachment


@pytest.mark.parametrize(
    "case", _vectors()["deliveries"], ids=lambda case: case["name"]
)
def test_the_deliveries_give_the_shared_vectors(case: dict[str, Any]):
    instance = _Instance()
    delivered: list[dict[str, Any]] = []
    for step in case["steps"]:
        if "schedule" in step:
            s = step["schedule"]
            handle = _Handle(
                s["seq"], s["endpoint"], s["service"], s["operation"], delivered
            )
            instance._pending_nexus_operations[handle.seq] = handle
        elif "start" in step:
            started = instance._pending_nexus_operations[step["start"]["seq"]]
            started.operation_token = step["start"]["operation_token"]
        elif "resolve" in step:
            instance._pending_nexus_operations.pop(step["resolve"]["seq"])
        else:
            delivered.clear()
            _WorkflowInstanceImpl._apply_workflow_task_attachments(
                cast(Any, instance),
                WorkflowTaskAttachments(
                    attachments=[_packed(_progress(p)) for p in step["attachments"]]
                ),
            )
            assert delivered == step["delivered"]
