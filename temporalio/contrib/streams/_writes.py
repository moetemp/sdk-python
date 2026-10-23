"""The Local Activities that write for Workflow code.

Each Workflow ``create_stream`` and ``close`` is one of them, and the publishes that wait
behind a write to the same Stream share one. History has the answer and not the values.
"""

from __future__ import annotations

import dataclasses
from collections import OrderedDict
from typing import Any

import temporalio.activity
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.common
from temporalio.contrib.streams import _activity, _errors
from temporalio.contrib.streams.system import client as service_client
from temporalio.contrib.streams.system import models
from temporalio.contrib.streams.workflow import (
    APPEND_ACTIVITY,
    CLOSE_ACTIVITY,
    CREATE_ACTIVITY,
)


def _context() -> tuple[_activity.ActivityStreams, service_client.StreamServiceClient]:
    streams = _activity.current.get()
    if streams is None:
        raise _errors.StreamConfigError("No Stream plugin runs this Local Activity")
    return streams, streams.backend.service(streams.namespace, streams.converter)


def _producer(streams: _activity.ActivityStreams) -> models.Producer:
    # Every attempt of one Workflow Task writes as one epoch with one sequence, so the backend
    # drops the copy that a retry sends. A later task has a higher epoch, so its write
    # withdraws what a failed task wrote under the same call key.
    producer = streams.producer
    epoch = producer.epoch
    started = epoch.task_started_time if epoch.HasField("task_started_time") else None
    return models.Producer(
        kind=producer.kind,
        id=producer.id,
        run_id=producer.run_id or None,
        epoch=models.ProducerEpoch(
            task_started_event_id=epoch.task_started_event_id or None,
            task_attempt=epoch.task_attempt or None,
            task_started_time=models.Timestamp(
                seconds=started.seconds, nanos=started.nanos
            )
            if started is not None
            else None,
            attempt=1,
        ),
    )


def _history(streams: _activity.ActivityStreams) -> dict[str, Any]:
    # The run's task history, so the write withdraws the run's orphaned Local Activity Records.
    previous = streams.previous_completed_task
    return {
        "previous_completed_task": models.WorkflowTaskEpoch(
            started_event_id=previous.started_event_id
        )
        if previous is not None
        else None,
        "failed_tasks": list(streams.failed_tasks),
    }


def _ref(ref: stream_pb.StreamRef) -> models.StreamRef:
    return models.StreamRef(
        id=ref.id,
        workflow_id=ref.workflow_id or None,
        first_execution_run_id=ref.first_execution_run_id or None,
    )


@temporalio.activity.defn(name=CREATE_ACTIVITY)
async def create_stream(request: stream_pb.CreateStreamRequest) -> int:
    """Create a Workflow's Stream."""
    _, service = _context()
    response = await service.create_stream(
        models.CreateStreamRequest(
            stream_id=request.stream_id,
            retention=request.retention.ToTimedelta()
            if request.HasField("retention")
            else None,
            auto_close=request.auto_close.ToTimedelta()
            if request.HasField("auto_close")
            else None,
            withdraw_run=request.withdraw_run or None,
            owner=models.Producer(
                kind=request.owner.kind,
                id=request.owner.id,
                run_id=request.owner.run_id or None,
            ),
        )
    )
    if response.status == stream_pb.StreamStatus.STREAM_STATUS_CLOSED:
        raise _errors.StreamClosedError(
            f"The Stream {request.stream_id} was created and closed before",
            reason="stream_closed",
        )
    return 0


# The appends that have not succeeded yet, by Producer and epoch, with their bodies after the
# codec. A retry of the Local Activity in this process sends the same bytes, so the Store's
# dedupe of a resend matches them, also when the codec adds random bytes. A rerun after a crash
# is a new epoch, so it needs none.
_PENDING: OrderedDict[bytes, list[stream_pb.AppendRecord]] = OrderedDict()
_PENDING_LIMIT = 1024


async def _encode(
    streams: _activity.ActivityStreams, request: stream_pb.AppendRecordsRequest
) -> list[stream_pb.AppendRecord]:
    # The Workflow converted the values, so only the codec runs on them here.
    bodies = [record.body for record in request.records]
    codec = streams.converter.payload_codec
    if codec is not None:
        bodies = await codec.encode(bodies)
    return [
        stream_pb.AppendRecord(topic=record.topic, body=body)
        for record, body in zip(request.records, bodies)
    ]


@temporalio.activity.defn(name=APPEND_ACTIVITY)
async def append_records(request: stream_pb.AppendRecordsRequest) -> int:
    """Append a Workflow's Records as one batch, and return the last offset."""
    streams, _ = _context()
    producer = _producer(streams)
    key = b"\n".join(
        [
            streams.namespace.encode(),
            request.stream.id.encode(),
            models._ProducerTransferTypeConverter()
            .to_transfer_type(producer)
            .SerializeToString(deterministic=True),
        ]
    )
    records = _PENDING.get(key)
    if records is None:
        records = await _encode(streams, request)
        _PENDING[key] = records
        while len(_PENDING) > _PENDING_LIMIT:
            _PENDING.popitem(last=False)
    # The bodies went through the codec once already.
    service = streams.backend.service(
        streams.namespace, dataclasses.replace(streams.converter, payload_codec=None)
    )
    response = await service.append_records(
        models.AppendRecordsRequest(
            stream=_ref(request.stream),
            producer=producer,
            sequence=1,
            records=[
                models.AppendRecord(
                    topic=record.topic, body=temporalio.common.RawValue(record.body)
                )
                for record in records
            ],
            withdraw_run=request.withdraw_run or None,
            **_history(streams),
        )
    )
    _PENDING.pop(key, None)
    return response.last_offset or 0


@temporalio.activity.defn(name=CLOSE_ACTIVITY)
async def close_stream(request: stream_pb.CloseStreamRequest) -> int:
    """Close a Workflow's Stream."""
    streams, service = _context()
    failure: Any = None
    if request.HasField("failure"):
        failure = streams.converter.failure_converter.from_failure(
            request.failure, streams.converter.payload_converter
        )
    await service.close_stream(
        models.CloseStreamRequest(
            stream=_ref(request.stream),
            producer=_producer(streams),
            result=temporalio.common.RawValue(request.result)
            if request.HasField("result")
            else None,
            failure=failure,
            **_history(streams),
        )
    )
    return 0


ACTIVITIES = [create_stream, append_records, close_stream]
