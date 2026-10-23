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
            ref=_ref(request.ref) if request.HasField("ref") else None,
        )
    )
    if response.status == stream_pb.StreamStatus.STREAM_STATUS_CLOSED:
        raise _errors.StreamClosedError(
            f"The Stream {request.stream_id} was created and closed before",
            reason="stream_closed",
        )
    return 0


# The Store's limits on the Records that it keeps, as the crate counts them.
_MAX_RECORD_BYTES = 1536 * 1024
_MAX_BATCH_BYTES = 4 * 1024 * 1024


@dataclasses.dataclass
class _Encoded:
    # The batches of one append and the calls of each, with the bodies after the codec, and
    # the answer of each batch that the Store took.
    batches: list[tuple[list[int], list[stream_pb.AppendRecord]]]
    answers: dict[int, Any] = dataclasses.field(default_factory=dict)


# The appends that have not succeeded yet, by Producer and epoch. A retry of the Local Activity
# in this process sends the same bytes, so the Store's dedupe of a resend matches them, also
# when the codec adds random bytes. A rerun after a crash is a new epoch, so it needs none.
_PENDING: OrderedDict[bytes, _Encoded] = OrderedDict()
_PENDING_LIMIT = 1024


async def _encode(
    streams: _activity.ActivityStreams,
    request: stream_pb.AppendRecordsRequest,
    counts: list[int],
    producer: stream_pb.Producer,
) -> _Encoded:
    # The Workflow converted the values, so only the codec runs on them here.
    bodies = [record.body for record in request.records]
    codec = streams.converter.payload_codec
    if codec is not None:
        bodies = await codec.encode(bodies)
    records = [
        stream_pb.AppendRecord(topic=record.topic, body=body)
        for record, body in zip(request.records, bodies)
    ]
    sizes = [
        stream_pb.StreamRecord(
            kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA,
            topic=record.topic,
            producer=producer,
            body=record.body,
        ).ByteSize()
        for record in records
    ]
    if sum(sizes) <= _MAX_BATCH_BYTES and max(sizes, default=0) <= _MAX_RECORD_BYTES:
        return _Encoded([(list(range(len(counts))), records)])
    # The codec grew the merge past the Store's limit, so each call goes as its own batch, as
    # it would with no merge. A call that is too large alone fails alone.
    batches: list[tuple[list[int], list[stream_pb.AppendRecord]]] = []
    start = 0
    for call, count in enumerate(counts):
        batches.append(([call], records[start : start + count]))
        start += count
    return _Encoded(batches)


@temporalio.activity.defn(name=APPEND_ACTIVITY)
async def append_records(
    request: stream_pb.AppendRecordsRequest, counts: list[int]
) -> list[Any]:
    """Append the Records of a Workflow's merged publishes.

    ``counts`` has the Records of each publish. Gives each publish the offset of its last
    Record, or the error of a publish that the Store refused as too large.
    """
    streams, service = _context()
    producer = _producer(streams)
    producer_pb = models._ProducerTransferTypeConverter().to_transfer_type(producer)
    key = b"\n".join(
        [
            streams.namespace.encode(),
            request.stream.id.encode(),
            producer_pb.SerializeToString(deterministic=True),
        ]
    )
    encoded = _PENDING.get(key)
    if encoded is None:
        encoded = await _encode(streams, request, counts, producer_pb)
        _PENDING[key] = encoded
        while len(_PENDING) > _PENDING_LIMIT:
            _PENDING.popitem(last=False)
    # The bodies went through the codec once already.
    service = streams.backend.service(
        streams.namespace, dataclasses.replace(streams.converter, payload_codec=None)
    )
    answers: list[Any] = [0] * len(counts)
    for index, (calls, records) in enumerate(encoded.batches):
        answer = encoded.answers.get(index)
        if answer is None:
            try:
                response = await service.append_records(
                    models.AppendRecordsRequest(
                        stream=_ref(request.stream),
                        producer=producer,
                        sequence=index + 1,
                        records=[
                            models.AppendRecord(
                                topic=record.topic,
                                body=temporalio.common.RawValue(record.body),
                            )
                            for record in records
                        ],
                        withdraw_run=(request.withdraw_run or None)
                        if index == 0
                        else None,
                        **_history(streams),
                    )
                )
                answer = response.last_offset or 0
            except _errors.StreamError as err:
                if err.reason != "stream_too_large":
                    raise
                answer = {"reason": err.reason, "message": err.message}
            encoded.answers[index] = answer
        # A Store gives one batch consecutive offsets, so each call's last Record is the
        # batch's last one less the Records after it.
        after = sum(counts[call] for call in calls)
        for call in calls:
            after -= counts[call]
            answers[call] = answer - after if isinstance(answer, int) else answer
    _PENDING.pop(key, None)
    return answers


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
            status=request.status or None,
            **_history(streams),
        )
    )
    return 0


ACTIVITIES = [create_stream, append_records, close_stream]
