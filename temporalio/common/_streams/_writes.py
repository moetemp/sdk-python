"""The Activities that write for Workflow code.

Each Workflow ``create_stream`` is a Local Activity. The publishes that wait behind a write to
the same Stream share one Local Activity. History has the result and not the values. Each
Workflow ``close`` is a regular Activity, so it runs only after its Workflow Task completes.
"""

from __future__ import annotations

import dataclasses
from collections import OrderedDict
from typing import Any

import temporalio.activity
import temporalio.api.stream.v1 as stream_v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.common
from temporalio.api.enums.v1 import StreamErrorReason, StreamStatus
from temporalio.common._streams import _activity, _errors, _ids
from temporalio.common._streams._workflow import (
    APPEND_ACTIVITY,
    CLOSE_ACTIVITY,
    CREATE_ACTIVITY,
)
from temporalio.common._streams.system import client as service_client
from temporalio.common._streams.system import models


def _context() -> tuple[
    _activity.ActivityStreams, service_client.StreamServiceClientExtension
]:
    streams = _activity.current.get()
    if streams is None:
        raise _errors.StreamConfigError("No Stream backend runs this Local Activity")
    return streams, streams.backend.service(streams.namespace, streams.converter)


def _producer(streams: _activity.ActivityStreams) -> models.Producer:
    # All attempts of one Workflow Task write with one epoch and sequence, so the Store dedupes a
    # retry. A later task's higher epoch withdraws a failed task's Records for the same call key.
    producer = streams.producer
    epoch = producer.epoch
    started = epoch.task_started_time if epoch.HasField("task_started_time") else None
    return models.Producer(
        kind=models.StreamProducerKind(producer.kind),
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


def _ref(ref: stream_v1.StreamRef) -> models.StreamRef:
    return models._StreamRefTransferTypeConverter().from_transfer_type(
        ref, models.StreamRef
    )


@temporalio.activity.defn(name=CREATE_ACTIVITY)
async def create_stream(request: stream_pb.CreateStreamRequest) -> int:
    """Create a Workflow's Stream, or get the one that exists."""
    _, service = _context()
    created = await service.create_stream(
        stream_id=request.stream_id,
        retention=request.retention.ToTimedelta()
        if request.HasField("retention")
        else None,
        withdraw_run=request.withdraw_run or None,
        owner=models.Producer(
            kind=models.StreamProducerKind(request.owner.kind),
            id=request.owner.id,
            run_id=request.owner.run_id or None,
        ),
        ref=_ref(request.ref) if request.HasField("ref") else None,
    )
    if created.status == StreamStatus.STREAM_STATUS_CLOSED:
        raise _errors.StreamClosedError(
            f"The Stream {request.stream_id} was created and closed before",
            reason=StreamErrorReason.STREAM_ERROR_REASON_CLOSED,
        )
    if created.status == StreamStatus.STREAM_STATUS_EXPIRED:
        raise _errors.StreamExpiredError(
            f"The Stream {request.stream_id} expired",
            reason=StreamErrorReason.STREAM_ERROR_REASON_EXPIRED,
        )
    return 0


# The Store's limits on the Records that it keeps, as the crate counts them.
_MAX_RECORD_BYTES = 1536 * 1024
_MAX_BATCH_BYTES = 4 * 1024 * 1024


@dataclasses.dataclass
class _Encoded:
    # The batches of one append, with each batch's calls and encoded bodies, and the result of
    # each batch that the Store accepted.
    batches: list[tuple[list[int], list[stream_v1.AppendRecord]]]
    size: int
    answers: dict[int, Any] = dataclasses.field(default_factory=dict)


_Key = tuple[str, str, str]
_Epoch = tuple[int, int, int, int]


class _Pending:
    # The appends that haven't succeeded, one for each Producer. A retry in this process resends
    # the same bytes, so the Store's dedupe matches them, even with a random codec.

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._size = 0
        self._entries: OrderedDict[_Key, tuple[_Epoch, _Encoded]] = OrderedDict()

    def get(self, key: _Key, epoch: _Epoch) -> _Encoded | None:
        found = self._entries.get(key)
        return found[1] if found is not None and found[0] == epoch else None

    def put(self, key: _Key, epoch: _Epoch, encoded: _Encoded) -> None:
        found = self._entries.get(key)
        if found is not None:
            if found[0] > epoch:
                # The Store fences this write, since the Producer has a later epoch.
                return
            self.pop(key, found[0])
        self._entries[key] = (epoch, encoded)
        self._size += encoded.size
        while self._size > self._limit and len(self._entries) > 1:
            _, (_, oldest) = self._entries.popitem(last=False)
            self._size -= oldest.size

    def pop(self, key: _Key, epoch: _Epoch) -> None:
        found = self._entries.get(key)
        if found is not None and found[0] == epoch:
            del self._entries[key]
            self._size -= found[1].size


# A run that ends while its append retries leaves its entry, so evict the oldest past this size.
_PENDING = _Pending(64 * 1024 * 1024)


async def _encode(
    streams: _activity.ActivityStreams,
    request: stream_pb.AppendRecordsRequest,
    counts: list[int],
    producer: stream_v1.Producer,
) -> _Encoded:
    # The Workflow converted the values, so only the codec runs on them here.
    bodies = [record.body for record in request.records]
    codec = streams.converter.payload_codec
    if codec is not None:
        bodies = await codec.encode(bodies)
    records = [stream_v1.AppendRecord(body=body) for body in bodies]
    sizes = [
        stream_v1.StreamRecord(
            data=stream_v1.StreamData(producer=producer, body=record.body)
        ).ByteSize()
        for record in records
    ]
    if sum(sizes) <= _MAX_BATCH_BYTES and max(sizes, default=0) <= _MAX_RECORD_BYTES:
        return _Encoded([(list(range(len(counts))), records)], sum(sizes))
    # The codec grew the merge past the Store's limit, so send each call as its own batch, as
    # with no merge. A call that's too large by itself fails by itself.
    batches: list[tuple[list[int], list[stream_v1.AppendRecord]]] = []
    start = 0
    for call, count in enumerate(counts):
        batches.append(([call], records[start : start + count]))
        start += count
    return _Encoded(batches, sum(sizes))


@temporalio.activity.defn(name=APPEND_ACTIVITY)
async def append_records(
    request: stream_pb.AppendRecordsRequest, counts: list[int]
) -> list[Any]:
    """Append the Records of a Workflow's merged publishes.

    ``counts`` has the Record count of each publish. Returns each publish's last offset, or the
    error of a publish that the Store refused as too large.
    """
    streams, service = _context()
    producer = _producer(streams)
    producer_pb = models._ProducerTransferTypeConverter().to_transfer_type(producer)
    key = (streams.namespace, request.stream.id, producer_pb.id)
    task = producer_pb.epoch
    epoch = (
        task.task_started_event_id,
        task.task_attempt,
        task.task_started_time.seconds,
        task.task_started_time.nanos,
    )
    encoded = _PENDING.get(key, epoch)
    if encoded is None:
        encoded = await _encode(streams, request, counts, producer_pb)
        _PENDING.put(key, epoch, encoded)
    # The codec already ran on the bodies.
    service = streams.backend.service(
        streams.namespace, dataclasses.replace(streams.converter, payload_codec=None)
    )
    answers: list[Any] = [0] * len(counts)
    for index, (calls, records) in enumerate(encoded.batches):
        answer = encoded.answers.get(index)
        if answer is None:
            try:
                response = await (
                    await service.append_records(
                        stream=_ref(request.stream),
                        producer=producer,
                        sequence=index + 1,
                        records=[
                            models.AppendRecord(
                                body=temporalio.common.RawValue(record.body)
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
                if err.reason != StreamErrorReason.STREAM_ERROR_REASON_TOO_LARGE:
                    if err.non_retryable:
                        _PENDING.pop(key, epoch)
                    raise
                answer = {"reason": err.reason, "message": err.message}
            encoded.answers[index] = answer
        # A Store gives one batch consecutive offsets, so each call's last Record is the
        # batch's last one less the Records after it.
        after = sum(counts[call] for call in calls)
        for call in calls:
            after -= counts[call]
            answers[call] = answer - after if isinstance(answer, int) else answer
    _PENDING.pop(key, epoch)
    return answers


@temporalio.activity.defn(name=CLOSE_ACTIVITY)
async def close_stream(request: stream_pb.CloseStreamRequest) -> int:
    """Close a Workflow's Stream, as the run chain that owns it."""
    streams, service = _context()
    # The Store refuses an Activity Producer's close of a Workflow's Stream, so the Activity
    # writes the close as the run that scheduled it, and only for that run's chain.
    if request.stream.owner.business_id != streams.workflow_id:
        raise _errors.StreamNotOwnerError(
            f"Can't close the Stream {request.stream.id} for another Workflow",
            reason=StreamErrorReason.STREAM_ERROR_REASON_NOT_OWNER,
        )
    run_id = streams.producer.run_id
    failure: Any = None
    if request.HasField("failure"):
        failure = streams.converter.failure_converter.from_failure(
            request.failure, streams.converter.payload_converter
        )
    await (
        await service.close_stream(
            stream=_ref(request.stream),
            producer=models.Producer(
                kind=models.StreamProducerKind.StreamProducerKindWorkflow,
                id=_ids.workflow_producer_id(run_id),
                run_id=run_id,
            ),
            result=temporalio.common.RawValue(request.result)
            if request.HasField("result")
            else None,
            failure=failure,
            status=models.StreamCloseStatus(request.status) if request.status else None,
        )
    )
    return 0


ACTIVITIES = [create_stream, append_records, close_stream]
