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
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.streams import _activity, _backend, _errors, _ids
from temporalio.streams._workflow import (
    APPEND_ACTIVITY,
    CLOSE_ACTIVITY,
    CREATE_ACTIVITY,
)
from temporalio.streams.system import models


def _context() -> tuple[_activity.ActivityStreams, _backend.StreamServiceCalls]:
    streams = _activity.current.get()
    if streams is None:
        raise _errors.StreamConfigError("No Stream backend runs this Local Activity")
    return streams, streams.backend.service(streams.namespace, streams.converter)


def _producer(streams: _activity.ActivityStreams) -> models.Producer:
    # All attempts of one Workflow Task write with one epoch and sequence, so the Store dedupes a
    # retry. A later task's higher epoch for the same call key fences the failed task's writes,
    # and with_superseded() withdraws its Records.
    local = streams.producer.local_activity
    task_pb = local.epoch.task
    started = task_pb.started_time if task_pb.HasField("started_time") else None
    # The Store refuses an epoch with a missing field, so the zero values go as they are.
    task = models.WorkflowTaskEpoch(
        started_event_id=task_pb.started_event_id,
        attempt=task_pb.attempt,
        started_time=models.Timestamp(seconds=started.seconds, nanos=started.nanos)
        if started is not None
        else None,
    )
    return models.Producer(
        id=streams.producer.id,
        kind=models.ProducerKindLocalActivity(
            models.LocalActivityProducer(
                run_id=local.run_id,
                epoch=models.LocalActivityEpoch(task=task, attempt=1),
            )
        ),
    )


def _model_ref(ref: stream_v1.StreamRef) -> models.StreamRef:
    return models._StreamRefTransferTypeConverter[Any]().from_transfer_type(
        ref, models.StreamRef
    )


@temporalio.activity.defn(name=CREATE_ACTIVITY)
async def create_stream(request: stream_pb.CreateStreamRequest) -> str:
    """Create a Workflow's standalone Stream, or with ``USE_EXISTING`` get it."""
    _, service = _context()
    # The request has the Workflow's request id, so a retry of this Activity isn't a conflict. A
    # closed, expired or another owner's Stream fails the create.
    await service._stream_service_create_stream(
        models._CreateStreamRequestTransferTypeConverter().from_transfer_type(
            request, models.CreateStreamRequest
        ),
        temporalio.common.RawValue,
    )
    return ""


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
    # Opaque offsets have no arithmetic, so a merged append's per-publish offsets can't be read
    # back from its endpoints. Each publish is its own batch, so the Store returns its last
    # offset. The publishes still share one Local Activity, so History keeps one marker.
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
    key = (streams.namespace, request.ref.id, producer_pb.id)
    task = producer_pb.local_activity.epoch.task
    epoch = (
        task.started_event_id,
        task.attempt,
        task.started_time.seconds,
        task.started_time.nanos,
    )
    encoded = _PENDING.get(key, epoch)
    if encoded is None:
        encoded = await _encode(streams, request, counts, producer_pb)
        _PENDING.put(key, epoch, encoded)
    # The codec already ran on the bodies.
    service = streams.backend.service(
        streams.namespace, dataclasses.replace(streams.converter, payload_codec=None)
    )
    answers: list[Any] = [""] * len(counts)
    for index, (calls, records) in enumerate(encoded.batches):
        answer = encoded.answers.get(index)
        if answer is None:
            try:
                response = await service.send_append_records(
                    models.AppendRecordsRequest(
                        ref=_model_ref(request.ref),
                        producer=producer,
                        sequence=index + 1,
                        records=[
                            models.AppendRecord(
                                body=temporalio.common.RawValue(record.body)
                            )
                            for record in records
                        ],
                    )
                )
                answer = response.last_offset or b""
            except _errors.StreamError as err:
                if err.reason != StreamErrorReason.STREAM_ERROR_REASON_TOO_LARGE:
                    if err.non_retryable:
                        _PENDING.pop(key, epoch)
                    raise
                answer = {"reason": int(err.reason), "message": err.message}
            encoded.answers[index] = answer
        # Each batch is one call, so its last offset is the call's last Record. A hex string
        # carries the opaque offset through the Local Activity result.
        for call in calls:
            answers[call] = answer.hex() if isinstance(answer, bytes) else answer
    _PENDING.pop(key, epoch)
    return answers


@temporalio.activity.defn(name=CLOSE_ACTIVITY)
async def close_stream(request: stream_pb.CloseStreamRequest) -> str:
    """Close a Stream as the Workflow run that scheduled this Activity."""
    streams, service = _context()
    # The close Activity runs in the Workflow, so its Producer carries the run id, and the Store
    # closes the Stream as the run's Workflow Producer.
    await service.send_close_stream(
        models.CloseStreamRequest(
            ref=_model_ref(request.ref),
            producer=_ids.workflow_producer(streams.producer.workflow_activity.run_id),
        )
    )
    return ""


ACTIVITIES = [create_stream, append_records, close_stream]
