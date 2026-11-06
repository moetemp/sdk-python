# Written by hand in the shape of nexgen's client output, since the writer has no WIT. Keep it
# close to that output, so a later switch to generated code changes no caller.

from __future__ import annotations

import dataclasses
import datetime
import enum
import typing

import google.protobuf.timestamp_pb2
import typing_extensions

import temporalio.api.common.v1.message_pb2
import temporalio.api.enums.v1.common_pb2
import temporalio.api.stream.v1.message_pb2
import temporalio.bridge.proto.streams.streams_pb2
import temporalio.converter
import temporalio.nexus.system._support as _support


class ExecutionType(enum.IntEnum):
    ExecutionTypeUnspecified = 0
    ExecutionTypeWorkflow = 1
    ExecutionTypeActivity = 2
    ExecutionTypeNexusOperation = 3


def _execution_type_from_proto(value: int) -> ExecutionType:
    try:
        return ExecutionType(value)
    except ValueError:
        # A newer peer can send a case that this enum doesn't know.
        return ExecutionType.ExecutionTypeUnspecified


@dataclasses.dataclass(slots=True)
class OpenWriterRequest:
    """Open a writer for one Producer of one Stream."""

    ref: StreamRef
    writer: StreamWriter | None = None
    max_batch_size: int | None = None
    max_retry_duration: datetime.timedelta | None = None


class _OpenWriterRequestTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        OpenWriterRequest,
        temporalio.bridge.proto.streams.streams_pb2.OpenWriterRequest,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.OpenWriterRequest] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.OpenWriterRequest

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.OpenWriterRequest,
        type_hint: type[OpenWriterRequest],
    ) -> OpenWriterRequest:
        if not value.HasField("ref"):
            raise ValueError("missing required field OpenWriterRequest.ref")
        ref = _StreamRefTransferTypeConverter().from_transfer_type(value.ref, StreamRef)
        return OpenWriterRequest(
            ref=ref,
            writer=_StreamWriterTransferTypeConverter().from_transfer_type(
                value.writer, StreamWriter
            )
            if value.HasField("writer")
            else None,
            max_batch_size=value.max_batch_size if value.max_batch_size != 0 else None,
            max_retry_duration=_support.duration_from_proto(value.max_retry_duration)
            if value.HasField("max_retry_duration")
            else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: OpenWriterRequest,
    ) -> temporalio.bridge.proto.streams.streams_pb2.OpenWriterRequest:
        message = temporalio.bridge.proto.streams.streams_pb2.OpenWriterRequest()
        message.ref.CopyFrom(
            _StreamRefTransferTypeConverter().to_transfer_type(value.ref)
        )
        if value.writer is not None:
            message.writer.CopyFrom(
                _StreamWriterTransferTypeConverter().to_transfer_type(value.writer)
            )
        if value.max_batch_size is not None:
            message.max_batch_size = value.max_batch_size
        if value.max_retry_duration is not None:
            message.max_retry_duration.CopyFrom(
                _support.duration_to_proto(value.max_retry_duration)
            )
        return message


temporalio.converter.transfer_type_convertible(_OpenWriterRequestTransferTypeConverter)(
    OpenWriterRequest
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class StreamRef:
    """Identifies one Stream. The id is opaque to callers."""

    id: str
    owner: Execution | None = None
    backend: str | None = None


class _StreamRefTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        StreamRef, temporalio.api.stream.v1.message_pb2.StreamRef
    ]
):
    transfer_type: type[temporalio.api.stream.v1.message_pb2.StreamRef] | None = (
        temporalio.api.stream.v1.message_pb2.StreamRef
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.StreamRef,
        type_hint: type[StreamRef],
    ) -> StreamRef:
        if not value.id:
            raise ValueError("missing required field StreamRef.id")
        id = value.id
        return StreamRef(
            id=id,
            owner=_ExecutionTransferTypeConverter().from_transfer_type(
                value.owner, Execution
            )
            if value.HasField("owner")
            else None,
            backend=value.backend if bool(value.backend) else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: StreamRef,
    ) -> temporalio.api.stream.v1.message_pb2.StreamRef:
        message = temporalio.api.stream.v1.message_pb2.StreamRef()
        message.id = value.id
        if value.owner is not None:
            message.owner.CopyFrom(
                _ExecutionTransferTypeConverter().to_transfer_type(value.owner)
            )
        if value.backend is not None:
            message.backend = value.backend
        return message


temporalio.converter.transfer_type_convertible(_StreamRefTransferTypeConverter)(
    StreamRef
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class Execution:
    """A Workflow run chain or a Standalone Activity run."""

    type: ExecutionType | None = None
    business_id: str | None = None
    run_id: str | None = None


class _ExecutionTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        Execution, temporalio.api.common.v1.message_pb2.Execution
    ]
):
    transfer_type: type[temporalio.api.common.v1.message_pb2.Execution] | None = (
        temporalio.api.common.v1.message_pb2.Execution
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.common.v1.message_pb2.Execution,
        type_hint: type[Execution],
    ) -> Execution:
        return Execution(
            type=_execution_type_from_proto(value.type) if value.type != 0 else None,
            business_id=value.business_id if bool(value.business_id) else None,
            run_id=value.run_id if bool(value.run_id) else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: Execution,
    ) -> temporalio.api.common.v1.message_pb2.Execution:
        message = temporalio.api.common.v1.message_pb2.Execution()
        if value.type is not None:
            message.type = temporalio.api.enums.v1.common_pb2.ExecutionType.ValueType(
                value.type
            )
        if value.business_id is not None:
            message.business_id = value.business_id
        if value.run_id is not None:
            message.run_id = value.run_id
        return message


temporalio.converter.transfer_type_convertible(_ExecutionTransferTypeConverter)(
    Execution
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True, kw_only=True)
class StreamWriter:
    """The Producer of a write, and which earlier Records the write withdraws."""

    producer: Producer | None = None
    previous_completed_task: WorkflowTaskEpoch | None = None
    failed_tasks: list[int]
    withdraw_run: str | None = None


class _StreamWriterTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        StreamWriter, temporalio.api.stream.v1.message_pb2.StreamWriter
    ]
):
    transfer_type: type[temporalio.api.stream.v1.message_pb2.StreamWriter] | None = (
        temporalio.api.stream.v1.message_pb2.StreamWriter
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.StreamWriter,
        type_hint: type[StreamWriter],
    ) -> StreamWriter:
        failed_tasks = list(value.failed_tasks)
        return StreamWriter(
            producer=_ProducerTransferTypeConverter().from_transfer_type(
                value.producer, Producer
            )
            if value.HasField("producer")
            else None,
            previous_completed_task=_WorkflowTaskEpochTransferTypeConverter().from_transfer_type(
                value.previous_completed_task, WorkflowTaskEpoch
            )
            if value.HasField("previous_completed_task")
            else None,
            failed_tasks=failed_tasks,
            withdraw_run=value.withdraw_run if bool(value.withdraw_run) else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: StreamWriter,
    ) -> temporalio.api.stream.v1.message_pb2.StreamWriter:
        message = temporalio.api.stream.v1.message_pb2.StreamWriter()
        if value.producer is not None:
            message.producer.CopyFrom(
                _ProducerTransferTypeConverter().to_transfer_type(value.producer)
            )
        if value.previous_completed_task is not None:
            message.previous_completed_task.CopyFrom(
                _WorkflowTaskEpochTransferTypeConverter().to_transfer_type(
                    value.previous_completed_task
                )
            )
        message.failed_tasks.extend(value.failed_tasks)
        if value.withdraw_run is not None:
            message.withdraw_run = value.withdraw_run
        return message


temporalio.converter.transfer_type_convertible(_StreamWriterTransferTypeConverter)(
    StreamWriter
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class Producer:
    """The Producer of a batch."""

    id: str
    epoch: ProducerEpoch | None = None
    run_id: str | None = None


class _ProducerTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        Producer, temporalio.api.stream.v1.message_pb2.Producer
    ]
):
    transfer_type: type[temporalio.api.stream.v1.message_pb2.Producer] | None = (
        temporalio.api.stream.v1.message_pb2.Producer
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.Producer,
        type_hint: type[Producer],
    ) -> Producer:
        if not value.id:
            raise ValueError("missing required field Producer.id")
        id = value.id
        return Producer(
            id=id,
            epoch=_ProducerEpochTransferTypeConverter().from_transfer_type(
                value.epoch, ProducerEpoch
            )
            if value.HasField("epoch")
            else None,
            run_id=value.run_id if bool(value.run_id) else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: Producer,
    ) -> temporalio.api.stream.v1.message_pb2.Producer:
        message = temporalio.api.stream.v1.message_pb2.Producer()
        message.id = value.id
        if value.epoch is not None:
            message.epoch.CopyFrom(
                _ProducerEpochTransferTypeConverter().to_transfer_type(value.epoch)
            )
        if value.run_id is not None:
            message.run_id = value.run_id
        return message


temporalio.converter.transfer_type_convertible(_ProducerTransferTypeConverter)(Producer)


@dataclasses.dataclass(slots=True)
class ProducerEpoch:
    """The ordered attempt of a Producer. A higher epoch supersedes a lower one."""

    kind: ProducerEpochKind | None = None


class _ProducerEpochTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        ProducerEpoch, temporalio.api.stream.v1.message_pb2.ProducerEpoch
    ]
):
    transfer_type: type[temporalio.api.stream.v1.message_pb2.ProducerEpoch] | None = (
        temporalio.api.stream.v1.message_pb2.ProducerEpoch
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.ProducerEpoch,
        type_hint: type[ProducerEpoch],
    ) -> ProducerEpoch:
        _oneof_kind: (
            ProducerEpochKindWorkflow
            | ProducerEpochKindLocalActivity
            | ProducerEpochKindActivity
            | ProducerEpochKindStandaloneActivity
            | ProducerEpochKindClient
            | None
        )
        match value.WhichOneof("kind"):
            case "workflow":
                _oneof_kind = ProducerEpochKindWorkflow(
                    _WorkflowTaskEpochTransferTypeConverter().from_transfer_type(
                        value.workflow, WorkflowTaskEpoch
                    )
                )
            case "local_activity":
                _oneof_kind = ProducerEpochKindLocalActivity(
                    _LocalActivityEpochTransferTypeConverter().from_transfer_type(
                        value.local_activity, LocalActivityEpoch
                    )
                )
            case "activity":
                _oneof_kind = ProducerEpochKindActivity(
                    _ActivityEpochTransferTypeConverter().from_transfer_type(
                        value.activity, ActivityEpoch
                    )
                )
            case "standalone_activity":
                _oneof_kind = ProducerEpochKindStandaloneActivity(
                    _ActivityEpochTransferTypeConverter().from_transfer_type(
                        value.standalone_activity, ActivityEpoch
                    )
                )
            case "client":
                _oneof_kind = ProducerEpochKindClient(
                    _ClientEpochTransferTypeConverter().from_transfer_type(
                        value.client, ClientEpoch
                    )
                )
            case None:
                _oneof_kind = None
        return ProducerEpoch(
            kind=_oneof_kind,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: ProducerEpoch,
    ) -> temporalio.api.stream.v1.message_pb2.ProducerEpoch:
        message = temporalio.api.stream.v1.message_pb2.ProducerEpoch()
        if value.kind is not None:
            match value.kind:
                case ProducerEpochKindWorkflow():
                    message.workflow.CopyFrom(
                        _WorkflowTaskEpochTransferTypeConverter().to_transfer_type(
                            value.kind.value
                        )
                    )
                case ProducerEpochKindLocalActivity():
                    message.local_activity.CopyFrom(
                        _LocalActivityEpochTransferTypeConverter().to_transfer_type(
                            value.kind.value
                        )
                    )
                case ProducerEpochKindActivity():
                    message.activity.CopyFrom(
                        _ActivityEpochTransferTypeConverter().to_transfer_type(
                            value.kind.value
                        )
                    )
                case ProducerEpochKindStandaloneActivity():
                    message.standalone_activity.CopyFrom(
                        _ActivityEpochTransferTypeConverter().to_transfer_type(
                            value.kind.value
                        )
                    )
                case ProducerEpochKindClient():
                    message.client.CopyFrom(
                        _ClientEpochTransferTypeConverter().to_transfer_type(
                            value.kind.value
                        )
                    )
        return message


temporalio.converter.transfer_type_convertible(_ProducerEpochTransferTypeConverter)(
    ProducerEpoch
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class WorkflowTaskEpoch:
    """A Workflow Task attempt of the run."""

    started_event_id: int | None = None
    attempt: int | None = None
    started_time: Timestamp | None = None


class _WorkflowTaskEpochTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        WorkflowTaskEpoch, temporalio.api.stream.v1.message_pb2.WorkflowTaskEpoch
    ]
):
    transfer_type: (
        type[temporalio.api.stream.v1.message_pb2.WorkflowTaskEpoch] | None
    ) = temporalio.api.stream.v1.message_pb2.WorkflowTaskEpoch

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.WorkflowTaskEpoch,
        type_hint: type[WorkflowTaskEpoch],
    ) -> WorkflowTaskEpoch:
        return WorkflowTaskEpoch(
            started_event_id=value.started_event_id
            if value.started_event_id != 0
            else None,
            attempt=value.attempt if value.attempt != 0 else None,
            started_time=_TimestampTransferTypeConverter().from_transfer_type(
                value.started_time, Timestamp
            )
            if value.HasField("started_time")
            else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: WorkflowTaskEpoch,
    ) -> temporalio.api.stream.v1.message_pb2.WorkflowTaskEpoch:
        message = temporalio.api.stream.v1.message_pb2.WorkflowTaskEpoch()
        if value.started_event_id is not None:
            message.started_event_id = value.started_event_id
        if value.attempt is not None:
            message.attempt = value.attempt
        if value.started_time is not None:
            message.started_time.CopyFrom(
                _TimestampTransferTypeConverter().to_transfer_type(value.started_time)
            )
        return message


temporalio.converter.transfer_type_convertible(_WorkflowTaskEpochTransferTypeConverter)(
    WorkflowTaskEpoch
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class Timestamp:
    seconds: int | None = None
    nanos: int | None = None


class _TimestampTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        Timestamp, google.protobuf.timestamp_pb2.Timestamp
    ]
):
    transfer_type: type[google.protobuf.timestamp_pb2.Timestamp] | None = (
        google.protobuf.timestamp_pb2.Timestamp
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: google.protobuf.timestamp_pb2.Timestamp,
        type_hint: type[Timestamp],
    ) -> Timestamp:
        return Timestamp(
            seconds=value.seconds if value.seconds != 0 else None,
            nanos=value.nanos if value.nanos != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: Timestamp,
    ) -> google.protobuf.timestamp_pb2.Timestamp:
        message = google.protobuf.timestamp_pb2.Timestamp()
        if value.seconds is not None:
            message.seconds = value.seconds
        if value.nanos is not None:
            message.nanos = value.nanos
        return message


temporalio.converter.transfer_type_convertible(_TimestampTransferTypeConverter)(
    Timestamp
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class LocalActivityEpoch:
    """A Local Activity attempt. Its Workflow Task orders it first."""

    task: WorkflowTaskEpoch | None = None
    attempt: int | None = None


class _LocalActivityEpochTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        LocalActivityEpoch, temporalio.api.stream.v1.message_pb2.LocalActivityEpoch
    ]
):
    transfer_type: (
        type[temporalio.api.stream.v1.message_pb2.LocalActivityEpoch] | None
    ) = temporalio.api.stream.v1.message_pb2.LocalActivityEpoch

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.LocalActivityEpoch,
        type_hint: type[LocalActivityEpoch],
    ) -> LocalActivityEpoch:
        return LocalActivityEpoch(
            task=_WorkflowTaskEpochTransferTypeConverter().from_transfer_type(
                value.task, WorkflowTaskEpoch
            )
            if value.HasField("task")
            else None,
            attempt=value.attempt if value.attempt != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: LocalActivityEpoch,
    ) -> temporalio.api.stream.v1.message_pb2.LocalActivityEpoch:
        message = temporalio.api.stream.v1.message_pb2.LocalActivityEpoch()
        if value.task is not None:
            message.task.CopyFrom(
                _WorkflowTaskEpochTransferTypeConverter().to_transfer_type(value.task)
            )
        if value.attempt is not None:
            message.attempt = value.attempt
        return message


temporalio.converter.transfer_type_convertible(
    _LocalActivityEpochTransferTypeConverter
)(LocalActivityEpoch)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class ActivityEpoch:
    """An Activity or Standalone Activity attempt."""

    attempt_scheduled_time: Timestamp | None = None
    attempt: int | None = None


class _ActivityEpochTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        ActivityEpoch, temporalio.api.stream.v1.message_pb2.ActivityEpoch
    ]
):
    transfer_type: type[temporalio.api.stream.v1.message_pb2.ActivityEpoch] | None = (
        temporalio.api.stream.v1.message_pb2.ActivityEpoch
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.ActivityEpoch,
        type_hint: type[ActivityEpoch],
    ) -> ActivityEpoch:
        return ActivityEpoch(
            attempt_scheduled_time=_TimestampTransferTypeConverter().from_transfer_type(
                value.attempt_scheduled_time, Timestamp
            )
            if value.HasField("attempt_scheduled_time")
            else None,
            attempt=value.attempt if value.attempt != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: ActivityEpoch,
    ) -> temporalio.api.stream.v1.message_pb2.ActivityEpoch:
        message = temporalio.api.stream.v1.message_pb2.ActivityEpoch()
        if value.attempt_scheduled_time is not None:
            message.attempt_scheduled_time.CopyFrom(
                _TimestampTransferTypeConverter().to_transfer_type(
                    value.attempt_scheduled_time
                )
            )
        if value.attempt is not None:
            message.attempt = value.attempt
        return message


temporalio.converter.transfer_type_convertible(_ActivityEpochTransferTypeConverter)(
    ActivityEpoch
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class ClientEpoch:
    """A client attempt, which the client picks."""

    attempt: int | None = None


class _ClientEpochTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        ClientEpoch, temporalio.api.stream.v1.message_pb2.ClientEpoch
    ]
):
    transfer_type: type[temporalio.api.stream.v1.message_pb2.ClientEpoch] | None = (
        temporalio.api.stream.v1.message_pb2.ClientEpoch
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.ClientEpoch,
        type_hint: type[ClientEpoch],
    ) -> ClientEpoch:
        return ClientEpoch(
            attempt=value.attempt if value.attempt != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: ClientEpoch,
    ) -> temporalio.api.stream.v1.message_pb2.ClientEpoch:
        message = temporalio.api.stream.v1.message_pb2.ClientEpoch()
        if value.attempt is not None:
            message.attempt = value.attempt
        return message


temporalio.converter.transfer_type_convertible(_ClientEpochTransferTypeConverter)(
    ClientEpoch
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class WriterResult:
    writer_id: int | None = None


class _WriterResultTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        WriterResult, temporalio.bridge.proto.streams.streams_pb2.OpenWriterResponse
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.OpenWriterResponse] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.OpenWriterResponse

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.OpenWriterResponse,
        type_hint: type[WriterResult],
    ) -> WriterResult:
        return WriterResult(
            writer_id=value.writer_id if value.writer_id != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: WriterResult,
    ) -> temporalio.bridge.proto.streams.streams_pb2.OpenWriterResponse:
        message = temporalio.bridge.proto.streams.streams_pb2.OpenWriterResponse()
        if value.writer_id is not None:
            message.writer_id = value.writer_id
        return message


temporalio.converter.transfer_type_convertible(_WriterResultTransferTypeConverter)(
    WriterResult
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class WriteRecordsRequest:
    records: list[AppendRecord] | None = dataclasses.field(default_factory=list)
    writer_id: int | None = None


class _WriteRecordsRequestTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        WriteRecordsRequest,
        temporalio.bridge.proto.streams.streams_pb2.WriteRecordsRequest,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.WriteRecordsRequest] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.WriteRecordsRequest

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.WriteRecordsRequest,
        type_hint: type[WriteRecordsRequest],
    ) -> WriteRecordsRequest:
        return WriteRecordsRequest(
            records=[
                _AppendRecordTransferTypeConverter().from_transfer_type(
                    item, AppendRecord
                )
                for item in value.records
            ],
            writer_id=value.writer_id if value.writer_id != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: WriteRecordsRequest,
    ) -> temporalio.bridge.proto.streams.streams_pb2.WriteRecordsRequest:
        message = temporalio.bridge.proto.streams.streams_pb2.WriteRecordsRequest()
        if value.records:
            for element in value.records:
                item = message.records.add()
                item.CopyFrom(
                    _AppendRecordTransferTypeConverter().to_transfer_type(element)
                )
        if value.writer_id is not None:
            message.writer_id = value.writer_id
        return message


temporalio.converter.transfer_type_convertible(
    _WriteRecordsRequestTransferTypeConverter
)(WriteRecordsRequest)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class AppendRecord:
    """One Record to append."""

    body: typing.Any


class _AppendRecordTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        AppendRecord, temporalio.api.stream.v1.message_pb2.AppendRecord
    ]
):
    transfer_type: type[temporalio.api.stream.v1.message_pb2.AppendRecord] | None = (
        temporalio.api.stream.v1.message_pb2.AppendRecord
    )

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.api.stream.v1.message_pb2.AppendRecord,
        type_hint: type[AppendRecord],
    ) -> AppendRecord:
        if not value.HasField("body"):
            raise ValueError("missing required field AppendRecord.body")
        body = _support.payload_from_proto(value.body)
        return AppendRecord(
            body=body,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: AppendRecord,
    ) -> temporalio.api.stream.v1.message_pb2.AppendRecord:
        message = temporalio.api.stream.v1.message_pb2.AppendRecord()
        message.body.CopyFrom(_support.payload_to_proto(value.body))
        return message


temporalio.converter.transfer_type_convertible(_AppendRecordTransferTypeConverter)(
    AppendRecord
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class WriteRecordsResponse:
    first_offset: int | None = None
    last_offset: int | None = None


class _WriteRecordsResponseTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        WriteRecordsResponse,
        temporalio.bridge.proto.streams.streams_pb2.WriteRecordsResponse,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.WriteRecordsResponse] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.WriteRecordsResponse

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.WriteRecordsResponse,
        type_hint: type[WriteRecordsResponse],
    ) -> WriteRecordsResponse:
        return WriteRecordsResponse(
            first_offset=value.first_offset if value.first_offset != 0 else None,
            last_offset=value.last_offset if value.last_offset != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: WriteRecordsResponse,
    ) -> temporalio.bridge.proto.streams.streams_pb2.WriteRecordsResponse:
        message = temporalio.bridge.proto.streams.streams_pb2.WriteRecordsResponse()
        if value.first_offset is not None:
            message.first_offset = value.first_offset
        if value.last_offset is not None:
            message.last_offset = value.last_offset
        return message


temporalio.converter.transfer_type_convertible(
    _WriteRecordsResponseTransferTypeConverter
)(WriteRecordsResponse)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class CloseWriterRequest:
    writer_id: int | None = None


class _CloseWriterRequestTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        CloseWriterRequest,
        temporalio.bridge.proto.streams.streams_pb2.CloseWriterRequest,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.CloseWriterRequest] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.CloseWriterRequest

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.CloseWriterRequest,
        type_hint: type[CloseWriterRequest],
    ) -> CloseWriterRequest:
        return CloseWriterRequest(
            writer_id=value.writer_id if value.writer_id != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: CloseWriterRequest,
    ) -> temporalio.bridge.proto.streams.streams_pb2.CloseWriterRequest:
        message = temporalio.bridge.proto.streams.streams_pb2.CloseWriterRequest()
        if value.writer_id is not None:
            message.writer_id = value.writer_id
        return message


temporalio.converter.transfer_type_convertible(
    _CloseWriterRequestTransferTypeConverter
)(CloseWriterRequest)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class CloseWriterResponse:
    last_offset: int | None = None


class _CloseWriterResponseTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        CloseWriterResponse,
        temporalio.bridge.proto.streams.streams_pb2.CloseWriterResponse,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.CloseWriterResponse] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.CloseWriterResponse

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.CloseWriterResponse,
        type_hint: type[CloseWriterResponse],
    ) -> CloseWriterResponse:
        return CloseWriterResponse(
            last_offset=value.last_offset if value.last_offset != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: CloseWriterResponse,
    ) -> temporalio.bridge.proto.streams.streams_pb2.CloseWriterResponse:
        message = temporalio.bridge.proto.streams.streams_pb2.CloseWriterResponse()
        if value.last_offset is not None:
            message.last_offset = value.last_offset
        return message


temporalio.converter.transfer_type_convertible(
    _CloseWriterResponseTransferTypeConverter
)(CloseWriterResponse)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class FreeWriterRequest:
    writer_id: int | None = None


class _FreeWriterRequestTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        FreeWriterRequest,
        temporalio.bridge.proto.streams.streams_pb2.FreeWriterRequest,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.FreeWriterRequest] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.FreeWriterRequest

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.FreeWriterRequest,
        type_hint: type[FreeWriterRequest],
    ) -> FreeWriterRequest:
        return FreeWriterRequest(
            writer_id=value.writer_id if value.writer_id != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: FreeWriterRequest,
    ) -> temporalio.bridge.proto.streams.streams_pb2.FreeWriterRequest:
        message = temporalio.bridge.proto.streams.streams_pb2.FreeWriterRequest()
        if value.writer_id is not None:
            message.writer_id = value.writer_id
        return message


temporalio.converter.transfer_type_convertible(_FreeWriterRequestTransferTypeConverter)(
    FreeWriterRequest
)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class FreeWriterResponse:
    pass


class _FreeWriterResponseTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        FreeWriterResponse,
        temporalio.bridge.proto.streams.streams_pb2.FreeWriterResponse,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.FreeWriterResponse] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.FreeWriterResponse

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.FreeWriterResponse,
        type_hint: type[FreeWriterResponse],
    ) -> FreeWriterResponse:
        return FreeWriterResponse()

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: FreeWriterResponse,
    ) -> temporalio.bridge.proto.streams.streams_pb2.FreeWriterResponse:
        message = temporalio.bridge.proto.streams.streams_pb2.FreeWriterResponse()
        return message


temporalio.converter.transfer_type_convertible(
    _FreeWriterResponseTransferTypeConverter
)(FreeWriterResponse)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class CloseWritersRequest:
    pass


class _CloseWritersRequestTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        CloseWritersRequest,
        temporalio.bridge.proto.streams.streams_pb2.CloseWritersRequest,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.CloseWritersRequest] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.CloseWritersRequest

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.CloseWritersRequest,
        type_hint: type[CloseWritersRequest],
    ) -> CloseWritersRequest:
        return CloseWritersRequest()

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: CloseWritersRequest,
    ) -> temporalio.bridge.proto.streams.streams_pb2.CloseWritersRequest:
        message = temporalio.bridge.proto.streams.streams_pb2.CloseWritersRequest()
        return message


temporalio.converter.transfer_type_convertible(
    _CloseWritersRequestTransferTypeConverter
)(CloseWritersRequest)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class CloseWritersResponse:
    failed: int | None = None


class _CloseWritersResponseTransferTypeConverter(
    temporalio.converter.TransferTypeConverter[
        CloseWritersResponse,
        temporalio.bridge.proto.streams.streams_pb2.CloseWritersResponse,
    ]
):
    transfer_type: (
        type[temporalio.bridge.proto.streams.streams_pb2.CloseWritersResponse] | None
    ) = temporalio.bridge.proto.streams.streams_pb2.CloseWritersResponse

    @typing_extensions.override
    def from_transfer_type(
        self,
        value: temporalio.bridge.proto.streams.streams_pb2.CloseWritersResponse,
        type_hint: type[CloseWritersResponse],
    ) -> CloseWritersResponse:
        return CloseWritersResponse(
            failed=value.failed if value.failed != 0 else None,
        )

    @typing_extensions.override
    def to_transfer_type(
        self,
        value: CloseWritersResponse,
    ) -> temporalio.bridge.proto.streams.streams_pb2.CloseWritersResponse:
        message = temporalio.bridge.proto.streams.streams_pb2.CloseWritersResponse()
        if value.failed is not None:
            message.failed = value.failed
        return message


temporalio.converter.transfer_type_convertible(
    _CloseWritersResponseTransferTypeConverter
)(CloseWritersResponse)  # pyright: ignore[reportUnusedCallResult]


@dataclasses.dataclass(slots=True)
class ProducerEpochKindWorkflow:
    value: WorkflowTaskEpoch


@dataclasses.dataclass(slots=True)
class ProducerEpochKindLocalActivity:
    value: LocalActivityEpoch


@dataclasses.dataclass(slots=True)
class ProducerEpochKindActivity:
    value: ActivityEpoch


@dataclasses.dataclass(slots=True)
class ProducerEpochKindStandaloneActivity:
    value: ActivityEpoch


@dataclasses.dataclass(slots=True)
class ProducerEpochKindClient:
    value: ClientEpoch


ProducerEpochKind = (
    ProducerEpochKindWorkflow
    | ProducerEpochKindLocalActivity
    | ProducerEpochKindActivity
    | ProducerEpochKindStandaloneActivity
    | ProducerEpochKindClient
)
"""The epoch of each kind of Producer."""
