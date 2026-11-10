# Written by hand in the shape of nexgen's client output, since the writer has no WIT. The shared
# types (StreamRef, StreamWriter, AppendRecord and the Producer models) come from the generated
# system models, so the writer always matches the generated client. Only the writer-specific
# request and response types live here.

from __future__ import annotations

import dataclasses
import datetime

import typing_extensions

import temporalio.bridge.proto.streams.streams_pb2
import temporalio.converter
import temporalio.nexus.system._support as _support
from temporalio.streams.system.models import (
    AppendRecord,
    Producer,
    StreamRef,
    StreamWriter,
    _AppendRecordTransferTypeConverter,
    _ProducerTransferTypeConverter,
    _StreamRefTransferTypeConverter,
    _StreamWriterTransferTypeConverter,
)

__all__ = [
    "AppendRecord",
    "CloseWriterRequest",
    "CloseWriterResponse",
    "CloseWritersRequest",
    "CloseWritersResponse",
    "FreeWriterRequest",
    "FreeWriterResponse",
    "OpenWriterRequest",
    "Producer",
    "StreamRef",
    "StreamWriter",
    "WriteRecordsRequest",
    "WriteRecordsResponse",
    "WriterResult",
    "_ProducerTransferTypeConverter",
]


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
class WriteRecordsResponse:
    first_offset: bytes | None = None
    last_offset: bytes | None = None


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
            first_offset=value.first_offset if value.first_offset else None,
            last_offset=value.last_offset if value.last_offset else None,
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
    last_offset: bytes | None = None


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
            last_offset=value.last_offset if value.last_offset else None,
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
