# Written by hand in the shape of nexgen's client output, since the writer has no WIT. The shared
# types (StreamRef, AppendRecord and the Producer models) come from the generated system models,
# so the writer always matches the generated client. Only the writer-specific request and
# response types live here. The runtime's client sets the namespace of each request, as for the
# generated requests, so no model carries it.

from __future__ import annotations

import dataclasses
import datetime
import typing

import typing_extensions

import temporalio.bridge.proto.streams.streams_pb2
import temporalio.converter
import temporalio.nexus.system._support as _support
from temporalio.streams.system.models import (
    AppendRecord,
    Producer,
    StreamRef,
    _AppendRecordTransferTypeConverter,
    _ProducerTransferTypeConverter,
    _StreamRefTransferTypeConverter,
)

__all__ = [
    "AppendRecord",
    "CloseWriterResponse",
    "OpenWriterRequest",
    "Producer",
    "StreamRef",
    "WriteRecordsRequest",
    "WriteRecordsResponse",
    "_ProducerTransferTypeConverter",
]


@dataclasses.dataclass(slots=True)
class OpenWriterRequest:
    """Open a writer for one Producer of one Stream."""

    ref: StreamRef
    producer: Producer
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
        ref = _StreamRefTransferTypeConverter[typing.Any]().from_transfer_type(
            value.ref, StreamRef
        )
        if not value.HasField("producer"):
            raise ValueError("missing required field OpenWriterRequest.producer")
        return OpenWriterRequest(
            ref=ref,
            producer=_ProducerTransferTypeConverter().from_transfer_type(
                value.producer, Producer
            ),
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
            _StreamRefTransferTypeConverter[typing.Any]().to_transfer_type(value.ref)
        )
        message.producer.CopyFrom(
            _ProducerTransferTypeConverter().to_transfer_type(value.producer)
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
class WriteRecordsRequest:
    records: list[AppendRecord] | None = dataclasses.field(default_factory=list)


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
