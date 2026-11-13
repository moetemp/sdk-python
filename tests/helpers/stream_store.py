from __future__ import annotations

from dataclasses import dataclass, field

import temporalio.api.enums.v1
import temporalio.api.stream.v1
import temporalio.api.streamservice.v1
from temporalio.client import StreamStore


@dataclass
class _MemoryStream:
    records: list[temporalio.api.stream.v1.StreamRecord] = field(default_factory=list)
    closed: bool = False


class MemoryStreamStore(StreamStore):
    def __init__(self) -> None:
        self.streams: dict[str, _MemoryStream] = {}

    @property
    def name(self) -> str:
        return "memory"

    async def call(self, operation: str, request: bytes) -> bytes:
        service = temporalio.api.streamservice.v1
        enums = temporalio.api.enums.v1
        if operation == "CreateStream":
            create = service.CreateStreamRequest.FromString(request)
            created = create.stream_ref.id not in self.streams
            use_existing = (
                enums.StreamIdConflictPolicy.STREAM_ID_CONFLICT_POLICY_USE_EXISTING
            )
            if not created and create.id_conflict_policy != use_existing:
                raise RuntimeError(f"Stream {create.stream_ref.id} exists")
            self.streams.setdefault(create.stream_ref.id, _MemoryStream())
            return service.CreateStreamResponse(
                ref=create.stream_ref, created=created
            ).SerializeToString()
        if operation == "AppendRecords":
            append = service.AppendRecordsRequest.FromString(request)
            stream = self.streams[append.ref.id]
            if stream.closed:
                raise RuntimeError(f"Stream {append.ref.id} is closed")
            for record in append.records:
                offset = (len(stream.records) + 1).to_bytes(8, "big")
                stream.records.append(
                    temporalio.api.stream.v1.StreamRecord(
                        body=record.body, producer=append.producer, offset=offset
                    )
                )
            appended = stream.records[len(stream.records) - len(append.records) :]
            return service.AppendRecordsResponse(
                first_offset=appended[0].offset if appended else b"",
                last_offset=appended[-1].offset if appended else b"",
            ).SerializeToString()
        if operation == "ReadRecords":
            read = service.ReadRecordsRequest.FromString(request)
            stream = self.streams[read.ref.id]
            start = int.from_bytes(read.from_offset, "big") if read.from_offset else 0
            page = stream.records[start : start + (read.max_records or 1000)]
            return service.ReadRecordsResponse(
                records=page,
                last_offset=page[-1].offset if page else read.from_offset,
                done=stream.closed and start + len(page) == len(stream.records),
            ).SerializeToString()
        if operation == "CloseStream":
            close = service.CloseStreamRequest.FromString(request)
            self.streams[close.ref.id].closed = True
            return service.CloseStreamResponse().SerializeToString()
        if operation == "DescribeStream":
            describe = service.DescribeStreamRequest.FromString(request)
            stream = self.streams[describe.ref.id]
            return service.DescribeStreamResponse(
                status=enums.StreamStatus.STREAM_STATUS_CLOSED
                if stream.closed
                else enums.StreamStatus.STREAM_STATUS_OPEN,
                last_offset=stream.records[-1].offset if stream.records else b"",
            ).SerializeToString()
        raise ValueError(f"Unknown Stream operation {operation}")
