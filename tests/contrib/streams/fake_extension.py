"""An in-memory stand-in for the temporalio-streams extension, for facade tests."""

from __future__ import annotations

import asyncio

import temporalio.api.streamservice.v1 as stream_pb
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.service import RPCStatusCode
from temporalio.worker import ExtensionError, WorkerExtension


def _error(code: RPCStatusCode, reason: str) -> ExtensionError:
    details = ext_pb.StreamErrorDetails(reason=reason).SerializeToString()
    return ExtensionError(reason, code, details)


class _Stream:
    def __init__(self) -> None:
        self.records: list[stream_pb.StreamRecord] = []
        self.closed = False
        self.changed = asyncio.Event()


class FakeExtension(WorkerExtension):
    def __init__(self) -> None:  # pyright: ignore[reportMissingSuperCall]
        self.streams: dict[str, _Stream] = {}
        self.writers: dict[int, tuple[str, stream_pb.Producer]] = {}
        self.namespaces: set[str] = set()
        self.next_offset = 1

    @property
    def name(self) -> str:
        return "fake-streams"

    def _append(self, stream_id: str, record: stream_pb.StreamRecord) -> int:
        stream = self.streams.get(stream_id)
        if stream is None:
            raise _error(RPCStatusCode.NOT_FOUND, "stream_not_found")
        if stream.closed:
            raise _error(RPCStatusCode.FAILED_PRECONDITION, "stream_closed")
        record.offset = self.next_offset
        self.next_offset += 1
        stream.records.append(record)
        stream.changed.set()
        stream.changed = asyncio.Event()
        return record.offset

    def call(self, method: str, input: bytes) -> bytes:
        if method == "Publish":
            request = ext_pb.PublishRequest.FromString(input)
            stream_id, producer = self.writers[request.writer_id]
            for record in request.records:
                self._append(
                    stream_id,
                    stream_pb.StreamRecord(
                        kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA,
                        topic=record.topic,
                        body=record.body,
                        producer=producer,
                    ),
                )
            return b""
        raise _error(RPCStatusCode.UNIMPLEMENTED, method)

    async def call_async(self, method: str, input: bytes) -> bytes:
        if method == "CreateStream":
            create = stream_pb.CreateStreamRequest.FromString(input)
            self.namespaces.add(create.namespace)
            self.streams.setdefault(create.stream_id, _Stream())
            return stream_pb.CreateStreamResponse(
                stream=stream_pb.StreamRef(id=create.stream_id), created=True
            ).SerializeToString()
        if method == "OpenWriter":
            open_writer = ext_pb.OpenWriterRequest.FromString(input)
            writer_id = len(self.writers) + 1
            self.writers[writer_id] = (open_writer.stream.id, open_writer.producer)
            return ext_pb.OpenWriterResponse(writer_id=writer_id).SerializeToString()
        if method in ("FlushWriter", "CloseWriter"):
            return ext_pb.FlushWriterResponse(
                last_offset=self.next_offset - 1
            ).SerializeToString()
        if method == "CloseStream":
            close = stream_pb.CloseStreamRequest.FromString(input)
            self._append(
                close.stream.id,
                stream_pb.StreamRecord(
                    kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_CLOSE,
                    producer=close.producer,
                    close_result=close.result if close.HasField("result") else None,
                ),
            )
            self.streams[close.stream.id].closed = True
            return b""
        if method == "DescribeStream":
            describe = stream_pb.DescribeStreamRequest.FromString(input)
            records = self.streams[describe.stream.id].records
            return stream_pb.DescribeStreamResponse(
                last_offset=records[-1].offset if records else 0
            ).SerializeToString()
        if method == "ReadRecords":
            return (
                await self._read(stream_pb.ReadRecordsRequest.FromString(input))
            ).SerializeToString()
        raise _error(RPCStatusCode.UNIMPLEMENTED, method)

    async def _read(
        self, request: stream_pb.ReadRecordsRequest
    ) -> stream_pb.ReadRecordsResponse:
        loop = asyncio.get_running_loop()
        create_until = loop.time() + request.create_wait.ToTimedelta().total_seconds()
        while request.stream.id not in self.streams:
            if loop.time() >= create_until:
                raise _error(RPCStatusCode.NOT_FOUND, "stream_not_found")
            await asyncio.sleep(0.01)
        stream = self.streams[request.stream.id]
        offset = request.from_offset or 1
        wait = request.wait.ToTimedelta().total_seconds()
        idle = request.idle_timeout.ToTimedelta().total_seconds()
        until = loop.time() + (min(wait, idle) if idle else wait)
        while True:
            found = [
                record
                for record in stream.records
                if record.offset >= offset
                and (
                    not request.topics
                    or record.kind != stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA
                    or record.topic in request.topics
                )
            ]
            if found:
                done = any(
                    r.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_CLOSE
                    for r in found
                )
                return stream_pb.ReadRecordsResponse(
                    records=found, next_offset=found[-1].offset + 1, done=done
                )
            left = until - loop.time()
            if left <= 0:
                if idle and wait >= idle:
                    idle_record = stream_pb.StreamRecord(
                        kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_IDLE,
                        offset=offset,
                    )
                    return stream_pb.ReadRecordsResponse(
                        records=[idle_record], next_offset=offset, done=True
                    )
                return stream_pb.ReadRecordsResponse(next_offset=offset)
            try:
                await asyncio.wait_for(stream.changed.wait(), left)
            except asyncio.TimeoutError:
                pass
