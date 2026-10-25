"""An in-memory stand-in for the temporalio-streams runtime, for facade tests."""

from __future__ import annotations

import asyncio

import temporalio.api.streamservice.v1 as stream_pb
from temporalio.bridge.streams import StreamCallError
from temporalio.client import Client
from temporalio.common import StreamBackend
from temporalio.common._streams._backend import _ExtensionCalls
from temporalio.common._streams.proto import v1 as ext_pb
from temporalio.service import RPCStatusCode


def _error(code: RPCStatusCode, reason: str) -> Exception:
    details = ext_pb.StreamErrorDetails(reason=reason).SerializeToString()
    return StreamCallError(int(code), reason, details)


class _Stream:
    def __init__(self) -> None:
        self.records: list[stream_pb.StreamRecord] = []
        self.closed = False
        self.changed = asyncio.Event()


class FakeExtension:
    def __init__(self) -> None:
        self.streams: dict[str, _Stream] = {}
        self.writers: dict[int, tuple[str, stream_pb.Producer]] = {}
        self.freed: list[int] = []
        self.namespaces: set[str] = set()
        self.next_offset = 1

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

    def free_writer(self, writer_id: int) -> None:
        self.freed.append(writer_id)

    async def call(self, method: str, input: bytes) -> bytes:
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
        if method == "WriteRecords":
            write = ext_pb.WriteRecordsRequest.FromString(input)
            stream_id, producer = self.writers[write.writer_id]
            offsets = [
                self._append(
                    stream_id,
                    stream_pb.StreamRecord(
                        kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA,
                        topic=record.topic,
                        body=record.body,
                        producer=producer,
                    ),
                )
                for record in write.records
            ]
            return ext_pb.WriteRecordsResponse(
                first_offset=offsets[0] if offsets else 0,
                last_offset=offsets[-1] if offsets else 0,
            ).SerializeToString()
        if method == "CloseWriter":
            return ext_pb.CloseWriterResponse(
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
        until = loop.time() + wait
        while True:
            found = [
                record
                for record in stream.records
                if record.offset >= offset
                and (
                    (not request.topics and not request.topic_prefixes)
                    or record.kind != stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA
                    or record.topic in request.topics
                    or any(record.topic.startswith(p) for p in request.topic_prefixes)
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
                return stream_pb.ReadRecordsResponse(next_offset=offset)
            try:
                await asyncio.wait_for(stream.changed.wait(), left)
            except asyncio.TimeoutError:
                pass


class _FakeBackend(StreamBackend):
    # The fake keeps Redis's rules, so the Redis calls take it.
    _kind = "redis"

    def __init__(self, calls: _ExtensionCalls) -> None:
        super().__init__("fake-streams", backend_name="fake", default=True)
        self._calls = calls

    def _backend_for(
        self,
        client: Client,  # pyright: ignore[reportUnusedParameter]
    ) -> _ExtensionCalls:
        return self._calls


def fake_client(client: Client) -> tuple[Client, FakeExtension]:
    """A copy of ``client`` whose one Stream backend, ``fake``, is a fake runtime."""
    extension = FakeExtension()
    backend = _FakeBackend(_ExtensionCalls(extension))
    config = client.config()
    config["stream_backends"] = [backend]
    return Client(**config), extension
