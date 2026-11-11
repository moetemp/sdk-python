"""An in-memory fake of the temporalio-streams runtime, for the facade tests."""

from __future__ import annotations

import asyncio
import weakref

import temporalio.api.stream.v1 as stream_v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.bridge.proto.streams as runtime_pb
from temporalio.api.enums.v1 import (
    StreamErrorReason,
    StreamIdConflictPolicy,
    StreamReadStart,
)
from temporalio.bridge.streams import StreamCallError
from temporalio.client import Client
from temporalio.service import RPCStatusCode
from temporalio.streams import StreamBackend, StreamConfig
from temporalio.streams._backend import _ExtensionCalls


def _error(code: RPCStatusCode, reason: StreamErrorReason.ValueType) -> Exception:
    details = stream_v1.StreamErrorDetails(reason=reason).SerializeToString()
    return StreamCallError(int(code), StreamErrorReason.Name(reason), details)


class _Stream:
    def __init__(self, request_id: str) -> None:
        self.request_id = request_id
        self.records: list[stream_v1.StreamRecord] = []
        self.closed = False
        self.changed = asyncio.Event()

    def wake(self) -> None:
        self.changed.set()
        self.changed = asyncio.Event()


class _FakeWriter:
    def __init__(
        self, extension: FakeExtension, stream_id: str, producer: stream_v1.Producer
    ) -> None:
        self._extension = extension
        self._stream_id = stream_id
        self._producer = producer
        self._last_offset = b""
        self.closed = False

    async def write(self, input: bytes) -> bytes:
        if self.closed:
            raise _error(
                RPCStatusCode.FAILED_PRECONDITION,
                StreamErrorReason.STREAM_ERROR_REASON_CLOSED,
            )
        write = runtime_pb.WriteRecordsRequest.FromString(input)
        offsets = [
            self._extension._append(
                self._stream_id,
                stream_v1.StreamRecord(
                    data=stream_v1.StreamData(body=record.body, producer=self._producer)
                ),
            )
            for record in write.records
        ]
        if offsets:
            self._last_offset = offsets[-1]
        return runtime_pb.WriteRecordsResponse(
            first_offset=offsets[0] if offsets else b"",
            last_offset=offsets[-1] if offsets else b"",
        ).SerializeToString()

    async def close(self) -> bytes:
        self.closed = True
        return runtime_pb.CloseWriterResponse(
            last_offset=self._last_offset
        ).SerializeToString()


class FakeExtension:
    def __init__(self) -> None:
        self.streams: dict[str, _Stream] = {}
        # The writers that Python holds. A writer that Python frees leaves the set.
        self.writers: weakref.WeakSet[_FakeWriter] = weakref.WeakSet()
        self.namespaces: set[str] = set()
        self.writer_namespaces: list[str] = []
        self.creates: list[stream_pb.CreateStreamRequest] = []
        self.next_offset = 1

    def _append(self, stream_id: str, record: stream_v1.StreamRecord) -> bytes:
        stream = self.streams.get(stream_id)
        if stream is None:
            raise _error(
                RPCStatusCode.NOT_FOUND, StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
            )
        if stream.closed:
            raise _error(
                RPCStatusCode.FAILED_PRECONDITION,
                StreamErrorReason.STREAM_ERROR_REASON_CLOSED,
            )
        # A fixed-width big-endian offset compares as raw bytes in the Store's order.
        record.data.offset = self.next_offset.to_bytes(8, "big")
        self.next_offset += 1
        stream.records.append(record)
        stream.wake()
        return record.data.offset

    async def open_writer(self, input: bytes) -> _FakeWriter:
        open_writer = runtime_pb.OpenWriterRequest.FromString(input)
        self.writer_namespaces.append(open_writer.namespace)
        writer = _FakeWriter(self, open_writer.ref.id, open_writer.producer)
        self.writers.add(writer)
        return writer

    def _create(self, create: stream_pb.CreateStreamRequest) -> bytes:
        self.creates.append(create)
        self.namespaces.add(create.namespace)
        stream_id = create.stream_ref.id
        stream = self.streams.get(stream_id)
        created = stream is None
        if stream is None:
            stream = self.streams[stream_id] = _Stream(create.request_id)
        elif stream.closed:
            raise _error(
                RPCStatusCode.FAILED_PRECONDITION,
                StreamErrorReason.STREAM_ERROR_REASON_CLOSED,
            )
        elif create.request_id and create.request_id == stream.request_id:
            # A retry of the create that made the Stream.
            created = True
        elif (
            create.id_conflict_policy
            != StreamIdConflictPolicy.STREAM_ID_CONFLICT_POLICY_USE_EXISTING
        ):
            raise _error(
                RPCStatusCode.ALREADY_EXISTS,
                StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS,
            )
        return stream_pb.CreateStreamResponse(
            ref=stream_v1.StreamRef(id=stream_id), created=created
        ).SerializeToString()

    async def call(self, method: str, input: bytes) -> bytes:
        if method == "CreateStream":
            return self._create(stream_pb.CreateStreamRequest.FromString(input))
        if method == "CloseStream":
            # A close writes no Record. It wakes the waiting reads, which then end.
            close = stream_pb.CloseStreamRequest.FromString(input)
            stream = self.streams[close.ref.id]
            stream.closed = True
            stream.wake()
            return b""
        if method == "DescribeStream":
            describe = stream_pb.DescribeStreamRequest.FromString(input)
            records = self.streams[describe.ref.id].records
            return stream_pb.DescribeStreamResponse(
                last_offset=records[-1].data.offset if records else b""
            ).SerializeToString()
        if method == "ReadRecords":
            return (
                await self._read(stream_pb.ReadRecordsRequest.FromString(input))
            ).SerializeToString()
        raise _error(
            RPCStatusCode.UNIMPLEMENTED,
            StreamErrorReason.STREAM_ERROR_REASON_UNSUPPORTED,
        )

    async def _read(
        self, request: stream_pb.ReadRecordsRequest
    ) -> stream_pb.ReadRecordsResponse:
        loop = asyncio.get_running_loop()
        stream = self.streams.get(request.ref.id)
        if stream is None:
            raise _error(
                RPCStatusCode.NOT_FOUND, StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
            )
        # A resume reads after the offset, so from_offset is exclusive. A read from the end
        # starts after the newest Record. Empty reads from the start.
        offset = b""
        if request.WhichOneof("position") == "from_offset":
            offset = request.from_offset
        elif request.start == StreamReadStart.STREAM_READ_START_END and stream.records:
            offset = stream.records[-1].data.offset
        wait = request.wait.ToTimedelta().total_seconds()
        until = loop.time() + wait
        while True:
            found = [r for r in stream.records if r.data.offset > offset]
            if found or stream.closed:
                return stream_pb.ReadRecordsResponse(
                    records=found,
                    last_offset=found[-1].data.offset if found else offset,
                    done=stream.closed,
                )
            left = until - loop.time()
            if left <= 0:
                return stream_pb.ReadRecordsResponse(last_offset=offset)
            try:
                await asyncio.wait_for(stream.changed.wait(), left)
            except asyncio.TimeoutError:
                pass


class _FakeBackend(StreamBackend):
    def __init__(self, calls: _ExtensionCalls) -> None:
        super().__init__("fake-streams", backend_name="fake")
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
    config["stream_config"] = StreamConfig(backends=[backend])
    return Client(**config), extension
