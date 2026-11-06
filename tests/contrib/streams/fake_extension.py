"""An in-memory fake of the temporalio-streams runtime, for the facade tests."""

from __future__ import annotations

import asyncio

import temporalio.api.stream.v1 as stream_v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.bridge.proto.streams as runtime_pb
from temporalio.api.enums.v1 import StreamErrorReason, StreamIdConflictPolicy
from temporalio.bridge.streams import StreamCallError
from temporalio.client import Client
from temporalio.service import RPCStatusCode
from temporalio.streams import StreamBackend
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


class FakeExtension:
    def __init__(self) -> None:
        self.streams: dict[str, _Stream] = {}
        self.writers: dict[int, tuple[str, stream_v1.Producer]] = {}
        self.freed: list[int] = []
        self.namespaces: set[str] = set()
        self.creates: list[stream_pb.CreateStreamRequest] = []
        self.next_offset = 1

    def _append(self, stream_id: str, record: stream_v1.StreamRecord) -> int:
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
        record.offset = self.next_offset
        self.next_offset += 1
        stream.records.append(record)
        stream.wake()
        return record.offset

    def free_writer(self, writer_id: int) -> None:
        self.freed.append(writer_id)

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
        if method == "OpenWriter":
            open_writer = runtime_pb.OpenWriterRequest.FromString(input)
            writer_id = len(self.writers) + 1
            self.writers[writer_id] = (open_writer.ref.id, open_writer.writer.producer)
            return runtime_pb.OpenWriterResponse(
                writer_id=writer_id
            ).SerializeToString()
        if method == "WriteRecords":
            write = runtime_pb.WriteRecordsRequest.FromString(input)
            stream_id, producer = self.writers[write.writer_id]
            offsets = [
                self._append(
                    stream_id,
                    stream_v1.StreamRecord(
                        data=stream_v1.StreamData(body=record.body, producer=producer)
                    ),
                )
                for record in write.records
            ]
            return runtime_pb.WriteRecordsResponse(
                first_offset=offsets[0] if offsets else 0,
                last_offset=offsets[-1] if offsets else 0,
            ).SerializeToString()
        if method == "CloseWriter":
            return runtime_pb.CloseWriterResponse(
                last_offset=self.next_offset - 1
            ).SerializeToString()
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
                last_offset=records[-1].offset if records else 0
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
        create_until = loop.time() + request.create_wait.ToTimedelta().total_seconds()
        while request.ref.id not in self.streams:
            if loop.time() >= create_until:
                raise _error(
                    RPCStatusCode.NOT_FOUND,
                    StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND,
                )
            await asyncio.sleep(0.01)
        stream = self.streams[request.ref.id]
        offset = request.from_offset or 1
        wait = request.wait.ToTimedelta().total_seconds()
        until = loop.time() + wait
        while True:
            found = [record for record in stream.records if record.offset >= offset]
            if found or stream.closed:
                return stream_pb.ReadRecordsResponse(
                    records=found,
                    next_offset=found[-1].offset + 1 if found else offset,
                    done=stream.closed,
                )
            left = until - loop.time()
            if left <= 0:
                return stream_pb.ReadRecordsResponse(next_offset=offset)
            try:
                await asyncio.wait_for(stream.changed.wait(), left)
            except asyncio.TimeoutError:
                pass


class _FakeBackend(StreamBackend):
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
