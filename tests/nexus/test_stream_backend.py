from __future__ import annotations

import ast
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from unittest import mock

import pytest

import temporalio
import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.stream.v1
import temporalio.api.streamservice.v1
import temporalio.api.workflowservice.v1
import temporalio.nexus.system
import temporalio.nexus.system._support
from temporalio.client import Client, StreamBackend
from temporalio.converter import DataConverter
from temporalio.nexus.system.stream_service import (
    ClientProducer,
    DescribeStreamRequest,
    Producer,
    ProducerKindClient,
    StreamRef,
    StreamService,
)
from temporalio.nexus.system.stream_service.client import (
    Stream,
    StreamServiceClientExtension,
)
from tests.test_converter import SimpleCodec


@dataclass
class _MemoryStream:
    records: list[temporalio.api.stream.v1.StreamRecord] = field(default_factory=list)
    closed: bool = False


class MemoryStreamBackend(StreamBackend):
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


def _offset(n: int) -> bytes:
    return n.to_bytes(8, "big")


async def _create_stream(client: Client, backend: MemoryStreamBackend) -> Stream[str]:
    config = client.config()
    config["stream_backends"] = [backend]
    return await Client(**config).create_stream(
        id=f"stream-{uuid.uuid4()}", body_type=str
    )


async def test_client_stream_uses_backend(client: Client):
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend()]
    stream_client = Client(**config)

    stream = await stream_client.create_stream(
        id=f"stream-{uuid.uuid4()}", body_type=str
    )
    assert stream.created
    appended = await stream.append("a", "b")
    assert (appended.first_offset, appended.last_offset) == (_offset(1), _offset(2))
    records = [record async for record in stream.iter_read_items()]
    assert [(record.body, record.offset) for record in records] == [
        ("a", _offset(1)),
        ("b", _offset(2)),
    ]

    other = stream_client.get_stream(stream.ref, body_type=str)
    assert [record.body async for record in other.iter_read_items()] == ["a", "b"]

    await stream.close()
    page = await stream.read(from_offset=records[-1].offset)
    assert page.done
    assert page.records == []


async def test_client_stream_backend_uses_payload_codec(client: Client):
    backend = MemoryStreamBackend()
    config = client.config()
    config["data_converter"] = DataConverter(payload_codec=SimpleCodec())
    config["stream_backends"] = [backend]
    stream_client = Client(**config)

    stream = await stream_client.create_stream(
        id=f"stream-{uuid.uuid4()}", body_type=str
    )
    await stream.append("a", "b")
    stored = [record.body for record in backend.streams[stream.ref.id].records]
    assert [body.metadata["simple-codec"] for body in stored] == [b"true", b"true"]
    assert [
        temporalio.api.common.v1.Payloads.FromString(body.data).payloads[0].data
        for body in stored
    ] == [b'"a"', b'"b"']
    assert [record.body async for record in stream.iter_read_items()] == ["a", "b"]


async def test_client_append_has_client_producer(client: Client):
    backend = MemoryStreamBackend()
    stream = await _create_stream(client, backend)
    await stream.append("a")
    [record] = backend.streams[stream.ref.id].records
    assert record.producer.id.startswith("client:")
    assert record.producer.WhichOneof("kind") == "client"


async def test_explicit_producer_is_sent(client: Client):
    backend = MemoryStreamBackend()
    stream = await _create_stream(client, backend)
    with mock.patch.object(
        temporalio.nexus.system._support, "default_producer"
    ) as default_producer:
        await stream.append(
            "a", producer=Producer(id="mine", kind=ProducerKindClient(ClientProducer()))
        )
    default_producer.assert_not_called()
    [record] = backend.streams[stream.ref.id].records
    assert record.producer.id == "mine"
    assert record.producer.WhichOneof("kind") == "client"


def test_client_stream_methods_are_generated():
    assert "create_stream" not in vars(Client)
    assert "get_stream" not in vars(Client)
    assert Client.create_stream is StreamServiceClientExtension.create_stream
    assert Client.get_stream is StreamServiceClientExtension.get_stream


def test_no_hand_written_stream_classes():
    root = Path(temporalio.__file__).parent
    generated = root / "nexus" / "system" / "stream_service"
    names = {"Stream", "WorkflowStream", "StreamCalls", "StreamClient"}
    found: list[tuple[str, str]] = []
    for source in sorted(root.rglob("*.py")):
        if generated in source.parents:
            continue
        for node in ast.walk(ast.parse(source.read_text(encoding="utf8"))):
            if isinstance(node, ast.ClassDef) and node.name in names:
                found.append((source.relative_to(root).as_posix(), node.name))
    # The Workflow Streams contrib library is older than the generated Stream API and
    # doesn't use it.
    found.remove(("contrib/workflow_streams/_stream.py", "WorkflowStream"))
    assert found == []


async def test_client_without_stream_backend_starts_nexus_operation(client: Client):
    requests: list[
        temporalio.api.workflowservice.v1.StartNexusOperationExecutionRequest
    ] = []

    async def start_nexus_operation_execution(
        req: temporalio.api.workflowservice.v1.StartNexusOperationExecutionRequest,
        **_: object,
    ) -> temporalio.api.workflowservice.v1.StartNexusOperationExecutionResponse:
        requests.append(req)
        return temporalio.api.workflowservice.v1.StartNexusOperationExecutionResponse(
            run_id="run"
        )

    nexus_client = client.create_nexus_client(
        StreamService, temporalio.nexus.system.TEMPORAL_SYSTEM_ENDPOINT
    )
    with mock.patch.object(
        client.workflow_service,
        "start_nexus_operation_execution",
        start_nexus_operation_execution,
    ):
        handle = await nexus_client.start_operation(
            StreamService.describe_stream,
            DescribeStreamRequest(ref=StreamRef(id="stream")),
            id="operation",
        )
    assert handle.run_id == "run"
    [request] = requests
    assert request.endpoint == temporalio.nexus.system.TEMPORAL_SYSTEM_ENDPOINT
    assert request.service == "temporal.api.streamservice.v1.StreamService"
    assert request.operation == "DescribeStream"


async def test_client_rejects_two_stream_backends(client: Client):
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend(), MemoryStreamBackend()]
    with pytest.raises(ValueError, match="selector"):
        Client(**config)
