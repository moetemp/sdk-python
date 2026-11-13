from __future__ import annotations

import ast
import dataclasses
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import google.protobuf.timestamp_pb2
import pytest

import temporalio
import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.stream.v1
import temporalio.api.workflowservice.v1
import temporalio.nexus.system
import temporalio.nexus.system._support
import temporalio.workflow
from temporalio.client import Client
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
from temporalio.testing import ActivityEnvironment
from tests.helpers.stream_store import MemoryStreamStore
from tests.test_converter import SimpleCodec


def _offset(n: int) -> bytes:
    return n.to_bytes(8, "big")


async def _create_stream(client: Client, store: MemoryStreamStore) -> Stream[str]:
    config = client.config()
    config["stream_stores"] = [store]
    return await Client(**config).create_stream(
        id=f"stream-{uuid.uuid4()}", body_type=str
    )


async def _append_in_activity(
    client: Client, info: temporalio.activity.Info
) -> temporalio.api.stream.v1.Producer:
    store = MemoryStreamStore()
    stream = await _create_stream(client, store)
    env = ActivityEnvironment()
    env.info = info
    await env.run(stream.append, "a")
    [record] = store.streams[stream.ref.id].records
    return record.producer


def _timestamp(value: datetime) -> google.protobuf.timestamp_pb2.Timestamp:
    proto = google.protobuf.timestamp_pb2.Timestamp()
    proto.FromDatetime(value)
    return proto


async def test_client_stream_uses_store(client: Client):
    config = client.config()
    config["stream_stores"] = [MemoryStreamStore()]
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


async def test_client_stream_store_uses_payload_codec(client: Client):
    store = MemoryStreamStore()
    config = client.config()
    config["data_converter"] = DataConverter(payload_codec=SimpleCodec())
    config["stream_stores"] = [store]
    stream_client = Client(**config)

    stream = await stream_client.create_stream(
        id=f"stream-{uuid.uuid4()}", body_type=str
    )
    await stream.append("a", "b")
    stored = [record.body for record in store.streams[stream.ref.id].records]
    assert [body.metadata["simple-codec"] for body in stored] == [b"true", b"true"]
    assert [
        temporalio.api.common.v1.Payloads.FromString(body.data).payloads[0].data
        for body in stored
    ] == [b'"a"', b'"b"']
    assert [record.body async for record in stream.iter_read_items()] == ["a", "b"]


async def test_client_append_has_client_producer(client: Client):
    store = MemoryStreamStore()
    stream = await _create_stream(client, store)
    await stream.append("a")
    [record] = store.streams[stream.ref.id].records
    assert record.producer.id.startswith("client:")
    assert record.producer.WhichOneof("kind") == "client"


async def test_workflow_activity_append_has_activity_producer(client: Client):
    scheduled = datetime(2026, 11, 12, 10, 0, 0, 123456, tzinfo=timezone.utc)
    attempt_scheduled = scheduled + timedelta(seconds=30)
    producer = await _append_in_activity(
        client,
        dataclasses.replace(
            ActivityEnvironment.default_info(),
            activity_id="step:1",
            attempt=3,
            workflow_run_id="run-1",
            scheduled_time=scheduled,
            current_attempt_scheduled_time=attempt_scheduled,
        ),
    )
    epoch_nanos = (
        (scheduled - datetime(1970, 1, 1, tzinfo=timezone.utc))
        // timedelta(microseconds=1)
        * 1000
    )
    assert producer.id == f"act:run-1:step%3A1:{epoch_nanos}"
    assert producer.WhichOneof("kind") == "workflow_activity"
    assert producer.workflow_activity.run_id == "run-1"
    assert producer.workflow_activity.epoch.attempt == 3
    assert producer.workflow_activity.epoch.attempt_scheduled_time == _timestamp(
        attempt_scheduled
    )


async def test_standalone_activity_append_has_activity_producer(client: Client):
    attempt_scheduled = datetime(2026, 11, 12, 10, 0, 30, tzinfo=timezone.utc)
    producer = await _append_in_activity(
        client,
        dataclasses.replace(
            ActivityEnvironment.default_info(),
            activity_id="job",
            activity_run_id="job-run",
            attempt=2,
            workflow_id=None,
            workflow_namespace=None,
            workflow_run_id=None,
            workflow_type=None,
            current_attempt_scheduled_time=attempt_scheduled,
        ),
    )
    assert producer.id == "sa:job:job-run"
    assert producer.WhichOneof("kind") == "standalone_activity"
    assert producer.standalone_activity.run_id == ""
    assert producer.standalone_activity.epoch.attempt == 2
    assert producer.standalone_activity.epoch.attempt_scheduled_time == _timestamp(
        attempt_scheduled
    )


async def test_local_activity_append_fails(client: Client):
    with pytest.raises(RuntimeError, match="Local Activity"):
        await _append_in_activity(
            client,
            dataclasses.replace(ActivityEnvironment.default_info(), is_local=True),
        )


async def test_explicit_producer_is_sent(client: Client):
    store = MemoryStreamStore()
    stream = await _create_stream(client, store)
    with mock.patch.object(
        temporalio.nexus.system._support, "default_producer"
    ) as default_producer:
        await stream.append(
            "a", producer=Producer(id="mine", kind=ProducerKindClient(ClientProducer()))
        )
    default_producer.assert_not_called()
    [record] = store.streams[stream.ref.id].records
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
    for name in ("Stream", "WorkflowStream"):
        value = getattr(temporalio.workflow, name, None)
        assert value is None or value.__module__.startswith(
            "temporalio.nexus.system.stream_service."
        )


async def test_client_without_stream_store_starts_nexus_operation(client: Client):
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


async def test_client_rejects_two_stream_stores(client: Client):
    config = client.config()
    config["stream_stores"] = [MemoryStreamStore(), MemoryStreamStore()]
    with pytest.raises(ValueError, match="selector"):
        Client(**config)
