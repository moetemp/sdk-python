"""The stream service, served by a Worker and called over the Nexus HTTP ingress.

Every case runs against the stock dev server, started here with an HTTP port,
through a Nexus endpoint the case creates and deletes, so the request goes
through the same ingress a remote caller's does. Core serves the contract and
its own tests pin each rule. These cases check the whole path end to end, and
pin what only the SDK decides: the authorizer, the payload codec and its
serialization context, and the failure that crosses.

The cases are parametrized over ``BACKINGS``, the stores the service runs
over. Each yields a :class:`Backing`.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio

import temporalio.api.nexus.v1
import temporalio.api.operatorservice.v1
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRef,
    StreamStorePlugin,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    AppendInput,
    AppendOutput,
    HTTPStatusError,
    ReadInput,
    ReadOutput,
    StreamAccess,
    TemporalStreamsHandler,
    TemporalStreamsHttpClient,
)
from temporalio.converter import (
    DataConverter,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import PollerBehaviorSimpleMaximum, Worker
from tests import DEV_SERVER_DOWNLOAD_VERSION
from tests.helpers import find_free_port

_SERVICE = "temporal.sdk.streams.v1.TemporalStreams"


@dataclass
class Server:
    """The dev server these cases share, and its Nexus HTTP ingress."""

    env: WorkflowEnvironment
    http: str

    @property
    def client(self) -> Client:
        return self.env.client


@pytest_asyncio.fixture(scope="module")  # type: ignore[reportUntypedFunctionDecorator]
async def server() -> AsyncIterator[Server]:
    port = find_free_port()
    env = await WorkflowEnvironment.start_local(
        dev_server_extra_args=[
            "--http-port",
            str(port),
            "--dynamic-config-value",
            'system.system.refreshNexusEndpointsMinWait="0s"',
            "--dynamic-config-value",
            "activity.enableStandalone=true",
        ],
        dev_server_download_version=DEV_SERVER_DOWNLOAD_VERSION,
    )
    try:
        yield Server(env, f"http://127.0.0.1:{port}")
    finally:
        await env.shutdown()


@dataclass
class Backing:
    """One store the service runs over, and a client that carries it."""

    name: str
    store: StreamStorePlugin
    client: Client
    host: Callable[[str], Awaitable[None]]
    """Starts the Workflow that owns a stream, since Core checks that the
    owner exists before a stream is written."""


@workflow.defn
class StreamOwner:
    """Owns a stream until it is told to finish."""

    def __init__(self) -> None:
        self.done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.done)

    @workflow.signal
    def finish(self) -> None:
        self.done = True


async def connect(
    client: Client, store: StreamStorePlugin, converter: DataConverter | None = None
) -> Client:
    """A client of ``client``'s server that carries ``store``."""
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        data_converter=converter or DataConverter.default,
        plugins=[store],
    )


def with_converter(client: Client, converter: DataConverter) -> Client:
    """``client`` on its own connection, with another data converter."""
    config = client.config()
    config["data_converter"] = converter
    return Client(**config)


@asynccontextmanager
async def _memory_backing(client: Client) -> AsyncIterator[Backing]:
    store = MemoryStreams()
    connected = await connect(client, store)

    async def host(workflow_id: str) -> None:
        # No Worker runs it, so it stays open for the case.
        await connected.start_workflow(
            StreamOwner.run, id=workflow_id, task_queue=f"unpolled-{uuid.uuid4().hex}"
        )

    yield Backing("memory", store, connected, host)


BACKINGS: dict[str, Callable[[Client], Any]] = {"memory": _memory_backing}


async def allow_all(_access: StreamAccess) -> bool:
    return True


@dataclass
class Service:
    """A handler served by a Worker, and a caller on its endpoint."""

    caller: TemporalStreamsHttpClient
    handler: TemporalStreamsHandler
    backing: Backing
    client: Client
    endpoint: str


@asynccontextmanager
async def serve(
    server: Server,
    backing: Backing,
    *,
    client: Client | None = None,
    **handler_options: Any,
) -> AsyncIterator[Service]:
    """Serve ``backing`` behind a fresh endpoint, deleted on the way out.

    The handler's Worker runs on ``client``, the backing's by default. It
    serves every call unless ``authorize`` is given.
    """
    handler_options.setdefault("authorize", allow_all)
    handler = TemporalStreamsHandler(**handler_options)
    task_queue = f"streams-service-{uuid.uuid4().hex}"
    endpoint_name = f"streams-{uuid.uuid4().hex}"
    worker_client = backing.client if client is None else client
    async with Worker(
        worker_client,
        task_queue=task_queue,
        nexus_service_handlers=[handler],
        # As the handler recommends: with the default five pollers over the
        # dev server's partitions, an append behind parked reads was seen to
        # wait for one of them to finish.
        nexus_task_poller_behavior=PollerBehaviorSimpleMaximum(20),
    ):
        created = await server.client.operator_service.create_nexus_endpoint(
            temporalio.api.operatorservice.v1.CreateNexusEndpointRequest(
                spec=temporalio.api.nexus.v1.EndpointSpec(
                    name=endpoint_name,
                    target=temporalio.api.nexus.v1.EndpointTarget(
                        worker=temporalio.api.nexus.v1.EndpointTarget.Worker(
                            namespace=server.client.namespace,
                            task_queue=task_queue,
                        )
                    ),
                )
            )
        )
        endpoint = created.endpoint
        try:
            yield Service(
                TemporalStreamsHttpClient(
                    f"{server.http}/nexus/endpoints/{endpoint.id}/services/{_SERVICE}"
                ),
                handler,
                backing,
                worker_client,
                endpoint_name,
            )
        finally:
            await server.client.operator_service.delete_nexus_endpoint(
                temporalio.api.operatorservice.v1.DeleteNexusEndpointRequest(
                    id=endpoint.id, version=endpoint.version
                )
            )


@pytest_asyncio.fixture(params=sorted(BACKINGS))  # type: ignore[reportUntypedFunctionDecorator]
async def backing(
    request: pytest.FixtureRequest, server: Server
) -> AsyncIterator[Backing]:
    async with BACKINGS[request.param](server.client) as found:
        yield found


@pytest_asyncio.fixture  # type: ignore[reportUntypedFunctionDecorator]
async def service(server: Server, backing: Backing) -> AsyncIterator[Service]:
    async with serve(server, backing) as found:
        yield found


async def stream_of(service: Service, topic: str = "out") -> StreamRef:
    """A fresh stream, with its owner started."""
    workflow_id = f"streams-service-{uuid.uuid4().hex}"
    await service.backing.host(workflow_id)
    return StreamRef.for_workflow(workflow_id, topic=topic)


async def activity_stream_of(service: Service, topic: str = "out") -> StreamRef:
    """A fresh stream of a standalone Activity, with its owner started.

    No Worker polls its task queue, so the Activity run stays open.
    """
    activity_id = f"streams-service-{uuid.uuid4().hex}"
    owner = await service.backing.client.start_activity(
        "never_polled",
        id=activity_id,
        task_queue=f"unpolled-{uuid.uuid4().hex}",
        start_to_close_timeout=timedelta(minutes=5),
    )
    return StreamRef.for_activity(activity_id, run_id=owner.run_id, topic=topic)


def body(text: str) -> bytes:
    """A serialized payload, as a writer's converter and codec leave it."""
    return Payload(
        metadata={"encoding": b"binary/test"}, data=text.encode()
    ).SerializeToString()


def records_of(answer: ReadOutput) -> list[WireRecord]:
    return [
        WireRecord.FromString(record.record)
        for record in answer.records
        if record.superseded is None
    ]


def texts(answer: ReadOutput) -> list[str]:
    return [record.body.data.decode() for record in records_of(answer)]


async def append(
    service: Service,
    ref: StreamRef,
    *texts_: str,
    producer_id: str = "p",
    attempt: int = 1,
    sequence: int = 1,
) -> AppendOutput:
    return await service.caller.append(
        AppendInput(
            stream=ref,
            producer_id=producer_id,
            attempt=attempt,
            sequence=sequence,
            payloads=[body(text) for text in texts_],
        )
    )


async def read(
    service: Service,
    ref: StreamRef,
    after: str = "",
    *,
    wait_ms: int = 2000,
    max_records: int | None = None,
) -> ReadOutput:
    return await service.caller.read(
        ReadInput(
            stream=ref, after_token=after, wait_ms=wait_ms, max_records=max_records
        )
    )


def refused_as(error: HTTPStatusError) -> str:
    """The stream error class the handler refused the call with.

    It leads the message and is the cause's application error type, and the
    two have to agree.
    """
    failure = json.loads(error.detail)
    cause_type = failure["details"]["cause"]["applicationFailureInfo"]["type"]
    assert failure["message"].startswith(f"{cause_type}: "), failure["message"]
    return cause_type
