"""What the Bridge scenario demos share: stores, a second namespace and Nexus endpoints.

The Bridge demos need a server with the stream notifier and Nexus operation
progress, as ``scripts/streams_live_lane.sh`` builds. The cross-namespace
demos also need the namespace ``streams-b``, which the lane registers.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import temporalio.api.nexus.v1
import temporalio.api.operatorservice.v1
from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
from temporalio.client import Client
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.service import RPCError, RPCStatusCode
from tests.contrib.streams.samples.scenarios._common import store

#: The namespace that owns the streams in the cross-namespace demos.
NAMESPACE_B = "streams-b"

#: Makes a store that no client connected yet, so a demo can turn on notifications.
NewStore = Callable[[], StreamStorePlugin]


async def connect(
    client: Client, plugin: StreamStorePlugin | None, *, namespace: str | None = None
) -> Client:
    """A client of ``client``'s server in ``namespace``, carrying ``plugin``.

    It connects, since connecting is what hands the store to Workers built
    from it.
    """
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=namespace or client.namespace,
        data_converter=client.data_converter,
        plugins=[plugin] if plugin is not None else [],
    )


async def has_namespace(client: Client, namespace: str) -> bool:
    """Whether the server knows ``namespace``."""
    try:
        await client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=namespace)
        )
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            return False
        raise
    return True


async def two_namespaces(client: Client, plugin: StreamStorePlugin) -> Client:
    """A client of namespace ``streams-b`` that carries ``plugin``.

    Only Workers built from it hold the store, so a caller in ``client``'s
    namespace reaches the streams through Nexus alone.

    Raises:
        RuntimeError: The server has no namespace ``streams-b``.
    """
    if not await has_namespace(client, NAMESPACE_B):
        raise RuntimeError(
            f"the server has no namespace {NAMESPACE_B!r}; the live lane registers it"
        )
    return await connect(client, plugin, namespace=NAMESPACE_B)


async def endpoint(
    client: Client, task_queue: str, *, namespace: str | None = None
) -> str:
    """Create a Nexus endpoint that targets ``task_queue`` in ``namespace``.

    Callers in any namespace of the server use it by name. Returns the name.
    """
    name = f"nexus-endpoint-{task_queue}"
    await client.operator_service.create_nexus_endpoint(
        temporalio.api.operatorservice.v1.CreateNexusEndpointRequest(
            spec=temporalio.api.nexus.v1.EndpointSpec(
                name=name,
                target=temporalio.api.nexus.v1.EndpointTarget(
                    worker=temporalio.api.nexus.v1.EndpointTarget.Worker(
                        namespace=namespace or client.namespace,
                        task_queue=task_queue,
                    )
                ),
            )
        )
    )
    return name


async def until(check: Callable[[], Awaitable[bool]], timeout: float = 30.0) -> None:
    """Wait until ``check`` answers true.

    Raises:
        TimeoutError: It didn't within ``timeout`` seconds.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await check():
        if loop.time() >= deadline:
            raise TimeoutError(f"the condition didn't hold within {timeout} s")
        await asyncio.sleep(0.1)


def run(main: Callable[..., Awaitable[Any]]) -> None:
    """Run a Bridge demo's ``main`` from the command line.

    ``TEMPORAL_ADDRESS`` names a server with the stream notifier (the live
    lane's), and ``STREAMS_REDIS_URL`` picks Redis over memory.
    """

    async def go() -> None:
        client = await Client.connect(
            os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
        )
        # The task queue names the endpoint, and an endpoint name takes no underscore.
        name = os.path.basename(sys.argv[0]).removesuffix(".py").replace("_", "-")
        result = await main(
            client,
            f"{name}-{uuid.uuid4().hex}",
            workflow_id=f"{name}-{uuid.uuid4().hex}",
            new_store=store,
        )
        print(f"result: {result!r}")

    asyncio.run(go())
