"""What the Bridge live tests share: a second namespace and Nexus endpoints.

The Bridge live tests need a server with the stream notifier and Nexus operation
progress, as ``scripts/streams_live_lane.sh`` builds. The cross-namespace
demos also need the namespace ``streams-b``, which the lane registers.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Any

import temporalio.api.nexus.v1
import temporalio.api.operatorservice.v1
from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
from temporalio.client import Client
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.contrib.streams.nexus import ClientTransport, StreamDirectReads
from temporalio.service import RPCError, RPCStatusCode

#: The namespace that owns the streams in the cross-namespace tests.
NAMESPACE_B = "streams-b"

#: Makes a store that no client connected yet, so a demo can turn on notifications.
NewStore = Callable[[], StreamStorePlugin]


async def connect(
    client: Client,
    plugin: StreamStorePlugin | None,
    *,
    namespace: str | None = None,
    reads: str | None = None,
) -> Client:
    """A client of ``client``'s server in ``namespace``, carrying ``plugin``.

    It connects, since connecting is what hands the store to Workers built
    from it. With ``reads``, an endpoint name, it also carries
    ``StreamDirectReads`` with a ``ClientTransport`` for that endpoint, so a
    ``StreamReader`` with ``replay_from_store=True`` on its Workers reads
    directly.
    """
    plugins: list[Any] = [plugin] if plugin is not None else []
    if reads is not None:
        plugins.append(StreamDirectReads({reads: ClientTransport(client, reads)}))
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=namespace or client.namespace,
        data_converter=client.data_converter,
        plugins=plugins,
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


def endpoint_name(task_queue: str) -> str:
    """The name :func:`endpoint` gives the endpoint of ``task_queue``."""
    return f"nexus-endpoint-{task_queue}"


async def endpoint(
    client: Client, task_queue: str, *, namespace: str | None = None
) -> str:
    """Create a Nexus endpoint that targets ``task_queue`` in ``namespace``.

    Callers in any namespace of the server use it by name. Returns the name.
    """
    name = endpoint_name(task_queue)
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


def http_address(client: Client) -> str:
    """The address of the server's Nexus HTTP ingress.

    ``TEMPORAL_HTTP_ADDRESS`` names it. Without it, the live lane's layout
    holds: the HTTP port is the gRPC port plus one.
    """
    if address := os.environ.get("TEMPORAL_HTTP_ADDRESS"):
        return address
    host, port = client.service_client.config.target_host.rsplit(":", 1)
    return f"{host}:{int(port) + 1}"


async def stream_service_url(client: Client, endpoint_name: str) -> str:
    """The stream service's address on endpoint ``endpoint_name``, for an HTTP caller."""
    listed = await client.operator_service.list_nexus_endpoints(
        temporalio.api.operatorservice.v1.ListNexusEndpointsRequest(name=endpoint_name)
    )
    [found] = listed.endpoints
    return (
        f"http://{http_address(client)}/nexus/endpoints/{found.id}"
        "/services/temporal.sdk.streams.v1.TemporalStreams"
    )


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
