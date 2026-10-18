"""The plugin a store configuration is, and how each context finds the store.

A configuration class is registered once, on the client:
``Client.connect(plugins=[RedisStreams(...)])``. Connecting the client asks the
stream runtime for the store it names. The runtime serves that one store to the
client's handles and to every Worker built from the client, so a Workflow's
publish, an Activity's append and an outside read meet in the same place.

On a Worker the plugin hands the Worker the store, and the Worker appends
what :func:`temporalio.contrib.streams.workflow_writer` publishes before it
completes the activation. It adds the interceptor that gives a Local Activity a
producer identity and tells Workflow code the Worker has a store. On a
Replayer it adds the interceptor only, since a replay stores nothing.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeVar, cast

import temporalio.bridge.client
from temporalio.bridge.proto.streams import (
    DeleteOwnerRequest,
    StreamOwnerKind,
    StreamStoreConfig,
)
from temporalio.bridge.streams import StreamCallFailure, StreamStore
from temporalio.bridge.streams_generated import StreamService
from temporalio.contrib.streams._capabilities import StoreCapabilities, describe
from temporalio.contrib.streams._errors import error_from_failure
from temporalio.plugin import SimplePlugin

if TYPE_CHECKING:
    from temporalio.bridge.temporal_sdk_bridge import StreamStoreRef
    from temporalio.client import Client, ClientConfig
    from temporalio.service import ConnectConfig, ServiceClient
    from temporalio.worker import ReplayerConfig, WorkerConfig

__all__ = ["StreamStorePlugin"]

T = TypeVar("T")


class StreamStorePlugin(SimplePlugin):
    """The base class of the store configuration classes.

    Use :class:`temporalio.contrib.streams.redis.RedisStreams` or
    :class:`temporalio.contrib.streams.memory.MemoryStreams`. One instance
    is one store, on one client connection, since the stream runtime asks about
    each stream's owner over the connection that connected the store. A second
    connection, such as another namespace's endpoint, needs its own instance.
    Registering a second, different store on the same client raises
    ``ValueError``, because a handle could otherwise pick either.
    """

    def __init__(self, name: str, config: StreamStoreConfig) -> None:
        """Name the plugin and the store the stream runtime connects to."""
        super().__init__(name)
        self._config = config
        self._service: StreamService | None = None
        self._store: StreamStore | None = None
        self._capabilities: StoreCapabilities | None = None
        self._connecting: asyncio.Lock | None = None
        self._service_client: ServiceClient | None = None
        # Whether Client.connect connected the store, which a Worker needs.
        self._connected_by_connect = False

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        """Register this store on the client.

        Raises:
            ValueError: Another store is already registered on the client.
        """
        for plugin in config.get("plugins", []):
            if isinstance(plugin, StreamStorePlugin) and plugin is not self:
                raise ValueError(_two_stores(plugin, self))
        return super().configure_client(config)

    async def connect_service_client(
        self,
        config: ConnectConfig,
        next: Callable[[ConnectConfig], Awaitable[ServiceClient]],
    ) -> ServiceClient:
        """Connect the client, then the store, so Workers built from it have one.

        A lazy client connects here too, since the stream runtime needs its
        connection to ask about a stream's owner.

        Raises:
            temporalio.contrib.streams.StreamError: The stream runtime could
                not reach the store.
        """
        service_client = await next(config)
        await self._connected(service_client)
        self._connected_by_connect = True
        return service_client

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Check that the Worker's client carries this store.

        Also adds the interceptor that stamps each Local Activity call, so a
        Local Activity can publish.

        Raises:
            ValueError: The plugin is not registered on the Worker's client,
                or that client was not made with ``Client.connect``. A
                client made otherwise is refused even after one of its handles
                connected the store.
        """
        config = super().configure_worker(config)
        client = config["client"]  # type:ignore[reportTypedDictNotRequiredAccess]
        if (
            self not in client.config()["plugins"]
            or not self._connected_by_connect
            or client.service_client is not self._service_client
        ):
            raise ValueError(
                f"stream store {self.name()!r} must be registered on the Worker's "
                "client, as Client.connect(..., plugins=[store])"
            )
        # Imported here, since the Activity reaches the store through this module.
        from temporalio.contrib.streams._local_activity import StreamsInterceptor

        interceptors = list(config.get("interceptors") or [])
        # Two would number each Local Activity call twice.
        if not any(isinstance(i, StreamsInterceptor) for i in interceptors):
            interceptors.append(StreamsInterceptor())
        config["interceptors"] = interceptors
        return config

    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Add the interceptor a Workflow that uses ``workflow_writer`` replays with.

        The Replayer needs no store, since a replay publishes no records.
        Without the interceptor, ``workflow_writer`` raises
        :class:`temporalio.contrib.streams.StreamUnsupportedError` on replay.
        """
        config = super().configure_replayer(config)
        from temporalio.contrib.streams._local_activity import StreamsInterceptor

        interceptors = list(config.get("interceptors") or [])
        if not any(isinstance(i, StreamsInterceptor) for i in interceptors):
            interceptors.append(StreamsInterceptor())
        config["interceptors"] = interceptors
        return config

    @property
    def _temporal_stream_store(self) -> StreamStoreRef | None:
        # The Worker looks for this name on its plugins and publishes its Workflows' records to it.
        return self._store.ref if self._store is not None else None

    @property
    def capabilities(self) -> StoreCapabilities:
        """What the store can do, as the stream runtime found on connect.

        Raises:
            ValueError: No client connected this store yet.
        """
        if self._capabilities is None:
            raise ValueError(f"no client has connected stream store {self.name()!r}")
        return self._capabilities

    async def delete_workflow_streams(self, namespace: str, workflow_id: str) -> int:
        """Delete every stream of ``workflow_id`` in ``namespace``.

        An admin helper for cleanup and compliance requests. Deleting a
        Workflow does not delete its streams. Their retention bounds what is
        left, and this removes it at once, across every run chain of the
        Workflow id. It asks Temporal nothing, so it can run after the
        Workflow is gone. On Redis it walks the keyspace, so run it rarely,
        not on a hot path.

        Delete only after the chain closed and its producers stopped. A late
        append writes a fresh stream with no dedupe state and no close flag.

        Returns:
            How many store keys were deleted.

        Raises:
            ValueError: No client connected this store yet.
            temporalio.contrib.streams.StreamUnsupportedError: The store
                can't delete by owner (see :attr:`capabilities`). Records go
                at retention.
            temporalio.contrib.streams.StreamStorageError: The store failed;
                some keys may be deleted.
        """
        if self._service is None:
            raise ValueError(f"no client has connected stream store {self.name()!r}")
        request = DeleteOwnerRequest(
            namespace=namespace,
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
            workflow_id=workflow_id,
        )
        return (await call(self._service.delete_owner(request))).deleted

    async def _service_for(self, client: Client) -> StreamService:
        """The stream service, connecting the store if no client did yet.

        Raises:
            ValueError: Another client connection already connected the store.
        """
        await self._connected(client.service_client)
        assert self._service is not None
        return self._service

    async def _connected(self, service_client: ServiceClient) -> None:
        if self._service_client is service_client:
            return
        if self._connecting is None:
            self._connecting = asyncio.Lock()
        async with self._connecting:
            if self._service_client is service_client:
                return
            if self._service_client is not None:
                raise ValueError(
                    f"stream store {self.name()!r} serves one client connection, "
                    "and another one already connected it; give each connection "
                    "a store instance of its own"
                )
            bridge: temporalio.bridge.client.Client = await cast(
                Any, service_client
            )._connected_client()
            store = await call(StreamStore.connect(bridge, self._config))
            self._capabilities = await call(describe(store))
            self._store, self._service = store, StreamService(store)
            self._service_client = service_client


async def call(pending: Awaitable[T]) -> T:
    """Await a stream call, raising its failure as the error a caller catches."""
    try:
        return await pending
    except StreamCallFailure as failure:
        raise error_from_failure(failure.failure) from None


def _two_stores(held: StreamStorePlugin, new: StreamStorePlugin) -> str:
    return (
        f"stream store {held.name()!r} is already registered, so {new.name()!r} "
        "cannot be; register one store per client"
    )


def with_owner_start_grace(
    config: StreamStoreConfig, grace: timedelta | None
) -> StreamStoreConfig:
    """Set ``owner_start_grace`` on ``config`` when ``grace`` is given.

    Raises:
        ValueError: ``grace`` is negative.
    """
    if grace is not None:
        if grace < timedelta(0):
            raise ValueError(f"owner_start_grace must not be negative, got {grace}")
        config.owner_start_grace.FromTimedelta(grace)
    return config


def store_for_client(client: Client) -> StreamStorePlugin:
    """The store registered on ``client``.

    Raises:
        ValueError: No store is registered on the client.
    """
    for plugin in client.config()["plugins"]:
        if isinstance(plugin, StreamStorePlugin):
            return plugin
    raise ValueError(
        "no stream store is registered on this client; pass one as a plugin, for "
        "example Client.connect(..., plugins=[RedisStreams(...)])"
    )
