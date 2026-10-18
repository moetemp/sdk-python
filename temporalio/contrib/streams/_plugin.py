"""The plugin a store configuration is, and how each context finds the store.

A configuration class is registered once, on the client:
``Client.connect(plugins=[RedisStreams(...)])``. Connecting the client asks
the stream runtime for the store it names. The stream runtime serves that one
store to the client's handles and to every Worker built from the client, so a
Workflow's publish, an Activity's append and an outside read meet in the same
place.

On a Worker the plugin hands the Worker the store, and the Worker appends
what :func:`temporalio.contrib.streams.workflow_writer` publishes before it
completes the activation. It adds the interceptor that gives a Local Activity a
producer identity and tells Workflow code the Worker has a store, and it
registers the built-in Activities that
:func:`temporalio.contrib.streams.workflow_reader` and
:func:`temporalio.contrib.streams.close_workflow_stream` run as Local
Activities.
On a Replayer it adds the interceptor only, since a replay stores nothing.
When a Worker stops, the plugin waits for the notifications the stream runtime
still has out.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeVar, cast

import temporalio.bridge.client
from temporalio.bridge.proto.streams import (
    DeleteOwnerRequest,
    FlushNotificationsRequest,
    StreamOwnerKind,
    StreamStoreConfig,
)
from temporalio.bridge.streams import StreamCallFailure, StreamStore
from temporalio.bridge.streams_generated import StreamNexusService, StreamService
from temporalio.contrib.streams._capabilities import StoreCapabilities, describe
from temporalio.contrib.streams._errors import error_from_failure
from temporalio.converter import DataConverter
from temporalio.plugin import SimplePlugin

if TYPE_CHECKING:
    from typing_extensions import Self

    from temporalio.bridge.temporal_sdk_bridge import StreamStoreRef
    from temporalio.client import Client, ClientConfig
    from temporalio.contrib.streams._ref import StreamRef
    from temporalio.service import ConnectConfig, ServiceClient
    from temporalio.worker import ReplayerConfig, Worker, WorkerConfig

__all__ = ["StreamStorePlugin"]

T = TypeVar("T")


class StreamStorePlugin(SimplePlugin):
    """The base class of the store configuration classes.

    Use :class:`temporalio.contrib.streams.redis.RedisStreams`,
    :class:`temporalio.contrib.streams.postgres.PostgresStreams` or
    :class:`temporalio.contrib.streams.memory.MemoryStreams`. One instance
    is one store, on one client connection, since the stream runtime asks about each
    stream's owner over the connection that connected the store. A second
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
        # Set once the server answered that it has no stream notifier.
        self._notifier_unsupported = False
        # Imported here, since that module imports this one.
        from temporalio.contrib.streams._replay_reads import (
            BatchCache,
            ReadStates,
        )

        self._batches = BatchCache()
        self._read_states = ReadStates()
        self._follows = BatchCache()

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        """Register this store on the client.

        Raises:
            ValueError: Another store is already registered on the client.
        """
        for plugin in config.get("plugins", []):
            if isinstance(plugin, StreamStorePlugin) and plugin is not self:
                raise ValueError(_two_stores(plugin, self))
        config = super().configure_client(config)
        # A positions-only Workflow read turns back into its batch on decode.
        from temporalio.contrib.streams._replay_reads import wrap_codec

        config["data_converter"] = wrap_codec(
            self, config.get("data_converter") or DataConverter.default
        )
        return config

    async def connect_service_client(
        self,
        config: ConnectConfig,
        next: Callable[[ConnectConfig], Awaitable[ServiceClient]],
    ) -> ServiceClient:
        """Connect the client, then the store, so Workers built from it have one.

        A lazy client connects here too, since the stream runtime needs its connection to
        ask about a stream's owner.

        Raises:
            temporalio.contrib.streams.StreamError: the stream runtime could not reach the
                store.
        """
        service_client = await next(config)
        await self._connected(service_client)
        self._connected_by_connect = True
        return service_client

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Check that the Worker's client carries this store, and add the built-in Activities.

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
        from temporalio.contrib.streams._workflow import read_activity
        from temporalio.contrib.streams._workflow_follow import follow_activity
        from temporalio.contrib.streams._workflow_writer import close_activity

        activities = list(config.get("activities") or [])
        # The same plugin on the client and the Worker configures the Worker twice.
        for builtin in (read_activity, close_activity, follow_activity):
            if builtin not in activities:
                activities.append(builtin)
        config["activities"] = activities
        interceptors = list(config.get("interceptors") or [])
        # Two would number each Local Activity call twice.
        if not any(isinstance(i, StreamsInterceptor) for i in interceptors):
            interceptors.append(StreamsInterceptor())
        config["interceptors"] = interceptors
        return config

    async def run_worker(
        self,
        worker: Worker,
        next: Callable[[Worker], Awaitable[None]],
    ) -> None:
        """Run the Worker, then wait for the notifications the stream runtime still has out.

        The Worker's Activities append and close through this store, and the stream runtime
        tells each stream's notifier in the background. A Worker that stops
        right after its last Activity would otherwise drop a notification a
        sleeping reader waits for.
        """
        try:
            await super().run_worker(worker, next)
        finally:
            await self.flush_notifications()

    def notify_on_append(
        self, *, max_notifiers: int | None = None, notification_log: bool = False
    ) -> Self:
        """Tell each stream's notifier on the server when the stream moves.

        That is what wakes a reader that sleeps on the stream: a client
        reader that waits on the notifier, or a Workflow reader. The stream runtime tells
        the notifier after every append, and folds the notifications of one
        stream into one call in flight. Off by default, since most streams
        have no sleeping reader. It needs a server with the stream notifier. Turn
        it on before a client connects the store, in every process that
        appends or reads, since a client reader sleeps on the notifier only
        with it on.

        Args:
            max_notifiers: How many streams the stream runtime keeps a notifier for,
                dropping the least recently used. The stream runtime's default when unset.
            notification_log: Write a log entry with each append, in the
                same store step, in place of the producer's notification. A
                :class:`temporalio.contrib.streams.StreamDriver` reads the
                log and notifies the server. Producers then make no call to
                Temporal. Turn it on in every process that appends, and run
                at least one driver.

        Raises:
            ValueError: A client already connected this store, or
                ``max_notifiers`` is below one.

        .. warning::
            This API is experimental.
        """
        if self._store is not None:
            raise ValueError(
                "turn notifications on before a client connects the stream store"
            )
        if max_notifiers is not None and max_notifiers < 1:
            raise ValueError(f"max_notifiers must be at least 1, got {max_notifiers}")
        self._config.notify_on_append = True
        self._config.notification_log = notification_log
        if max_notifiers is not None:
            self._config.max_notifiers = max_notifiers
        return self

    def _waits_on_notifier(self) -> bool:
        """Whether a client reader sleeps on the stream's notifier when the stream is idle.

        Only with notifications on, since without them no producer in the
        deployment tells the notifier, and a sleeping reader would wake only
        at the server's wait limit.
        """
        return self._config.notify_on_append and not self._notifier_unsupported

    async def close_stream(
        self,
        client: Client,
        ref: StreamRef,
        result: Any = None,
        *,
        error: BaseException | None = None,
    ) -> None:
        """Close the stream ``ref`` names, with ``result`` or ``error`` as its outcome.

        See :meth:`temporalio.contrib.streams.StreamHandle.close`.

        Raises:
            ValueError: Both ``result`` and ``error`` are set.
            temporalio.contrib.streams.StreamError: The store or the
                notifier refused the close.

        .. warning::
            This API is experimental.
        """
        # Imported here, since the handles reach the store through this module.
        from temporalio.contrib.streams._handles import StreamHandle

        ref._require_supported()
        await StreamHandle(self, client, ref).close(result, error=error)

    async def flush_notifications(self) -> None:
        """Wait for the notifications the stream runtime still has out, for up to 10 seconds.

        The stream runtime tells a stream's notifier on the server in the background, after
        an append that a sleeping reader waits for and after a close. A
        Worker built from the client does this when it stops. A process that
        holds only a client calls it before it exits. Before a client
        connected the store, it returns at once.

        .. warning::
            This API is experimental.
        """
        if self._service is not None:
            await call(self._service.flush_notifications(FlushNotificationsRequest()))

    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Add the interceptor a Workflow that uses ``workflow_writer`` replays with.

        The Replayer needs no store, since a replay publishes no records.
        Without the interceptor, ``workflow_writer`` raises
        :class:`temporalio.contrib.streams.StreamUnsupportedError` on replay.
        A ``workflow_reader`` with ``replay_from_store=True``, the default, reads its batches
        from the store again, so it needs a client in this process that
        connected this store.
        """
        config = super().configure_replayer(config)
        from temporalio.contrib.streams._replay_reads import wrap_codec

        config["data_converter"] = wrap_codec(
            self, config.get("data_converter") or DataConverter.default
        )
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
        """What the store can do, as the stream runtime found when a client connected it.

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
        Workflow id, and the streams of every Activity that a run of the
        Workflow scheduled. It asks Temporal nothing, so it can run after the
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

    async def delete_activity_streams(self, namespace: str, activity_id: str) -> int:
        """Delete every stream of standalone Activity ``activity_id`` in ``namespace``.

        The same admin helper as :meth:`delete_workflow_streams`, for the
        streams of every run of the Activity id.

        Returns:
            How many store keys were deleted.

        Raises:
            ValueError: No client connected this store yet.
            temporalio.contrib.streams.StreamStorageError: The store failed;
                some keys may be deleted.
        """
        if self._service is None:
            raise ValueError(f"no client has connected stream store {self.name()!r}")
        request = DeleteOwnerRequest(
            namespace=namespace,
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_ACTIVITY,
            activity_id=activity_id,
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

    async def _nexus_service_for(self, client: Client) -> StreamNexusService:
        """The stream service's Nexus calls, on the store of ``client``.

        Raises:
            ValueError: Another client connection already connected the store.
        """
        await self._connected(client.service_client)
        assert self._store is not None
        return StreamNexusService(self._store)

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
    """Await a stream call, raising the stream runtime's failure as the error a caller catches."""
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
