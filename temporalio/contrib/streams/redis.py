"""Streams in the application's Redis."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from temporalio.bridge.proto.streams import RedisStoreConfig, StreamStoreConfig
from temporalio.contrib.streams._plugin import (
    StreamStorePlugin,
    with_owner_start_grace,
)
from temporalio.contrib.streams.archive import StreamArchive

__all__ = ["RedisStreams"]


class RedisStreams(StreamStorePlugin):
    """A store in a Redis or a Redis Cluster that the stream runtime connects to.

    One instance per Redis deployment. Pass it to the client as a plugin,
    and open handles from the client with
    :func:`temporalio.contrib.streams.get_stream_handle`. Redis 7.0 or later
    is required, with ``maxmemory-policy noeviction``.
    """

    def __init__(
        self,
        url: str | Sequence[str],
        *,
        cluster: bool = False,
        key_prefix: str = "temporal-streams",
        retention: timedelta = timedelta(days=7),
        blocking_reads_per_node: int | None = None,
        response_timeout: timedelta | None = None,
        floor_lease: timedelta | None = None,
        floor_release_grace: timedelta | None = None,
        owner_start_grace: timedelta | None = None,
        notify_doorbells: int | None = None,
        archive: StreamArchive | None = None,
        plain: bool = False,
    ) -> None:
        """Create the store's configuration.

        Args:
            url: A ``redis://`` or ``rediss://`` URL, or the seed nodes of a
                cluster. Credentials and TLS settings come from the URL. In
                this release TLS verifies the server against the system's
                root certificates only, with no custom CA and no client
                certificate.
            cluster: Whether ``url`` names a Redis Cluster. Every key of one
                Workflow's streams shares a hash tag, so a cluster serves
                them from one slot.
            key_prefix: Prepended to every key, so streams can share a Redis
                with other data and an ACL can scope them.
            retention: How long a stream keeps a record, and how long after
                its last write the stream itself lives.
            blocking_reads_per_node: How many reads may wait on one node at
                once, each on a connection of its own. The stream runtime's default when
                unset.
            response_timeout: How long a short call waits for its reply.
                The stream runtime's default when unset.
            floor_lease: How long a Workflow reader that replays from the
                store, as ``workflow_reader`` does by default, holds the records it read against
                retention after its last read. ``retention`` when unset.
            floor_release_grace: How long such a hold lasts after a reader
                releases it. One day when unset. A Python Workflow reader
                never releases, since its run may replay its reads until it
                ends, so its hold lasts ``floor_lease``.
            owner_start_grace: How long a read of a Workflow that Temporal
                doesn't know yet answers empty before it fails with
                :class:`temporalio.contrib.streams.StreamNotFoundError`, so
                a reader can start before its owner. 60 seconds when unset.
                Zero fails at once.
            notify_doorbells: With a notification log, how many doorbell
                streams an append rings when its slot's log takes a first
                entry. A driver blocks on each one. The stream runtime's 16 when unset.
            archive: Object storage that keeps records after ``retention``
                trims them, so streams stay readable longer than Redis
                memory allows. A running
                :class:`temporalio.contrib.streams.StreamDriver` fills it,
                and the store needs
                ``notify_on_append(notification_log=True)``. The stream runtime refuses
                to connect the store without the log.
            plain: Run no Lua scripts, for a Redis that refuses them. The
                store then has no producer state, no kept last position, no
                holds and no change feed, and the stream runtime takes the fallback of
                each (see :attr:`capabilities`).

        Raises:
            ValueError: No URL was given, ``key_prefix`` is empty,
                ``retention`` is shorter than a millisecond, or
                ``owner_start_grace`` is negative, or ``notify_doorbells`` is
                below one.
        """
        urls = [url] if isinstance(url, str) else list(url)
        if not urls:
            raise ValueError("RedisStreams needs a URL")
        if not key_prefix:
            raise ValueError("key_prefix must not be empty")
        if retention < timedelta(milliseconds=1):
            raise ValueError(
                f"retention must be at least a millisecond, got {retention}"
            )
        if notify_doorbells is not None and notify_doorbells < 1:
            raise ValueError(
                f"notify_doorbells must be at least 1, got {notify_doorbells}"
            )
        # Set only when asked for, so the config also fits a Core without it.
        extra: dict[str, Any] = {"plain": True} if plain else {}
        config = RedisStoreConfig(
            urls=urls,
            cluster=cluster,
            key_prefix=key_prefix,
            blocking_reads_per_node=blocking_reads_per_node or 0,
            notify_doorbells=notify_doorbells or 0,
            **extra,
        )
        config.retention.FromTimedelta(retention)
        if response_timeout is not None:
            config.response_timeout.FromTimedelta(response_timeout)
        if floor_lease is not None:
            config.floor_lease.FromTimedelta(floor_lease)
        if floor_release_grace is not None:
            config.floor_release_grace.FromTimedelta(floor_release_grace)
        store = StreamStoreConfig(redis=config)
        if archive is not None:
            store.archive.CopyFrom(archive._config)
        super().__init__(
            "temporalio.contrib.streams.RedisStreams",
            with_owner_start_grace(store, owner_start_grace),
        )
