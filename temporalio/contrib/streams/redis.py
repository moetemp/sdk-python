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

__all__ = ["RedisStreams"]


class RedisStreams(StreamStorePlugin):
    """A store in a Redis or a Redis Cluster that the stream runtime connects to.

    One instance per Redis deployment. Pass it to the client as a plugin,
    and open handles from the client with
    :func:`temporalio.contrib.streams.stream_handle`. Redis 7.0 or later
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
        owner_start_grace: timedelta | None = None,
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
                once, each on a connection of its own. The stream runtime's
                default when unset.
            response_timeout: How long a short call waits for its reply.
                The stream runtime's default when unset.
            owner_start_grace: How long a read of a Workflow that Temporal
                doesn't know yet answers empty before it fails with
                :class:`temporalio.contrib.streams.StreamNotFoundError`, so
                a reader can start before its owner. 60 seconds when unset.
                Zero fails at once.
            plain: Run no Lua scripts, for a Redis that refuses them. The store
                then has no producer state, no kept last position, no holds and
                no change feed, and the stream runtime takes the fallback of
                each (see :attr:`capabilities`).

        Raises:
            ValueError: No URL was given, ``key_prefix`` is empty,
                ``retention`` is shorter than a millisecond, or
                ``owner_start_grace`` is negative.
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
        # Set only when asked for, so the config also fits a runtime without it.
        extra: dict[str, Any] = {"plain": True} if plain else {}
        config = RedisStoreConfig(
            urls=urls,
            cluster=cluster,
            key_prefix=key_prefix,
            blocking_reads_per_node=blocking_reads_per_node or 0,
            **extra,
        )
        config.retention.FromTimedelta(retention)
        if response_timeout is not None:
            config.response_timeout.FromTimedelta(response_timeout)
        super().__init__(
            "temporalio.contrib.streams.RedisStreams",
            with_owner_start_grace(StreamStoreConfig(redis=config), owner_start_grace),
        )
