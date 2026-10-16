"""Streams in this process's memory, for tests and local development."""

from __future__ import annotations

from datetime import timedelta

from temporalio.bridge.proto.streams import MemoryStoreConfig, StreamStoreConfig
from temporalio.contrib.streams._plugin import (
    StreamStorePlugin,
    with_owner_start_grace,
)

__all__ = ["MemoryStreams"]


class MemoryStreams(StreamStorePlugin):
    """A store in this process's memory.

    Core holds it, so the client's handles and every Worker built from the
    client share it. Nothing survives the process, and another process sees
    none of it.
    """

    def __init__(self, *, owner_start_grace: timedelta | None = None) -> None:
        """Create the store's configuration.

        Args:
            owner_start_grace: How long a read of a Workflow that Temporal
                doesn't know yet answers empty before it fails with
                :class:`temporalio.contrib.streams.StreamNotFoundError`, so
                a reader can start before its owner. 60 seconds when unset.
                Zero fails at once.

        Raises:
            ValueError: ``owner_start_grace`` is negative.
        """
        super().__init__(
            "temporalio.contrib.streams.MemoryStreams",
            with_owner_start_grace(
                StreamStoreConfig(memory=MemoryStoreConfig()), owner_start_grace
            ),
        )
