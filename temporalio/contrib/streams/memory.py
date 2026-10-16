"""Streams in this process's memory, for tests and local development."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio.bridge.proto.streams import MemoryStoreConfig, StreamStoreConfig
from temporalio.contrib.streams._capabilities import StoreCapabilities
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

    def __init__(
        self,
        *,
        owner_start_grace: timedelta | None = None,
        capabilities: StoreCapabilities | None = None,
    ) -> None:
        """Create the store's configuration.

        Args:
            owner_start_grace: How long a read of a Workflow that Temporal
                doesn't know yet answers empty before it fails with
                :class:`temporalio.contrib.streams.StreamNotFoundError`, so
                a reader can start before its owner. 60 seconds when unset.
                Zero fails at once.
            capabilities: A mask over the store's capabilities. A field set to
                ``False`` turns that capability off, and Core takes its
                fallback, as on a store that lacks it. For tests, and to try
                the weaker guarantees. Every capability when unset.

        Raises:
            ValueError: ``owner_start_grace`` is negative, or a mask was
                given to an SDK whose Core can't mask a store.
        """
        # Set only when asked for, so the config also fits a Core without masks.
        mask: dict[str, Any] = {}
        if capabilities is not None:
            mask["capabilities"] = capabilities._to_proto()
        memory = MemoryStoreConfig(**mask)
        super().__init__(
            "temporalio.contrib.streams.MemoryStreams",
            with_owner_start_grace(StreamStoreConfig(memory=memory), owner_start_grace),
        )
