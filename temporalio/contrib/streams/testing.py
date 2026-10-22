"""A Stream plugin for tests, with no Redis.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.runtime
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import StreamPlugin

MEMORY_EXTENSION_NAME = "temporalio-streams-memory"


class MemoryStreamPlugin(StreamPlugin):
    """Keeps Streams in this process, with the same rules as on Redis, for tests.

    Put it on the client, as with ``RedisStreamPlugin``. Every Worker and client that shares one
    plugin shares its Streams, and they go when the process ends. It registers the backend
    name ``memory``, so tests swap the Store with no change to Workflow code.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        *,
        retention: timedelta | None = None,
        safety_ttl: timedelta | None = None,
        durable_writes: int = 0,
        runtime: temporalio.runtime.Runtime | None = None,
        name: str = "memory",
        default: bool = False,
    ) -> None:
        """Create the plugin.

        Args:
            retention: The default Retention of a new Stream. Defaults to 7 days.
            safety_ttl: How long an open Workflow-owned Stream stays after its last write.
                Defaults to 90 days.
            durable_writes: When above zero, each Workflow flush waits for this many
                replicas. The memory Store has none, so each Workflow flush fails with the
                missing ``replica_wait`` capability. It is for tests of that failure.
            runtime: The runtime of the client. Defaults to the default runtime.
            name: The backend name that Stream handles carry.
            default: Make this the client's default backend when the client has more than
                one Stream plugin.
        """
        config = ext_pb.RedisStreamConfig(durable_writes=durable_writes)
        if retention is not None:
            config.retention.FromTimedelta(retention)
        if safety_ttl is not None:
            config.safety_ttl.FromTimedelta(safety_ttl)
        super().__init__(
            "temporalio.MemoryStreamPlugin",
            MEMORY_EXTENSION_NAME,
            config,
            runtime,
            name,
            default,
        )
