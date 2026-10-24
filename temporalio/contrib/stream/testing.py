"""A Stream backend for tests, with no Redis.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.runtime
from temporalio.common.streams._extension import ExtensionStreamBackend
from temporalio.common.streams.proto import v1 as ext_pb


class MemoryStreamBackend(ExtensionStreamBackend):
    """Keeps Streams in this process, with the same rules as on Redis, for tests.

    Register it on the client, as with ``RedisStreamBackend``. Every Worker and client that
    shares one backend shares its Streams, and they go when the process ends. It registers the
    backend name ``memory``. It keeps Redis's rules, so the calls of
    :py:mod:`temporalio.contrib.stream` take it as a Redis backend, and tests swap the Store
    with no change to Workflow code.

    .. warning::
        This API is experimental.
    """

    _kind = "redis"

    def __init__(
        self,
        *,
        retention: timedelta | None = None,
        safety_ttl: timedelta | None = None,
        runtime: temporalio.runtime.Runtime | None = None,
        name: str = "memory",
        default: bool = False,
    ) -> None:
        """Create the backend.

        Args:
            retention: The default Retention of a new Stream. Defaults to 7 days.
            safety_ttl: How long an open Workflow-owned Stream stays after its last write.
                Defaults to 90 days.
            runtime: The runtime of the backend's memory Store. Defaults to the default
                runtime.
            name: The backend name that Stream handles carry.
            default: Make this the backend of a create with no backend name. With more than
                one registered backend, exactly one has it.
        """
        config = ext_pb.RedisStreamConfig()
        if retention is not None:
            config.retention.FromTimedelta(retention)
        if safety_ttl is not None:
            config.safety_ttl.FromTimedelta(safety_ttl)
        super().__init__(
            "temporalio.MemoryStreamBackend", True, config, runtime, name, default
        )
