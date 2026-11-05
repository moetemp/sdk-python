"""A Stream backend for tests, with no Redis.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.bridge.proto.streams as runtime_pb
import temporalio.runtime
from temporalio.streams._extension import ExtensionStreamBackend


class MemoryStreamBackend(ExtensionStreamBackend):
    """Keeps Streams in this process, with the same rules as Redis, for tests.

    Register it on the client, as with ``RedisStreamBackend``. Every Worker and client that
    shares one backend shares its Streams. The Streams are lost when the process ends. It
    registers its ``name``, ``memory`` by default. It has Redis's rules, so tests can swap the
    Store without changing Workflow code.

    .. warning::
        This API is experimental.
    """

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
            safety_ttl: How long an open Stream bound to a Workflow is kept after its last
                write. Defaults to 90 days. A standalone Stream, the only kind that this SDK
                creates, expires one Retention after its last write instead.
            runtime: The runtime of the backend's memory Store. Defaults to the default
                runtime.
            name: The backend name that Stream refs store.
            default: Make this the backend of a create with no backend name. With more than
                one registered backend, exactly one must have it.
        """
        config = runtime_pb.MemoryStreamConfig()
        if retention is not None:
            config.retention.FromTimedelta(retention)
        if safety_ttl is not None:
            config.safety_ttl.FromTimedelta(safety_ttl)
        super().__init__(
            "temporalio.MemoryStreamBackend", True, config, runtime, name, default
        )
