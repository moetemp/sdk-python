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

    Register it on the client in a :py:class:`temporalio.streams.StreamConfig`, as with
    ``RedisStreamBackend``. Every Worker and client that shares one backend shares its Streams.
    The Streams are lost when the process ends. It registers its ``name``, ``memory`` by
    default. It has Redis's rules, so tests can swap the Store without changing Workflow code.
    As on Redis, a Workflow's close is a regular Activity on the Workflow's task queue, so a
    Worker on that queue that polls Activities must have the backend.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        *,
        retention: timedelta | None = None,
        last_write_ttl: timedelta | None = None,
        runtime: temporalio.runtime.Runtime | None = None,
        name: str = "memory",
    ) -> None:
        """Create the backend.

        Args:
            retention: The default Retention of a new Stream, which is how long the Stream
                stays readable after its close. Defaults to 7 days.
            last_write_ttl: How long an open Stream is kept after its last write. Unset, the
                default, keeps an open Stream and every Record until its close.
            runtime: The runtime of the backend's memory Store. Defaults to the default
                runtime.
            name: The backend name that Stream refs store.
        """
        config = runtime_pb.MemoryStreamConfig()
        if retention is not None:
            config.retention.FromTimedelta(retention)
        if last_write_ttl is not None:
            config.last_write_ttl.FromTimedelta(last_write_ttl)
        super().__init__("temporalio.MemoryStreamBackend", True, config, runtime, name)
