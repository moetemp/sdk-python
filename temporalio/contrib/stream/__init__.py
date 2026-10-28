"""The Redis Stream backend.

Register it on the client. Workflow code and clients create Streams with
:py:func:`temporalio.workflow.create_stream` and :py:meth:`temporalio.client.Client.create_stream`.
``backend=`` picks a backend that isn't the default::

    from temporalio.contrib.stream import RedisStreamBackend

    client = await Client.connect(
        "localhost:7233",
        stream_backends=[RedisStreamBackend("redis://localhost", default=True)],
    )

A Workflow's close is a regular Activity on the Workflow's task queue. So a Worker with a Redis
backend must poll Activities on its task queue for the close Activity. All Workers that share a
task queue must register the Redis backend.

.. warning::
    This package is experimental.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

from temporalio.common._streams._extension import ExtensionStreamBackend
from temporalio.common._streams.proto import v1 as ext_pb


class RedisStreamBackend(ExtensionStreamBackend):
    """Sends Stream calls to Redis, through the temporalio-streams runtime in the bridge.

    Register it on the client with ``stream_backends=``. A Worker uses its client's backends,
    so one backend serves Workflow code, Activities and the client. The clients of the backend
    that share a :py:class:`temporalio.runtime.Runtime` share one runtime object and its
    connections, and so do their Workers. It registers the backend name ``redis``, and each
    Stream handle that it creates stores that name. A Worker serves only one backend. A client
    can have more than one, and ``backend=`` picks one.

    A Workflow's close is a regular Activity on the Workflow's task queue, which runs after the
    Workflow Task commits. So a Worker with this backend must poll Activities on its task queue,
    and all Workers that share a task queue must register the backend.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        url: str | Sequence[str],
        *,
        cluster: bool = False,
        key_prefix: str | None = None,
        retention: timedelta | None = None,
        safety_ttl: timedelta | None = None,
        response_timeout: timedelta | None = None,
        name: str = "redis",
        default: bool = False,
    ) -> None:
        """Create the backend.

        The backend connects to Redis at the first Stream call on a runtime, and the clients of
        that runtime share the connection.

        Args:
            url: A Redis URL, or the seed URLs of a cluster. Credentials and TLS come from the
                URL.
            cluster: Whether the URLs are of a Redis cluster.
            key_prefix: The prefix of every key. Defaults to ``temporal-streams``.
            retention: The default Retention of a new Stream. Defaults to 7 days.
            safety_ttl: How long an open Workflow-owned Stream is kept after its last write.
                Defaults to 90 days.
            response_timeout: The longest time that one Redis call can take. Defaults to 5 s.
            name: The backend name that Stream handles store, so every process can route a
                handle that it receives.
            default: Make this the backend of a create with no backend name. With more than
                one registered backend, exactly one must have it, or the client raises
                ``StreamConfigError``.
        """
        config = ext_pb.RedisStreamConfig(
            urls=[url] if isinstance(url, str) else list(url),
            cluster=cluster,
            key_prefix=key_prefix or "",
        )
        for field, value in (
            ("retention", retention),
            ("safety_ttl", safety_ttl),
            ("response_timeout", response_timeout),
        ):
            if value is not None:
                getattr(config, field).FromTimedelta(value)
        super().__init__(
            "temporalio.RedisStreamBackend", False, config, None, name, default
        )


__all__ = ["RedisStreamBackend"]
