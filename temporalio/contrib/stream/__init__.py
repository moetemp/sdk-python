"""The Redis Stream backend.

Register it on the client. Workflow code and clients create Streams with
:py:data:`temporalio.workflow.create_stream` and :py:attr:`temporalio.client.Client.create_stream`.
``backend=`` picks a backend that isn't the default::

    from temporalio.contrib.stream import RedisStreamBackend

    client = await Client.connect(
        "localhost:7233",
        stream_backends=[RedisStreamBackend("redis://localhost", default=True)],
    )

A Workflow's close is a regular Activity on the Workflow's task queue. So a Worker on that queue
that polls Activities must run the close, and must register the Redis backend. A Worker with
``no_remote_activities=True`` logs a warning, since the close waits for another Worker.

.. warning::
    This package is experimental.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta

from temporalio.streams._extension import ExtensionStreamBackend
from temporalio.streams.proto import v1 as ext_pb


class RedisStreamBackend(ExtensionStreamBackend):
    """Sends Stream calls to Redis, through the temporalio-streams runtime in the bridge.

    Register it on the client with ``stream_backends=``. A Worker uses its client's backends,
    so one backend serves Workflow code, Activities and the client. The clients of the backend
    that share a :py:class:`temporalio.runtime.Runtime` share one runtime object and its
    connections, and so do their Workers. It registers its ``name``, ``redis`` by default, and
    each Stream ref that it creates keeps that name. A Worker serves only one backend. A client
    can have more than one, and ``backend=`` picks one.

    A Workflow's close is a regular Activity on the Workflow's task queue, which runs after the
    Workflow Task commits. So a Worker on that queue that polls Activities must have this
    backend. A Worker with ``no_remote_activities=True`` logs a warning, since the close waits
    for another Worker.

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
            retention: The default Retention of a new Stream, which is how long the Stream
                stays readable after its close. Defaults to 7 days.
            safety_ttl: How long an open Stream is kept after its last write. Until then, it
                keeps every Record. Defaults to 90 days.
            response_timeout: The longest time that one Redis call can take. Defaults to 5 s.
            name: The backend name that Stream refs store, so every process can route a
                ref that it receives.
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
