"""The Redis Stream backend.

Register it on the client. :py:func:`create` makes a Stream that the client owns, and
:py:mod:`temporalio.contrib.stream.workflow` has the Workflow calls that pick this backend::

    from temporalio.contrib import stream as redis

    client = await Client.connect(
        "localhost:7233", stream_backends=[redis.RedisStreamBackend("redis://localhost")]
    )

.. warning::
    This package is experimental.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TypeVar

import temporalio.client
from temporalio.common.streams import _plugin, _stream_client
from temporalio.common.streams._errors import StreamConfigError
from temporalio.common.streams._extension import ExtensionStreamBackend
from temporalio.common.streams._handle import Creating
from temporalio.common.streams._stream_client import ClientStream
from temporalio.common.streams.proto import v1 as ext_pb
from temporalio.contrib.stream import workflow

D = TypeVar("D")


class RedisStreamBackend(ExtensionStreamBackend):
    """Carries Stream calls to Redis, through the temporalio-streams runtime in the bridge.

    Register it on the client with ``stream_backends=``. A Worker takes its client's backends,
    so one backend serves Workflow code, Activities and the client. Each client of the backend
    gets its own runtime object, and the client's Workers share it. It registers the backend
    name ``redis``, which the Stream handles it creates carry. For now a Worker serves one
    backend. A client may have more than one, and ``backend=`` picks one.

    .. warning::
        This API is experimental.
    """

    _kind = "redis"

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
        """Create the backend. Each client of the backend connects to Redis on its first
        Stream call, on the client's runtime.

        Args:
            url: A Redis URL, or the seed URLs of a cluster. Credentials and TLS come from the
                URL.
            cluster: Whether the URLs are of a Redis cluster.
            key_prefix: The prefix of every key. Defaults to ``temporal-streams``.
            retention: The default Retention of a new Stream. Defaults to 7 days.
            safety_ttl: How long an open Workflow-owned Stream stays after its last write.
                Defaults to 90 days.
            response_timeout: How long one Redis call takes at most. Defaults to 5 s.
            name: The backend name that Stream handles carry, so every process routes a
                handle it receives.
            default: Make this the backend of a create with no backend name. With more than
                one registered backend, exactly one has it, else the client raises
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


def create(
    client: temporalio.client.Client,
    definition: type[D],
    *,
    id: str,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> Creating[ClientStream[D]]:
    """Create a Stream that this client owns, so only a client closes it.

    It is idempotent for one id and one Retention. Await the call for the handle, or use it in
    ``async with``, which closes the Stream at the end of the block::

        async with redis.create(client, Events, id="chat-1") as chat:
            await chat.topic(Events.turns).publish(Turn("user", "hi"))

    .. warning::
        This API is experimental.

    Args:
        client: The client, with a registered Redis backend.
        definition: The Stream definition class.
        id: The Stream id, as a Workflow takes a Workflow id.
        retention: How long the Records stay after the close. Defaults to the backend's.
        backend: The name of the Redis backend. Defaults to the client's only Redis backend,
            or the one with ``default=True``.

    Raises:
        StreamConfigError: The client has no Redis backend of that name. No call is made.
        StreamClosedError: A Stream of that id was created and closed before.
        StreamError: The Stream exists with another Retention, with the reason
            ``stream_already_exists``.
    """

    async def create() -> ClientStream[D]:
        name = _redis_backend(_plugin.client_backends(client), backend)
        return await _stream_client.create(
            client, definition, id=id, retention=retention, backend=name
        )

    return Creating(create)


def _redis_backend(backends: Sequence[_plugin.StreamBackend], name: str | None) -> str:
    # The memory backend keeps Redis's rules, so it stands in for Redis in tests.
    redis = [backend for backend in backends if backend._kind == "redis"]
    if name:
        if not any(backend.backend_name == name for backend in redis):
            raise StreamConfigError(f"The client has no Redis Stream backend {name}")
        return name
    if not redis:
        raise StreamConfigError(
            "The client has no Redis Stream backend. Register a RedisStreamBackend with "
            "stream_backends=."
        )
    defaults = [backend for backend in redis if backend.default]
    if len(redis) > 1 and len(defaults) != 1:
        raise StreamConfigError(
            "The client has more than one Redis Stream backend, so pass backend="
        )
    return (defaults if len(redis) > 1 else redis)[0].backend_name


__all__ = ["RedisStreamBackend", "create", "workflow"]
