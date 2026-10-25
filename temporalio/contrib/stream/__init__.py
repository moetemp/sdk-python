"""The Redis Stream backend.

Register it on the client. :py:func:`create` creates a Stream that the client owns.
:py:mod:`temporalio.contrib.stream.workflow` has the Workflow functions that use this backend::

    from temporalio.contrib import stream as redis

    client = await Client.connect(
        "localhost:7233", stream_backends=[redis.RedisStreamBackend("redis://localhost")]
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
from typing import TypeVar

import temporalio.client
from temporalio.common._streams import _plugin, _stream_client
from temporalio.common._streams._errors import StreamConfigError
from temporalio.common._streams._extension import ExtensionStreamBackend
from temporalio.common._streams._handle import Creating
from temporalio.common._streams._stream_client import ClientStream
from temporalio.common._streams.proto import v1 as ext_pb
from temporalio.contrib.stream import workflow

D = TypeVar("D")


class RedisStreamBackend(ExtensionStreamBackend):
    """Sends Stream calls to Redis, through the temporalio-streams runtime in the bridge.

    Register it on the client with ``stream_backends=``. A Worker uses its client's backends,
    so one backend serves Workflow code, Activities and the client. Each client of the backend
    gets its own runtime object, and the client's Workers share it. It registers the backend
    name ``redis``, and each Stream handle that it creates stores that name. A Worker serves
    only one backend. A client can have more than one, and ``backend=`` picks one.

    A Workflow's close is a regular Activity on the Workflow's task queue, which runs after the
    Workflow Task commits. So a Worker with this backend must poll Activities on its task queue,
    and all Workers that share a task queue must register the backend.

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
        """Create the backend.

        Each client of the backend connects to Redis on its first Stream call, on the client's
        runtime.

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


def create(
    client: temporalio.client.Client,
    definition: type[D],
    *,
    id: str,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> Creating[ClientStream[D]]:
    """Create a Stream that the client owns, so only a client can close it.

    The call is idempotent for the same id and Retention. Await the call to get the handle, or
    use it in ``async with``, which closes the Stream at the end of the block::

        async with redis.create(client, Events, id="chat-1") as chat:
            await chat.topic(Events.turns).publish(Turn("user", "hi"))

    .. warning::
        This API is experimental.

    Args:
        client: The client, with a registered Redis backend.
        definition: The Stream definition class.
        id: The Stream id, which the caller picks, as for a Workflow id.
        retention: The age after which a Record is deleted, even while the Stream is open.
            A closed Stream is kept for one Retention after its close. Defaults to the
            backend's Retention.
        backend: The name of the Redis backend. Defaults to the client's only Redis backend,
            or the one with ``default=True``.

    Raises:
        StreamConfigError: The client has no Redis backend of that name. No call is made.
        StreamClosedError: A Stream of that id was created and closed before.
        StreamExpiredError: A Stream of that id expired.
        StreamError: The Stream exists with a different Retention, with the reason
            ``stream_already_exists``.
    """

    async def create() -> ClientStream[D]:
        name = _redis_backend(_plugin.client_backends(client), backend)
        return await _stream_client.create(
            client, definition, id=id, retention=retention, backend=name
        )

    return Creating(create)


def _redis_backend(backends: Sequence[_plugin.StreamBackend], name: str | None) -> str:
    # The memory backend has Redis's rules, so tests can use it in place of Redis.
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
