"""Streams in Workflow code on the registered Redis backend.

These functions work like :py:func:`temporalio.workflow.create_stream`,
:py:func:`temporalio.workflow.create_stream_nowait` and
:py:func:`temporalio.workflow.stream_handle`, but use the Worker's Redis backend in place of the
default one. A ``MemoryStreamBackend`` has Redis's rules, so tests can use it here without
changing Workflow code.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TypeVar

from temporalio.common._streams import _workflow
from temporalio.common._streams._errors import StreamConfigError
from temporalio.common._streams._handle import Creating
from temporalio.common._streams._workflow import WorkflowStream

D = TypeVar("D")


def _backend(backend: str | None) -> str:
    # These errors come from the Worker's config, so they fail the Workflow Task and not the run.
    redis = [b for b in _workflow.worker_backends() if b.kind == "redis"]
    if backend:
        if not any(b.name == backend for b in redis):
            raise StreamConfigError(f"The Worker has no Redis Stream backend {backend}")
        return backend
    if not redis:
        raise StreamConfigError(
            "The Worker has no Redis Stream backend. Register a RedisStreamBackend on its "
            "client with stream_backends=."
        )
    defaults = [b for b in redis if b.default]
    if len(redis) > 1 and len(defaults) != 1:
        raise StreamConfigError(
            "The Worker has more than one Redis Stream backend, so pass backend="
        )
    return (defaults if len(redis) > 1 else redis)[0].name


def create_stream(
    definition: type[D],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> Creating[WorkflowStream[D]]:
    """Create a Stream that this run chain owns, on the Worker's Redis backend.

    See :py:func:`temporalio.workflow.create_stream`. On Redis, the end of the run chain doesn't
    close a Stream outside an ``async with`` block, so close the Stream before the run returns.

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class. Its name is the Stream name.
        retention: The age after which a Record is deleted, even while the Stream is open.
            A closed Stream is kept for one Retention after its close. Defaults to the
            backend's Retention.
        backend: The name of the Redis backend. Defaults to the Worker's only Redis
            backend, or the one with ``default=True``.

    Raises:
        StreamError: The backend refused the create, such as for a different Retention.
        StreamConfigError: The Worker has no Redis backend, or none of that name. It fails
            the Workflow Task, not the run.
    """

    async def create() -> WorkflowStream[D]:
        return await _workflow.create_stream(
            definition, retention=retention, backend=_backend(backend)
        )

    return Creating(create)


def create_stream_nowait(
    definition: type[D],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> WorkflowStream[D]:
    """Create a Stream on the Worker's Redis backend, without an await.

    See :py:func:`temporalio.workflow.create_stream_nowait`.

    .. warning::
        This API is experimental.
    """
    return _workflow.create_stream_nowait(
        definition, retention=retention, backend=_backend(backend)
    )


def stream_handle(
    definition: type[D], *, backend: str | None = None
) -> WorkflowStream[D]:
    """Get a Stream that this run chain created on the Worker's Redis backend. It makes no call.

    See :py:func:`temporalio.workflow.stream_handle`.

    .. warning::
        This API is experimental.
    """
    return _workflow.stream_handle(definition, backend=_backend(backend))
