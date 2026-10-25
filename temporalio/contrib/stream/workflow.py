"""Streams in Workflow code on the registered Redis backend.

These calls work as :py:func:`temporalio.workflow.create_stream`,
:py:func:`temporalio.workflow.create_stream_nowait` and
:py:func:`temporalio.workflow.stream_handle`, with the Worker's Redis backend in place of the
default one. A ``MemoryStreamBackend`` keeps Redis's rules, so tests use it here with no change
to Workflow code.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TypeVar

from temporalio.common.streams import _workflow
from temporalio.common.streams._errors import StreamConfigError
from temporalio.common.streams._handle import Creating
from temporalio.common.streams._workflow import WorkflowStream

D = TypeVar("D")


def _backend(backend: str | None) -> str:
    # The Worker's config gives this error, so it fails the Workflow Task and not the run.
    worker = _workflow.worker_backend()
    if worker is None:
        raise _workflow.no_worker_backend()
    if worker.kind != "redis":
        raise StreamConfigError(
            f"The Worker's Stream backend {worker.name} is not a Redis backend"
        )
    return backend or worker.name


def create_stream(
    definition: type[D],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> Creating[WorkflowStream[D]]:
    """Create a Stream that this run chain owns, on the Worker's Redis backend.

    See :py:func:`temporalio.workflow.create_stream`. On Redis, the end of the run chain closes
    nothing, so close the Stream before the run returns.

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class. Its name is the Stream name.
        retention: The age after which a Record is dropped, also while the Stream is open.
            A closed Stream stays one Retention after its close. Defaults to the backend's.
        backend: The name of the Redis backend. Defaults to the Worker's.

    Raises:
        StreamError: The backend refused the create, such as for another Retention.
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
    """Create a Stream on the Worker's Redis backend, with no await.

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
    """A Stream that this run chain created on the Worker's Redis backend. It makes no call.

    See :py:func:`temporalio.workflow.stream_handle`.

    .. warning::
        This API is experimental.
    """
    return _workflow.stream_handle(definition, backend=_backend(backend))
