"""Streams in Workflow code.

The calls load :py:mod:`temporalio.common.streams` on first use, so a Workflow with no Stream
never loads it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from temporalio.common.streams import Creating, WorkflowStream

D = TypeVar("D")


def create_stream(
    definition: type[D],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> Creating[WorkflowStream[D]]:
    """Create a Stream that this run chain owns.

    Its id is ``<workflow id>:<first run id>:<name>``, so a client finds it from the Workflow
    handle with no call. The create is one Local Activity, and its error raises here. Only the
    run chain closes the Stream. The native server closes it when the run chain ends, with the
    result or the failure of its last run. On Redis, the end of the run chain closes nothing,
    so a Stream that nobody closes stays open until its ``safety_ttl``.

    Await the call for the handle, or use it in ``async with``, which closes the Stream at the
    end of the block::

        async with workflow.create_stream(streams.with_name(Events, "turn-3")) as turn:
            await turn.topic(Events.tokens).publish("hi")

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class. Its name is the Stream name. For one Stream
            per turn, for example, pass ``streams.with_name(Events, f"turn-{n}")``.
        retention: How long the Records stay after the close. Defaults to the backend's.
        backend: The name of the backend. Defaults to the registered backend with
            ``default=True``, else to ``native``. The default is part of the Workflow's code,
            since a replay must create on the same backend. To move a Workflow to another
            backend, gate it with a patch, for example
            ``backend="native" if workflow.patched("move") else "redis"``.

    Raises:
        StreamError: The backend refused the create, such as for another Retention.
        ReadOnlyContextError: The code must not change state, such as a query handler.
        StreamConfigError: The Worker has no backend of that name. It fails the Workflow
            Task, not the run.
    """
    from temporalio.common.streams import _workflow

    return _workflow.create_stream(definition, retention=retention, backend=backend)


def create_stream_nowait(
    definition: type[D],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> WorkflowStream[D]:
    """Create a Stream that this run chain owns, with no await.

    It is for sync Workflow code, such as ``@workflow.init``, as ``asyncio.Queue.put_nowait``
    is for code that can't await ``put``. It takes the options of :py:func:`create_stream`. A
    backend error fails the Workflow Task.

    .. warning::
        This API is experimental.
    """
    from temporalio.common.streams import _workflow

    return _workflow.create_stream_nowait(
        definition, retention=retention, backend=backend
    )


def stream_handle(
    definition: type[D], *, backend: str | None = None
) -> WorkflowStream[D]:
    """A Stream that this run chain created, by its definition. It makes no call.

    A later run of the chain, such as one after continue-as-new, uses it to write or close the
    Stream again.

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class, renamed as for the create.
        backend: The backend of the Stream. Defaults to the one that the run chain created it
            on, else to the registered backend with ``default=True``, else to ``native``.

    Raises:
        StreamConfigError: The Worker has no backend of that name.
    """
    from temporalio.common.streams import _workflow

    return _workflow.stream_handle(definition, backend=backend)
