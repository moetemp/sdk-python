"""Streams in Workflow code.

These functions import the Streams API of :py:mod:`temporalio.common` on first use, so a
Workflow with no Stream never imports it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from temporalio.common import Creating, WorkflowStream

D = TypeVar("D")


def create_stream(
    definition: type[D],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> Creating[WorkflowStream[D]]:
    """Create a Stream that this run chain owns.

    The Stream id is ``<workflow id>:<first run id>:<name>``, so a client can get it from the
    Workflow handle without a call. The create and each publish are Local Activities, and the
    create's error is raised here. Only the run chain can close the Stream. A close runs after
    its Workflow Task commits, as a regular Activity on Redis and as a System Nexus command on
    native. So a task that fails first leaves the Stream open for its retry. The native server
    also closes the Stream when the run chain ends, with the result or the failure of its last
    run. On Redis, the end of the run chain doesn't close a Stream outside an ``async with``
    block. So a Stream that nobody closes stays open until its ``safety_ttl``.

    Await the call to get the handle, or use it in ``async with``, which closes the Stream at
    the end of the block::

        async with workflow.create_stream(common.with_name(Events, "turn-3")) as turn:
            await turn.topic(Events.tokens).publish("hi")

    An exception in the block closes the Stream with that exception as the failure. If a retry
    run can come next, the close waits for the run's end. The Stream then stays open only for a
    retry run, or for the retry of a failed Workflow Task. A cron run leaves the Stream open.
    See :py:meth:`temporalio.common.WorkflowStream.__aexit__`.

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class. Its name is the Stream name. For one Stream
            per turn, for example, pass ``common.with_name(Events, f"turn-{n}")``.
        retention: The age after which a Record is deleted, even while the Stream is open.
            A closed Stream is kept for one Retention after its close. Defaults to the
            backend's Retention.
        backend: The name of the backend. Defaults to the registered backend with
            ``default=True``, or else to ``native``. The default is part of the Workflow's
            code, since a replay must create the Stream on the same backend. To switch a
            Workflow to another backend, gate the change with a patch, for example
            ``backend="native" if workflow.patched("move") else "redis"``.

    Raises:
        StreamError: The backend refused the create, such as for a different Retention. It's
            ``StreamClosedError`` or ``StreamExpiredError`` if a Stream with that id closed or
            expired.
        ReadOnlyContextError: The code must not change state, such as a query handler.
        StreamConfigError: The Worker has no backend with that name. It fails the Workflow
            Task, not the run.
    """
    from temporalio.common._streams import _workflow

    return _workflow.create_stream(definition, retention=retention, backend=backend)


def create_stream_nowait(
    definition: type[D],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> WorkflowStream[D]:
    """Create a Stream that this run chain owns, without an await.

    Use it in sync Workflow code, such as ``@workflow.init``. It's like
    ``asyncio.Queue.put_nowait`` for code that can't await ``put``. It takes the options of
    :py:func:`create_stream`. A backend error raises in the run as an error that the run
    doesn't catch, so a ``StreamError`` fails the run.

    .. warning::
        This API is experimental.
    """
    from temporalio.common._streams import _workflow

    return _workflow.create_stream_nowait(
        definition, retention=retention, backend=backend
    )


def stream_handle(
    definition: type[D], *, backend: str | None = None
) -> WorkflowStream[D]:
    """Get a Stream that this run chain created, by its definition. It makes no call.

    A later run of the chain, such as one after continue-as-new, uses it to write to or close
    the Stream. In ``@workflow.init`` of a run that continued as new, the run hasn't read the
    backends of the chain's Streams yet. So pass ``backend=`` there, or call it in the run
    method.

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class, with the same name as in the create.
        backend: The backend of the Stream. Defaults to the backend that the run chain created
            it on, or else to the registered backend with ``default=True``, or else to
            ``native``.

    Raises:
        StreamConfigError: The Worker has no backend with that name.
    """
    from temporalio.common._streams import _workflow

    return _workflow.stream_handle(definition, backend=backend)
