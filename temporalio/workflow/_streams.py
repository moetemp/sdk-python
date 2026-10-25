"""Streams in Workflow code.

The calls load the Streams API of :py:mod:`temporalio.common` on first use, so a Workflow with
no Stream never loads it.
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

    Its id is ``<workflow id>:<first run id>:<name>``, so a client finds it from the Workflow
    handle with no call. The create and each publish are Local Activities, and the create's
    error raises here. Only the run chain closes the Stream. A close runs once its Workflow
    Task commits, as a regular Activity on Redis, so a task that fails before then leaves the
    Stream open for its retry. On Redis, the end of the run chain closes nothing, so a Stream
    that nobody closes stays open until its ``safety_ttl``.

    Await the call for the handle, or use it in ``async with``, which closes the Stream at the
    end of the block::

        async with workflow.create_stream(common.with_name(Events, "turn-3")) as turn:
            await turn.topic(Events.tokens).publish("hi")

    An exception closes the Stream with that exception as the failure. When a retry run may
    follow, the close waits for the run's end, and the Stream stays open only for a retry run
    or for the retry of a Workflow Task that failed. A cron run leaves the Stream open. See
    :py:meth:`temporalio.common.WorkflowStream.__aexit__`.

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class. Its name is the Stream name. For one Stream
            per turn, for example, pass ``common.with_name(Events, f"turn-{n}")``.
        retention: The age after which a Record is dropped, also while the Stream is open.
            A closed Stream stays one Retention after its close. Defaults to the backend's.
        backend: The name of the backend. Defaults to the registered backend with
            ``default=True``. The default is part of the Workflow's code, since a replay must
            create on the same backend. For now a Worker serves one backend, so the name is
            that of the Worker's backend.

    Raises:
        StreamError: The backend refused the create, such as for another Retention, or
            ``StreamClosedError`` and ``StreamExpiredError`` for a Stream of that id that
            closed or expired.
        ReadOnlyContextError: The code must not change state, such as a query handler.
        StreamConfigError: No Stream backend is on the Worker, none of that name, or none
            has ``default=True`` for a create with no backend. It fails the Workflow Task,
            not the run.
    """
    from temporalio.common._streams import _workflow

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
    from temporalio.common._streams import _workflow

    return _workflow.create_stream_nowait(
        definition, retention=retention, backend=backend
    )


def stream_handle(
    definition: type[D], *, backend: str | None = None
) -> WorkflowStream[D]:
    """A Stream that this run chain created, by its definition. It makes no call.

    A later run of the chain, such as one after continue-as-new, uses it to write or close the
    Stream again. In ``@workflow.init`` of a run that continued as new, the run has not read
    the backends of the chain's Streams yet, so pass ``backend=`` there, or call it in the run
    method.

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class, renamed as for the create.
        backend: The backend of the Stream. Defaults to the one that the run chain created it
            on, and else to the registered backend with ``default=True``.

    Raises:
        StreamConfigError: No backend is given, known or marked as the default.
    """
    from temporalio.common._streams import _workflow

    return _workflow.stream_handle(definition, backend=backend)
