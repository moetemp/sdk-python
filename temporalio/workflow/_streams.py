"""Streams in Workflow code.

These functions import the Streams API of :py:mod:`temporalio.common` on first use, so a
Workflow with no Stream never imports it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from typing_extensions import Unpack

if TYPE_CHECKING:
    from temporalio.common import (
        Creating,
        StreamDefinition,
        StreamOptions,
        WorkflowStream,
    )

T = TypeVar("T")


def create_stream(
    definition: type[StreamDefinition[T]],
    *,
    backend: str | None = None,
    **options: Unpack[StreamOptions],
) -> Creating[WorkflowStream[T]]:
    """Create a Stream that this run chain owns, or get the one that exists.

    A Stream must exist before its first publish. The Stream id is
    ``<workflow id>:<first run id>:<name>``, with each part percent-encoded, so a create is
    create-or-get by that id. On a Stream that exists, it ignores ``options`` and returns that
    Stream. So a Workflow Task retry, a retry run and the run after continue-as-new all get the
    same Stream. The create and each publish are Local Activities, and the create's error is
    raised here. Only the run chain can close the Stream. A close runs after its Workflow Task
    commits, as a regular Activity on Redis, so a task that fails first leaves the Stream open
    for its retry. On Redis, the end of the run chain doesn't close a Stream outside an
    ``async with`` block. So a Stream that nobody closes stays open until its ``safety_ttl``.

    Await the call to get the handle, or use it in ``async with``, which closes the Stream at
    the end of the block::

        async with workflow.create_stream(Tokens) as tokens:
            await tokens.publish("hi")

    An exception in the block closes the Stream with that exception as the failure. If a retry
    run can come next, the close waits for the run's end. The Stream then stays open only for a
    retry run, or for the retry of a failed Workflow Task. A cron run leaves the Stream open.
    See :py:meth:`temporalio.common.WorkflowStream.__aexit__`.

    .. warning::
        This API is experimental.

    Args:
        definition: The Stream definition class. Its name is the Stream name.
        backend: The name of the backend. Defaults to the registered backend with
            ``default=True``. The default is part of the Workflow's code, since a replay must
            create the Stream on the same backend. A Worker serves only one backend, so the
            name must be the Worker's backend name.
        options: The options of a new Stream, such as ``retention``. See
            :py:class:`temporalio.common.StreamOptions`.

    Raises:
        StreamError: The backend refused the create. It's ``StreamClosedError`` or
            ``StreamExpiredError`` if a Stream with that id closed or expired.
        ReadOnlyContextError: The code must not change state, such as a query handler.
        StreamConfigError: The Worker has no Stream backend or none with that name, or no
            backend has ``default=True`` and the call gives no backend. It fails the Workflow
            Task, not the run.
    """
    from temporalio.common._streams import _workflow

    return _workflow.create_stream(definition, backend=backend, options=options)


def create_stream_nowait(
    definition: type[StreamDefinition[T]],
    *,
    backend: str | None = None,
    **options: Unpack[StreamOptions],
) -> WorkflowStream[T]:
    """Create a Stream that this run chain owns, or get it, without an await.

    Use it in sync Workflow code, such as ``@workflow.init``. It's like
    ``asyncio.Queue.put_nowait`` for code that can't await ``put``. It takes the arguments of
    :py:func:`create_stream`. A backend error raises in the run as an error that the run
    doesn't catch, so a ``StreamError`` fails the run.

    .. warning::
        This API is experimental.
    """
    from temporalio.common._streams import _workflow

    return _workflow.create_stream_nowait(definition, backend=backend, options=options)


def stream_handle(
    definition: type[StreamDefinition[T]], *, backend: str | None = None
) -> WorkflowStream[T]:
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
            it on, or else to the registered backend with ``default=True``.

    Raises:
        StreamConfigError: No backend is given, known or marked as the default.
    """
    from temporalio.common._streams import _workflow

    return _workflow.stream_handle(definition, backend=backend)
