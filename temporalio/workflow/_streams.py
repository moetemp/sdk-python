"""Streams in Workflow code.

These calls import the Streams API of :py:mod:`temporalio.streams` on first use, so a Workflow
with no Stream never imports it.
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

from typing_extensions import Self

if TYPE_CHECKING:
    import temporalio.api.stream.v1
    from temporalio.streams import StreamIDConflictPolicy, StreamOffset, StreamRef

T = TypeVar("T")


class WorkflowStream(Generic[T]):
    """A Stream in Workflow code, with its calls. ``T`` is the payload type.

    Get one with :py:func:`create_stream` or :py:func:`get_stream` for a ref that came as data.
    Its :py:attr:`ref` is the data to pass on, such as to an Activity, a child Workflow or the
    next run of continue-as-new, or through a query.

    Workflow code writes to and closes a Stream, and can't read it. Each append is a Local
    Activity, and each close a command that the backend runs after the Workflow Task commits.

    .. warning::
        This API is experimental.
    """

    def __init__(self, ref: StreamRef[T]) -> None:
        """Create a Stream. Use ``create_stream`` or ``get_stream`` instead."""
        self._ref = ref

    @property
    def ref(self) -> StreamRef[T]:
        """The data that identifies the Stream, to pass on."""
        return self._ref

    @property
    def proto_ref(self) -> temporalio.api.stream.v1.StreamRef:
        """The ``StreamRef`` wire message of the Stream, for a field of a proto message."""
        from temporalio.streams import _ref

        return _ref.to_proto(self._ref)

    def append(self, *values: T) -> asyncio.Future[StreamOffset]:
        """Start the write of one Record for each value, in order, and return its future.

        The write has its place in the Stream's order when the call returns, also when no
        caller awaits the future. The writes to one Stream keep the order of the calls. A cancel
        of the future doesn't stop the write. A list is one value.

        The write is a Local Activity, so History has its result and not the values. Appends
        that wait behind a write to the same Stream are sent as one Local Activity, and each
        call gets the offset of its own last Record. A caller that awaits the future, or calls
        its ``result()`` or ``exception()``, gets the write's error and owns it. An error that
        no caller gets raises at the end of the run, as an error of :py:meth:`append_nowait`
        does. So a ``StreamError`` fails the run. The Workflow's logger logs the error when the
        write fails, if no caller waits for it then.

        Args:
            values: One value for each Record.

        Returns:
            The future of the write. It gives the offset of the last Record, or raises
            ``StreamError`` when the backend refused the write, such as ``reason``
            ``STREAM_ERROR_REASON_NOT_FOUND`` for a Stream that nobody created.

        Raises:
            StreamError: With ``reason`` ``STREAM_ERROR_REASON_CLOSED``, this run closed the
                Stream.
            StreamConfigError: The Worker has no Stream backend or not the Stream's backend,
                which fails the Workflow Task and not the run.
            temporalio.workflow.ReadOnlyContextError: The Workflow code must not change state,
                such as a query handler.
        """
        from temporalio.streams import _workflow

        return _workflow.append(self._ref, values)

    def append_nowait(self, *values: T) -> None:
        """Write one Record for each value, in order, without an await.

        Use it in sync Workflow code, such as ``@workflow.init`` and sync signal and update
        handlers. It's like ``asyncio.Queue.put_nowait`` for code that can't await ``put``. A
        backend error raises in the run as an error that the run doesn't catch, so a
        ``StreamError`` fails the run.
        """
        from temporalio.streams import _workflow

        _workflow.append_nowait(self._ref, values)

    async def close(self) -> None:
        """Close the Stream after the writes before it, once the Workflow Task commits.

        A close carries no value, so code with a last value appends it first. Readers get the
        Records before the close, then their read ends. Later writes in the run raise a
        :py:class:`temporalio.streams.StreamError` with ``reason`` ``STREAM_ERROR_REASON_CLOSED``.

        If the task fails first, or the Worker crashes, the Stream stays open, and the task's
        retry writes to it again. On Redis the close is a regular Activity on the Workflow's
        task queue, which closes the Stream as the run's Workflow Producer. So every Worker on
        that queue must register the Redis backend. A Worker that polls Activities runs the
        close, and a Worker with ``no_remote_activities=True`` doesn't.

        Raises:
            StreamError: The Stream is bound to another run chain, which alone closes it. It
                raises with no call.
        """
        from temporalio.streams import _workflow

        await _workflow.close(self._ref)

    def close_nowait(self) -> None:
        """Close the Stream without an await, in sync Workflow code.

        It is to :py:meth:`close` what ``asyncio.Queue.put_nowait`` is to ``put``. It starts the
        close, and the end of the run waits for it. A backend error raises in the run as an
        error that the run doesn't catch, so a ``StreamError`` fails the run.
        """
        from temporalio.streams import _workflow

        _workflow.close_nowait(self._ref)

    async def __aenter__(self) -> Self:
        """Use the Stream in ``async with``, which closes it at the end."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream at the end of the block.

        If this run already closed the Stream, this does nothing. A cancel of the block or of the
        run closes the Stream. An exception that fails the run with no retry run closes it too.
        Examples are an exception in a run with no retry policy, a non-retryable
        ``ApplicationError``, and an exception on the last attempt. For any other exception, the
        close waits for the run's end. The Stream then closes if the run returns, continues as new,
        or fails with no retry run. It stays open if a retry run comes next, since that run writes
        to the same Stream. It also stays open if the exception fails only the Workflow Task, since
        the task's retry runs the block again. In a cron run, the block end leaves the Stream open,
        since the Stream spans the cron chain. An eviction cancels the block, and the Worker sends
        no commands for an evicted run. An explicit :py:meth:`close` always closes the Stream.
        """
        from temporalio.streams import _workflow

        await _workflow.exit_block(self._ref, exc)

    def __repr__(self) -> str:
        """Show the ref."""
        return f"{type(self).__name__}({self._ref!r})"


@overload
async def create_stream(
    id: str,
    *,
    payload_type: type[T],
    backend: str | None = None,
    id_conflict_policy: StreamIDConflictPolicy | None = None,
) -> WorkflowStream[T]: ...


@overload
async def create_stream(
    id: str,
    *,
    payload_type: None = None,
    backend: str | None = None,
    id_conflict_policy: StreamIDConflictPolicy | None = None,
) -> WorkflowStream[Any]: ...


async def create_stream(
    id: str,
    *,
    payload_type: type[T] | None = None,
    backend: str | None = None,
    id_conflict_policy: StreamIDConflictPolicy | None = None,
) -> WorkflowStream[Any]:
    """Create a Stream, and return it.

    Give the payload type with ``payload_type=``::

        turns = await workflow.create_stream(f"{session_id}-turns", payload_type=Turn)

    A Stream must exist before its first append. A Workflow creates only standalone Streams, under
    an id of its choice, and the Stream doesn't end with the run. Each run of a chain sends the
    same create for an id, so a retry run or a cron run that creates the Stream of an earlier run
    gets that Stream. A run after continue-as-new uses the Stream through the ref that the earlier
    run forwards, with :py:func:`get_stream`. The create and each append are Local Activities.

    Args:
        id: The Stream id, which the Workflow picks, such as a session id.
        payload_type: The type that reads decode each Record with.
        backend: The name of the backend. Defaults to the Worker's backend. A replay must create
            the Stream on the same backend, so keep the Worker's default, or gate a new one with
            ``workflow.patched``.
        id_conflict_policy: ``FAIL`` raises a :py:class:`StreamError` with ``reason``
            ``STREAM_ERROR_REASON_ALREADY_EXISTS`` on an id that exists, which no earlier create
            of this run chain made. ``USE_EXISTING`` returns the existing Stream.

    Raises:
        StreamError: ``FAIL`` on an id that exists, with ``reason``
            ``STREAM_ERROR_REASON_ALREADY_EXISTS``.
        ReadOnlyContextError: The code must not change state, such as a query handler.
        StreamConfigError: The Worker has no Stream backend or none with that name. It fails the
            Workflow Task, not the run.

    .. warning::
        This API is experimental.
    """
    from temporalio.streams import StreamIDConflictPolicy, _workflow

    return await _workflow.create_stream(
        payload_type,
        id,
        backend=backend,
        id_conflict_policy=id_conflict_policy
        if id_conflict_policy is not None
        else StreamIDConflictPolicy.FAIL,
    )


@overload
def create_stream_nowait(
    id: str,
    *,
    payload_type: type[T],
    backend: str | None = None,
    id_conflict_policy: StreamIDConflictPolicy | None = None,
) -> WorkflowStream[T]: ...


@overload
def create_stream_nowait(
    id: str,
    *,
    payload_type: None = None,
    backend: str | None = None,
    id_conflict_policy: StreamIDConflictPolicy | None = None,
) -> WorkflowStream[Any]: ...


def create_stream_nowait(
    id: str,
    *,
    payload_type: type[T] | None = None,
    backend: str | None = None,
    id_conflict_policy: StreamIDConflictPolicy | None = None,
) -> WorkflowStream[Any]:
    """Start the create, and return the Stream at once.

    Use it in sync Workflow code, such as ``@workflow.init``. It's like ``asyncio.Queue.put_nowait``
    for code that can't await ``put``. It takes the arguments of :py:func:`create_stream`. A
    backend error raises in the run as an error that the run doesn't catch, so a ``StreamError``
    fails the run.

    .. warning::
        This API is experimental.
    """
    from temporalio.streams import StreamIDConflictPolicy, _workflow

    return _workflow.create_stream_nowait(
        payload_type,
        id,
        backend=backend,
        id_conflict_policy=id_conflict_policy
        if id_conflict_policy is not None
        else StreamIDConflictPolicy.FAIL,
    )


def get_stream(ref: StreamRef[T]) -> WorkflowStream[T]:
    """Get the Stream of a ref that came as data, such as an argument. It makes no call.

    The Stream's calls check its backend. A Worker that doesn't have it fails the Workflow Task
    at the first call.

    .. warning::
        This API is experimental.
    """
    from temporalio.streams import _workflow

    return _workflow.get_stream(ref)
