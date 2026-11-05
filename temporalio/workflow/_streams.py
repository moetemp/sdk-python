"""Streams in Workflow code.

These calls import the Streams API of :py:mod:`temporalio.streams` on first use, so a Workflow
with no Stream never imports it.
"""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import Self, Unpack

if TYPE_CHECKING:
    import temporalio.api.stream.v1
    from temporalio.streams import StreamOptions, StreamRef

T = TypeVar("T")
_U = TypeVar("_U")


class WorkflowStream(Generic[T]):
    """A Stream in Workflow code, with its calls. ``T`` is the payload type.

    Get one with :py:data:`create_stream`, :py:data:`create_or_get_stream`, or
    :py:func:`to_stream` for a ref that came as data. Its :py:attr:`ref` is the data to pass
    on, such as to an Activity, a child Workflow or the next run of continue-as-new, or through
    a query.

    Workflow code writes to and closes a Stream, and can't read it. Each publish is a Local
    Activity, and each close a command that the backend runs after the Workflow Task commits.

    .. warning::
        This API is experimental.
    """

    def __init__(self, ref: StreamRef[T]) -> None:
        """Create a Stream. Use ``create_stream`` or ``to_stream`` instead."""
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

    def publish(self, *values: T) -> asyncio.Future[int]:
        """Start the write of one Record for each value, in order, and return its future.

        The write has its place in the Stream's order when the call returns, also when no
        caller awaits the future. The writes to one Stream keep the order of the calls. A cancel
        of the future doesn't stop the write. A list is one value.

        The write is a Local Activity, so History has its result and not the values. Publishes
        that wait behind a write to the same Stream are sent as one Local Activity, and each
        call gets the offset of its own last Record. A caller that awaits the future, or calls
        its ``result()`` or ``exception()``, gets the write's error and owns it. An error that
        no caller gets raises at the end of the run, as an error of :py:meth:`publish_nowait`
        does. So a ``StreamError`` fails the run. The Workflow's logger logs the error when the
        write fails, if no caller waits for it then.

        Args:
            values: One value for each Record.

        Returns:
            The future of the write. It gives the offset of the last Record, or raises
            ``StreamError`` when the backend refused the write, such as
            ``StreamNotFoundError`` for a Stream that nobody created.

        Raises:
            StreamClosedError: This run closed the Stream.
            StreamConfigError: The Worker has no Stream backend or not the Stream's backend,
                which fails the Workflow Task and not the run.
            temporalio.workflow.ReadOnlyContextError: The Workflow code must not change state,
                such as a query handler.
        """
        from temporalio.streams import _workflow

        return _workflow.publish(self._ref, values)

    def publish_nowait(self, *values: T) -> None:
        """Write one Record for each value, in order, without an await.

        Use it in sync Workflow code, such as ``@workflow.init`` and sync signal and update
        handlers. It's like ``asyncio.Queue.put_nowait`` for code that can't await ``put``. A
        backend error raises in the run as an error that the run doesn't catch, so a
        ``StreamError`` fails the run.
        """
        from temporalio.streams import _workflow

        _workflow.publish_nowait(self._ref, values)

    async def close(self) -> None:
        """Close the Stream after the writes before it, once the Workflow Task commits.

        A close carries no value, so code with a last value publishes it first. Readers get the
        Records before the close, then their read ends. Later writes in the run raise
        :py:class:`temporalio.streams.StreamClosedError`.

        If the task fails first, or the Worker crashes, the Stream stays open, and the task's
        retry writes to it again. On Redis the close is a regular Activity on the Workflow's
        task queue, which closes the Stream as the run's Workflow Producer. So every Worker on
        that queue must register the Redis backend. A Worker that polls Activities runs the
        close, and a Worker with ``no_remote_activities=True`` doesn't.

        Raises:
            StreamNotOwnerError: The Stream is bound to another run chain, which alone closes
                it. It raises with no call.
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
        """Use the Stream in ``async with``, which closes it at the end.

        Raises:
            StreamNotOwnerError: This run chain can't close the Stream. See :py:meth:`close`.
        """
        from temporalio.streams import _workflow

        # Fail before the block, since the close at its end would raise and hide the block's
        # own error.
        _workflow.check_owner(self._ref)
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


# Python has no type arguments on a call, so a call that takes the payload type is an object
# whose subscript binds the type, as in workflow.create_stream[Turn]("session-7-turns"). Type
# checkers then see WorkflowStream[Turn]. With no subscript, T is Any.


class CreateStream(Generic[T]):
    """The call of :py:data:`create_stream` and :py:data:`create_or_get_stream`.

    Subscript it with the payload type, as in ``workflow.create_stream[Turn]("turns-7")``.

    .. warning::
        This API is experimental.
    """

    def __init__(self, payload_type: type[T] | None, *, or_get: bool) -> None:
        """Create the call. Use the module's ``create_stream`` instead."""
        self._payload_type = payload_type
        self._or_get = or_get

    def __getitem__(self, payload_type: type[_U]) -> CreateStream[_U]:
        """The call for Streams of ``payload_type``.

        It takes a class, such as ``Turn`` or ``list[str]``. Type checkers refuse a union, such
        as ``str | None``, so use ``Any`` or a class that holds the value instead.
        """
        return CreateStream(payload_type, or_get=self._or_get)

    async def __call__(
        self,
        id: str,
        *,
        backend: str | None = None,
        **options: Unpack[StreamOptions],
    ) -> WorkflowStream[T]:
        """Create the Stream, and return it.

        Args:
            id: The Stream id, which the Workflow picks, such as a session id.
            backend: The name of the backend. Defaults to the registered backend with
                ``default=True``. A replay must create the Stream on the same backend, so keep
                the Worker's default, or gate a new one with ``workflow.patched``. A Worker
                serves only one backend, so the name must be the Worker's backend name.
            options: The options of a new Stream, such as ``retention``. See
                :py:class:`temporalio.streams.StreamOptions`.

        Raises:
            StreamAlreadyExistsError: ``create_stream`` of an id that exists, which no earlier
                create of this run chain made.
            StreamError: The backend refused the create. It's ``StreamClosedError`` or
                ``StreamExpiredError`` if a Stream with that id closed or expired.
            ReadOnlyContextError: The code must not change state, such as a query handler.
            StreamConfigError: The Worker has no Stream backend or none with that name, or no
                backend has ``default=True`` and the call gives no backend. It fails the
                Workflow Task, not the run.
        """
        from temporalio.streams import _workflow

        return await _workflow.create_stream(
            self._payload_type,
            id,
            backend=backend,
            options=options,
            or_get=self._or_get,
        )


class CreateStreamNowait(Generic[T]):
    """The call of :py:data:`create_stream_nowait` and :py:data:`create_or_get_stream_nowait`.

    Subscript it with the payload type, as in ``workflow.create_stream_nowait[Turn]("turns-7")``.

    .. warning::
        This API is experimental.
    """

    def __init__(self, payload_type: type[T] | None, *, or_get: bool) -> None:
        """Create the call. Use the module's ``create_stream_nowait`` instead."""
        self._payload_type = payload_type
        self._or_get = or_get

    def __getitem__(self, payload_type: type[_U]) -> CreateStreamNowait[_U]:
        """The call for Streams of ``payload_type``. It takes a class, as for ``create_stream``."""
        return CreateStreamNowait(payload_type, or_get=self._or_get)

    def __call__(
        self,
        id: str,
        *,
        backend: str | None = None,
        **options: Unpack[StreamOptions],
    ) -> WorkflowStream[T]:
        """Start the create, and return the Stream at once.

        It takes the arguments of :py:meth:`CreateStream.__call__`. A backend error raises in
        the run as an error that the run doesn't catch, so a ``StreamError`` fails the run.
        """
        from temporalio.streams import _workflow

        return _workflow.create_stream_nowait(
            self._payload_type,
            id,
            backend=backend,
            options=options,
            or_get=self._or_get,
        )


create_stream: CreateStream[Any] = CreateStream(None, or_get=False)
"""Create a Stream, which fails if a Stream of the id exists.

Give the payload type as a type argument::

    turns = await workflow.create_stream[Turn](f"{session_id}-turns")

A Stream must exist before its first publish. A Workflow creates only standalone Streams, under an
id of its choice, and the Stream doesn't end with the run. Each run of a chain sends the same create
for an id, so a retry run or a cron run that creates the Stream of an earlier run gets that Stream,
and the options don't change. A run after continue-as-new uses the Stream through the ref that the
earlier run forwards, with :py:func:`to_stream`. The create and each publish are Local Activities,
and the create's error is raised here. A close runs after its Workflow Task commits, as a regular
Activity on Redis, so a task that fails first leaves the Stream open for its retry. See
:py:meth:`CreateStream.__call__`.

.. warning::
    This API is experimental.
"""

create_or_get_stream: CreateStream[Any] = CreateStream(None, or_get=True)
"""Create a Stream, or get the one of the id that exists.

It takes the arguments of :py:data:`create_stream`. On a Stream that exists, it ignores the
options. It still fails on a closed or expired Stream.

.. warning::
    This API is experimental.
"""

create_stream_nowait: CreateStreamNowait[Any] = CreateStreamNowait(None, or_get=False)
"""Create a Stream without an await, which fails the run where :py:data:`create_stream` raises.

Use it in sync Workflow code, such as ``@workflow.init``. It's like ``asyncio.Queue.put_nowait``
for code that can't await ``put``. It takes the arguments of :py:data:`create_stream`. A backend
error raises in the run as an error that the run doesn't catch, so a ``StreamError`` fails the
run.

.. warning::
    This API is experimental.
"""

create_or_get_stream_nowait: CreateStreamNowait[Any] = CreateStreamNowait(
    None, or_get=True
)
"""Create a Stream, or get the one that exists, without an await.

It is to :py:data:`create_or_get_stream` what :py:data:`create_stream_nowait` is to
:py:data:`create_stream`.

.. warning::
    This API is experimental.
"""


def to_stream(ref: StreamRef[T]) -> WorkflowStream[T]:
    """Get the Stream of a ref that came as data, such as an argument. It makes no call.

    The Stream's calls check its backend. A Worker that doesn't have it fails the Workflow Task
    at the first call.

    .. warning::
        This API is experimental.
    """
    from temporalio.streams import _workflow

    return _workflow.to_stream(ref)
