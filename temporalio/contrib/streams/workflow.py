"""Streams in Workflow code.

Workflow code creates a Stream with :py:func:`create_stream` and gets one again with
:py:func:`stream_handle`. A Stream that the Workflow receives as an argument, such as a
:py:class:`WorkflowStream` that a run passes on through continue-as-new, works the same.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Coroutine
from dataclasses import dataclass, field
from datetime import timedelta
from types import TracebackType
from typing import Any, Generic, TypeVar, overload

import temporalio.exceptions
import temporalio.workflow
from temporalio.contrib.streams import _errors, _ids
from temporalio.contrib.streams._defn import TopicDefinition, definition_of
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams.system import create_stream as _create_stream
from temporalio.contrib.streams.system import models
from temporalio.contrib.streams.system._resources.stream import Stream as _Stream

E = TypeVar("E")
T = TypeVar("T")
_R = TypeVar("_R")

__all__ = [
    "WorkflowStream",
    "WorkflowTopic",
    "create_stream",
    "create_stream_nowait",
    "stream_handle",
]


@dataclass
class _RunStream:
    # What this run knows about one Stream. Many handles may name it, so it is kept by id.
    ref: models.StreamRef
    close_with_run: bool = False
    closed: bool = False


@dataclass
class _RunState:
    streams: dict[str, _RunStream] = field(default_factory=dict)

    def stream(self, ref: models.StreamRef) -> _RunStream:
        known = self.streams.get(ref.id)
        if known is None:
            known = _RunStream(ref)
            self.streams[ref.id] = known
        return known


# One state per Workflow instance. The plugin passes temporalio.contrib.streams through the
# sandbox, so the plugin's interceptor sees the same state, and it goes when the run leaves
# the cache.
_states: weakref.WeakKeyDictionary[Any, _RunState] = weakref.WeakKeyDictionary()


def _state() -> _RunState:
    runtime = temporalio.workflow._Runtime.current()
    state = _states.get(runtime)
    if state is None:
        state = _RunState()
        _states[runtime] = state
    return state


def _backend_name() -> str | None:
    # A Stream plugin marks its outbound interceptor with its backend name. With none, the
    # call would go to the server as a System Nexus command, which only native Streams serve.
    outbound = getattr(temporalio.workflow._Runtime.current(), "_outbound", None)
    while outbound is not None:
        if getattr(outbound, "_temporal_stream_transport", False):
            return getattr(outbound, "_temporal_stream_backend_name", "")
        outbound = getattr(outbound, "next", None)
    return None


def _check_backend(backend: str | None, action: str) -> str:
    # The Worker's config gives these errors, not the history, so they fail the Workflow Task
    # and a Worker with the right config retries it. An ApplicationError would fail the run.
    worker_backend = _backend_name()
    if worker_backend is None:
        raise RuntimeError(
            "No Stream plugin is on the Worker, such as RedisStreamPlugin"
        )
    if backend and backend != worker_backend:
        raise RuntimeError(
            f"Can't {action} a Stream of the backend {backend}. The Worker has only "
            f"{worker_backend}."
        )
    return worker_backend


def _now(call: Coroutine[Any, Any, _R]) -> _R:
    # The plugin's interceptor answers at once, so the call ends in its first step.
    try:
        call.send(None)
    except StopIteration as done:
        return done.value
    call.close()
    raise RuntimeError("No Stream plugin took the call, such as RedisStreamPlugin")


class WorkflowTopic(Generic[T]):
    """One Topic of a :py:class:`WorkflowStream`. All Topics of a Stream share one order.

    .. warning::
        This API is experimental.
    """

    def __init__(self, stream: WorkflowStream[Any], name: str, type: Any) -> None:
        """Create a Topic. Use :py:meth:`WorkflowStream.topic` instead."""
        self._stream = stream
        self._name = name
        self._type = type

    @property
    def name(self) -> str:
        """Name of the Topic."""
        return self._name

    @property
    def type(self) -> Any:
        """Type of the Topic's values."""
        return self._type

    async def publish(self, *values: T) -> None:
        """Add one Record for each value, in order.

        It returns when the Records are in the run's buffer. The backend writes them before
        the Workflow Task completes, and before a Local Activity that the run starts later. A
        list is one value.

        Raises:
            StreamClosedError: This run closed the Stream.
            ReadOnlyContextError: The code must not change state, such as a query handler.
            RuntimeError: No Stream plugin is on the Worker, or the Stream is of a backend
                that the Worker does not have. It fails the Workflow Task, not the run.
        """
        self.publish_nowait(*values)

    def publish_nowait(self, *values: T) -> None:
        """Add one Record for each value, in order, with no await.

        It is for sync Workflow code, such as ``@workflow.init`` and sync signal and update
        handlers, as ``asyncio.Queue.put_nowait`` is for code that can't await ``put``. It has
        the buffer and the errors of :py:meth:`publish`.
        """
        self._stream._append(self._name, values)


class WorkflowStream(StreamHandle[E]):
    """A Stream in Workflow code. Workflow code writes and closes it, and can't read it.

    Pass it to an Activity, a child Workflow or the next run of continue-as-new as an
    argument. Use it in ``async with`` to close it at the end of the block.

    .. warning::
        This API is experimental.
    """

    def _model_ref(self) -> models.StreamRef:
        return models.StreamRef(
            id=self.id,
            workflow_id=self.workflow_id,
            first_execution_run_id=self.first_execution_run_id,
        )

    def _resource(self) -> _Stream:
        # The generated Stream resource makes the System Nexus calls. This class adds the
        # definition's typed Topics and the run's replay rules.
        return _Stream(stream=self._model_ref())

    @overload
    def topic(self, topic: TopicDefinition[T]) -> WorkflowTopic[T]: ...

    @overload
    def topic(self, topic: str) -> WorkflowTopic[Any]: ...

    @overload
    def topic(self, topic: str, *, type: type[T]) -> WorkflowTopic[T]: ...

    def topic(
        self, topic: str | TopicDefinition[Any], *, type: Any = None
    ) -> WorkflowTopic[Any]:
        """A Topic of this Stream.

        Args:
            topic: A Topic of the Stream's definition, such as ``Events.turns``, or a name.
                A name of the definition takes its type.
            type: The type of a Topic outside the definition, or of a Stream with no
                definition.

        Raises:
            StreamError: The Topic is not in the Stream's definition, with the reason
                ``unknown_topic``.
        """
        name, topic_type = self._topic_spec(topic, type)
        return WorkflowTopic(self, name, topic_type)

    def _check(self, action: str, *, backend_checked: bool = False) -> _RunStream:
        if temporalio.workflow.unsafe.is_read_only():
            raise _errors.ReadOnlyContextError(
                f"Can't {action} a Stream in read-only code"
            )
        run_stream = _state().stream(self._model_ref())
        if run_stream.closed:
            raise _errors.StreamClosedError(
                f"Can't {action} the Stream {self.id}, since this run closed it",
                reason="stream_closed",
            )
        if not backend_checked:
            _check_backend(self.backend, action)
        return run_stream

    def _append(self, topic: str, values: tuple[Any, ...]) -> None:
        if not values:
            raise TypeError("publish takes one or more values")
        self._check("write to")
        # The backend's answer has no offsets that replay could give again, so it is dropped.
        _now(
            self._resource().append_records(
                records=[
                    models.AppendRecord(topic=topic, body=value) for value in values
                ]
            )
        )

    async def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Close the Stream. It goes in the run's buffer, as a publish does.

        Readers get the Records before it, then a ``Closed`` item with ``result`` or
        ``failure``. Later writes in this run raise :py:class:`StreamClosedError`.
        """
        self.close_nowait(result=result, failure=failure)

    def close_nowait(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Close the Stream with no await, for sync Workflow code.

        It is to :py:meth:`close` what ``asyncio.Queue.put_nowait`` is to ``put``, with the same
        buffer and errors.
        """
        run_stream = self._check("close")
        run_stream.closed = True
        _now(self._resource().close(result=result, failure=failure))

    async def __aenter__(self) -> WorkflowStream[E]:
        """Use the Stream in ``async with``, which closes it at the end."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream, with the exception as its failure if there is one.

        A cancel closes it with a ``CancelledError`` failure. Another ``BaseException``, such as
        an eviction, leaves it open, since the block did not end in Workflow code.
        """
        if _state().stream(self._model_ref()).closed:
            return
        if isinstance(exc, asyncio.CancelledError):
            await self.close(
                failure=temporalio.exceptions.CancelledError(str(exc) or "Cancelled")
            )
        elif exc is None or isinstance(exc, Exception):
            await self.close(failure=exc)


def _own_handle(
    definition: type[E], name: str | None, backend: str | None
) -> WorkflowStream[E]:
    info = temporalio.workflow.info()
    stream_name = name or definition_of(definition).name
    return WorkflowStream(
        id=_ids.scoped_stream_id(
            info.workflow_id, info.first_execution_run_id, stream_name
        ),
        workflow_id=info.workflow_id,
        first_execution_run_id=info.first_execution_run_id,
        backend=backend,
        definition=definition,
    )


async def create_stream(
    definition: type[E],
    *,
    name: str | None = None,
    retention: timedelta | None = None,
    auto_close: timedelta | None = None,
    close_with_run: bool = True,
    backend: str | None = None,
) -> WorkflowStream[E]:
    """Create a Stream that this run chain owns.

    Its id is ``<workflow id>:<first run id>:<name>``, so a client finds it from the Workflow
    handle with no call. The create goes in the run's buffer, as a publish does. A bad setting
    fails the Workflow Task when the buffer goes out.

    Args:
        definition: The Stream definition class.
        name: The Stream name. Defaults to the definition's name. Give one Stream per turn,
            for example, its own name.
        retention: How long the Records stay after the close. Defaults to the backend's.
        auto_close: Close the Stream with no result when no Record was appended for this
            long, so a Stream that nobody closes does not stay open. Defaults to never.
        close_with_run: Close the Stream when the run chain closes, with the result or the
            failure of its last run. A run that continues as new passes it on.
        backend: The name of the backend. Defaults to the Worker's.

    Raises:
        StreamClosedError: This run closed the Stream.
        ReadOnlyContextError: The code must not change state, such as a query handler.
        RuntimeError: No Stream plugin is on the Worker, or it has no backend of that name.
            It fails the Workflow Task, not the run.
        ValueError: ``close_with_run=False`` for a Stream that already closes with the run
            chain.
    """
    return create_stream_nowait(
        definition,
        name=name,
        retention=retention,
        auto_close=auto_close,
        close_with_run=close_with_run,
        backend=backend,
    )


def create_stream_nowait(
    definition: type[E],
    *,
    name: str | None = None,
    retention: timedelta | None = None,
    auto_close: timedelta | None = None,
    close_with_run: bool = True,
    backend: str | None = None,
) -> WorkflowStream[E]:
    """Create a Stream that this run chain owns, with no await.

    It is for sync Workflow code, such as ``@workflow.init``, as ``asyncio.Queue.put_nowait``
    is for code that can't await ``put``. It takes the options of :py:func:`create_stream`
    and has the same buffer and errors.
    """
    stream = _own_handle(definition, name, _check_backend(backend, "create"))
    run_stream = stream._check("create", backend_checked=True)
    if run_stream.close_with_run and not close_with_run:
        raise ValueError(
            f"The Stream {stream.id} already closes with the run chain, and "
            "close_with_run=False can't turn that off"
        )
    _now(
        _create_stream(stream_id=stream.id, retention=retention, auto_close=auto_close)
    )
    run_stream.close_with_run = close_with_run
    return stream


def stream_handle(definition: type[E], *, name: str | None = None) -> WorkflowStream[E]:
    """A Stream that this run chain created, by its definition and name. It makes no call.

    A later run of the chain, such as one after continue-as-new, uses it to write the Stream
    again.

    Args:
        definition: The Stream definition class.
        name: The Stream name. Defaults to the definition's name.
    """
    return _own_handle(definition, name, _backend_name() or None)
