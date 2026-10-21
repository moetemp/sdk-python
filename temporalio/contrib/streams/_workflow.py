"""Streams in Workflow code."""

from __future__ import annotations

import weakref
from collections.abc import Coroutine
from datetime import timedelta
from types import TracebackType
from typing import Any, TypeVar

import temporalio.workflow
from temporalio.contrib.streams import _errors, _ids
from temporalio.contrib.streams._stream import StreamRef, Topic, _Writable
from temporalio.contrib.streams.system import models
from temporalio.contrib.streams.system.operations.append_records import (
    append_records,
)
from temporalio.contrib.streams.system.operations.close_stream import close_stream
from temporalio.contrib.streams.system.operations.create_stream import (
    create_stream,
)

T = TypeVar("T")
_R = TypeVar("_R")


class _RunState:
    def __init__(self) -> None:
        self.streams: dict[str, WorkflowStream] = {}


# One state per Workflow instance. The temporalio modules pass through the sandbox, so the key
# is the instance, and the state goes when the run leaves the cache.
_states: weakref.WeakKeyDictionary[Any, _RunState] = weakref.WeakKeyDictionary()


def _state() -> _RunState:
    runtime = temporalio.workflow._Runtime.current()
    state = _states.get(runtime)
    if state is None:
        state = _RunState()
        _states[runtime] = state
    return state


def _has_transport() -> bool:
    # A Stream plugin marks its outbound interceptor. With none, the call would go to the
    # server as a System Nexus command, which only native Streams serve.
    outbound = getattr(temporalio.workflow._Runtime.current(), "_outbound", None)
    while outbound is not None:
        if getattr(outbound, "_temporal_stream_transport", False):
            return True
        outbound = getattr(outbound, "next", None)
    return False


def _now(call: Coroutine[Any, Any, _R]) -> _R:
    # The plugin's interceptor answers at once, so the call ends in its first step.
    try:
        call.send(None)
    except StopIteration as done:
        return done.value
    call.close()
    raise _errors.StreamTransportMissingError(
        "No Stream plugin took the call, such as RedisStreamPlugin"
    )


class WorkflowStream(_Writable):
    """A Stream that Workflow code writes to.

    Writes are buffered, and the Records go to the Store when the Workflow Task completes.
    Workflow code can't read a Stream.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        stream_id: str,
        *,
        create: bool,
        retention: timedelta | None,
        auto_close: bool,
    ) -> None:
        """Create a Stream handle. Use :py:func:`workflow_stream` or
        :py:func:`create_workflow_stream` instead.
        """
        self._id = stream_id
        self._create = create
        self._retention = retention
        self._auto_close = auto_close
        self._created = False
        self._used = False
        self._closed = False

    @property
    def ref(self) -> StreamRef:
        """Reference to pass to Activities, child Workflows and clients."""
        return StreamRef(self._id)

    @property
    def auto_close(self) -> bool:
        """Whether the run closes the Stream when it closes."""
        return self._auto_close

    def topic(self, name: str, *, type: type[T] = Any) -> Topic[T]:  # type: ignore[assignment]
        """A Topic of this Stream."""
        return Topic(self, name, type)

    def _check(self, action: str) -> None:
        if temporalio.workflow.unsafe.is_read_only():
            raise _errors.ReadOnlyContextError(
                f"Can't {action} a Stream in read-only code"
            )
        if self._closed:
            raise _errors.StreamClosedError(
                f"Can't {action} the closed Stream {self._id}", reason="stream_closed"
            )
        if not _has_transport():
            raise _errors.StreamTransportMissingError(
                "No Stream plugin is on the Worker, such as RedisStreamPlugin"
            )

    def _ensure_created(self) -> None:
        if self._create and not self._created:
            self._created = True
            _now(create_stream(stream_id=self._id, retention=self._retention))

    def _publish(self, topic: str, value: Any, force_flush: bool) -> None:
        self._check("write to")
        self._ensure_created()
        self._used = True
        append_records(
            stream=models.StreamRef(id=self._id),
            records=[models.AppendRecord(topic=topic, body=value)],
        )

    def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Close the Stream. It goes out after the server accepts this Workflow Task.

        Readers get the Records before it, then a ``Closed`` item with ``result`` or
        ``failure``. Later writes raise :py:class:`StreamClosedError`.
        """
        self._check("close")
        self._ensure_created()
        self._closed = True
        close_stream(
            stream=models.StreamRef(id=self._id), result=result, failure=failure
        )

    def __enter__(self) -> WorkflowStream:
        """Use the Stream in a ``with`` block, which closes it at the end."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream, with the exception as its failure if there is one."""
        if not self._closed:
            failure = exc if isinstance(exc, Exception) else None
            self.close(failure=failure)


def workflow_stream(
    ref: StreamRef | None = None, *, auto_close: bool | None = None
) -> WorkflowStream:
    """A Stream for this Workflow to write to.

    Args:
        ref: A Stream that the Workflow received. With none, the run chain's default Stream,
            whose id is ``<workflow id>:<first run id>``.
        auto_close: Whether the run closes the Stream when the run closes, with the run's
            result or failure. Defaults to True for the default Stream and False for a
            received one. A run that continues as new does not close it.
    """
    state = _state()
    if ref is None:
        info = temporalio.workflow.info()
        stream_id = _ids.default_stream_id(
            info.workflow_id, info.first_execution_run_id
        )
        create, close = True, True if auto_close is None else auto_close
    else:
        stream_id = ref.id
        create, close = False, bool(auto_close)
    stream = state.streams.get(stream_id)
    if stream is None:
        stream = WorkflowStream(
            stream_id, create=create, retention=None, auto_close=close
        )
        state.streams[stream_id] = stream
    elif auto_close is not None:
        stream._auto_close = auto_close
    return stream


def create_workflow_stream(
    name: str, *, retention: timedelta | None = None
) -> WorkflowStream:
    """Create a Stream with a name, scoped to this run chain.

    Its id is ``<workflow id>:<first run id>:<name>``, so a client finds it with
    :py:func:`for_handle`. The name must be unique in the run chain. Use it in a ``with``
    block, which closes it at the end.

    Args:
        name: The name of the Stream.
        retention: The age after which a Record is dropped. Defaults to the Store's.
    """
    info = temporalio.workflow.info()
    stream_id = _ids.scoped_stream_id(
        info.workflow_id, info.first_execution_run_id, name
    )
    stream = WorkflowStream(
        stream_id, create=True, retention=retention, auto_close=False
    )
    stream._check("create")
    stream._ensure_created()
    _state().streams[stream_id] = stream
    return stream
