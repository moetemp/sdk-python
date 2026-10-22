"""Streams in Workflow code.

Workflow code creates a Stream with :py:func:`create_stream` and gets one again with
:py:func:`stream_handle`. A Stream that the Workflow receives as an argument, such as a
:py:class:`WorkflowStream` that a run passes on through continue-as-new, works the same.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from types import TracebackType
from typing import Any, TypeVar, overload

import google.protobuf.message

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
import temporalio.exceptions
import temporalio.workflow
from temporalio.contrib.streams import _errors, _ids
from temporalio.contrib.streams._defn import TopicDefinition, definition_of
from temporalio.contrib.streams._handle import StreamHandle, TopicBase
from temporalio.contrib.streams.system import models

E = TypeVar("E")
T = TypeVar("T")

# The plugin's Local Activities that write for Workflow code. The marker keeps the answer only,
# since a Local Activity's input is not in History.
CREATE_ACTIVITY = "__temporal_streams_create"
APPEND_ACTIVITY = "__temporal_streams_append"
CLOSE_ACTIVITY = "__temporal_streams_close"
_WRITE_TIMEOUT = timedelta(seconds=30)

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
    # Set for a Stream that the run chain created, which continue-as-new passes on.
    backend: str | None = None


@dataclass
class _RunState:
    streams: dict[str, _RunStream] = field(default_factory=dict)
    # The last write of each Stream. Each write waits for it, so writes keep their order.
    tails: dict[str, asyncio.Task[int]] = field(default_factory=dict)
    # The Streams that a retry run's first write has withdrawn the previous run on.
    withdrawn: set[str] = field(default_factory=set)

    def stream(self, ref: models.StreamRef) -> _RunStream:
        known = self.streams.get(ref.id)
        if known is None:
            known = _RunStream(ref)
            self.streams[ref.id] = known
        return known


def _state() -> _RunState:
    # The state is an attribute of the run, not a weak map entry, since its tasks refer to the
    # run's loop and would keep a map entry alive.
    runtime = temporalio.workflow._Runtime.current()
    state = getattr(runtime, "_temporal_streams_state", None)
    if state is None:
        state = _RunState()
        setattr(runtime, "_temporal_streams_state", state)
    return state


def _backends() -> list[tuple[str, bool]]:
    # Each Stream plugin marks its outbound interceptor with its backend name, so the chain of
    # this run lists the Worker's backends, with their default marks.
    backends = []
    outbound = getattr(temporalio.workflow._Runtime.current(), "_outbound", None)
    while outbound is not None:
        if getattr(outbound, "_temporal_stream_transport", False):
            backends.append(
                (
                    getattr(outbound, "_temporal_stream_backend_name", ""),
                    getattr(outbound, "_temporal_stream_default", False),
                )
            )
        outbound = getattr(outbound, "next", None)
    # Each plugin's inbound interceptor wraps the outbound chain that the plugins before it
    # built, so the chain has the last plugin first.
    backends.reverse()
    return backends


def _backend_name() -> str | None:
    # The Worker's default backend: the one marked default, or else the first.
    backends = _backends()
    if not backends:
        return None
    return next((name for name, default in backends if default), backends[0][0])


def _check_backend(backend: str | None, action: str) -> str:
    # The Worker's config gives these errors, not the history, so they fail the Workflow Task
    # and a Worker with the right config retries it. An ApplicationError would fail the run.
    backends = _backends()
    if not backends:
        raise _errors.StreamConfigError(
            "No Stream plugin is on the Worker, such as RedisStreamPlugin"
        )
    names = [name for name, _ in backends]
    if not backend:
        return next((name for name, default in backends if default), names[0])
    if backend not in names:
        raise _errors.StreamConfigError(
            f"Can't {action} a Stream of the backend {backend}. The Worker has "
            f"{', '.join(names)}."
        )
    return backend


def _write(
    stream_id: str,
    activity: str,
    request: google.protobuf.message.Message,
    backend: str,
) -> asyncio.Task[int]:
    # The Local Activity writes to the backend that the request's ref names.
    ref = (
        request.ref
        if isinstance(request, stream_pb.CreateStreamRequest)
        else getattr(request, "stream")
    )
    ref.backend = backend
    state = _state()
    previous = state.tails.get(stream_id)
    info = temporalio.workflow.info()
    if (
        info.attempt > 1
        and info.continued_run_id
        and stream_id not in state.withdrawn
        and isinstance(
            request, (stream_pb.CreateStreamRequest, stream_pb.AppendRecordsRequest)
        )
    ):
        # A retry run's first write to a Stream withdraws the Records of the run it retries.
        setattr(request, "withdraw_run", info.continued_run_id)
        state.withdrawn.add(stream_id)

    async def run() -> int:
        if previous is not None:
            try:
                await previous
            except BaseException:
                pass
        try:
            return await temporalio.workflow.execute_local_activity(
                activity,
                request,
                result_type=int,
                start_to_close_timeout=_WRITE_TIMEOUT,
            )
        except (
            temporalio.exceptions.ActivityError,
            temporalio.exceptions.ApplicationError,
        ) as err:
            raise _errors.from_activity_error(err) from None

    task = asyncio.get_running_loop().create_task(run())
    state.tails[stream_id] = task
    return task


def _write_nowait(task: asyncio.Task[int]) -> None:
    runtime = temporalio.workflow._Runtime.current()

    def done(task: asyncio.Task[int]) -> None:
        # No caller awaits the write, so its error fails the Workflow Task.
        if not task.cancelled() and task.exception() is not None:
            setattr(runtime, "_current_activation_error", task.exception())

    task.add_done_callback(done)


async def drain() -> None:
    """Wait for the run's writes that nobody awaits, before the run ends."""
    for task in list(_state().tails.values()):
        try:
            await task
        except BaseException:
            # A write that nobody awaits already failed the Workflow Task with its error.
            pass


def _payload(value: Any) -> Any:
    # The Workflow's converter runs here, in the sandbox. The plugin runs only the codec.
    return temporalio.workflow.payload_converter().to_payload(value)


class WorkflowTopic(TopicBase[T]):
    """One Topic of a :py:class:`WorkflowStream`. All Topics of a Stream share one order.

    .. warning::
        This API is experimental.
    """

    async def publish(self, *values: T) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        The write is one Local Activity, so History has its answer and not the values. A
        list is one value. Writes to one Stream keep the order of the calls.

        Raises:
            StreamError: The backend refused the write, such as ``StreamClosedError`` for a
                Stream that something else closed.
            StreamClosedError: This run closed the Stream.
            ReadOnlyContextError: The code must not change state, such as a query handler.
            StreamConfigError: No Stream plugin is on the Worker, or the Stream is of a
                backend that the Worker does not have. It fails the Workflow Task, not the run.
        """
        return await self._stream._append(self._name, values)

    def publish_nowait(self, *values: T) -> None:
        """Write one Record for each value, in order, with no await.

        It is for sync Workflow code, such as ``@workflow.init`` and sync signal and update
        handlers, as ``asyncio.Queue.put_nowait`` is for code that can't await ``put``. A
        backend error fails the Workflow Task.
        """
        _write_nowait(self._stream._append(self._name, values))


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

    def _ref(self) -> stream_pb.StreamRef:
        return stream_pb.StreamRef(
            id=self.id,
            workflow_id=self.workflow_id or "",
            first_execution_run_id=self.first_execution_run_id or "",
        )

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

    def _check(
        self, action: str, *, backend: str | None = None
    ) -> tuple[_RunStream, str]:
        # The run's state of the Stream, and the backend that its calls go to.
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
        return run_stream, backend or _check_backend(self.backend, action)

    def _append(self, topic: str, values: tuple[Any, ...]) -> asyncio.Task[int]:
        if not values:
            raise TypeError("publish takes one or more values")
        _, backend = self._check("write to")
        records = [
            stream_pb.AppendRecord(topic=topic, body=_payload(v)) for v in values
        ]
        return _write(
            self.id,
            APPEND_ACTIVITY,
            stream_pb.AppendRecordsRequest(stream=self._ref(), records=records),
            backend,
        )

    async def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Close the Stream with one Local Activity, after the writes before it.

        Readers get the Records before it, then a ``Closed`` item with ``result`` or
        ``failure``. Later writes in this run raise :py:class:`StreamClosedError`.
        """
        await self._close(result, failure)

    def close_nowait(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Close the Stream with no await, for sync Workflow code.

        It is to :py:meth:`close` what ``asyncio.Queue.put_nowait`` is to ``put``. A backend
        error fails the Workflow Task.
        """
        _write_nowait(self._close(result, failure))

    def _close(self, result: Any, failure: BaseException | None) -> asyncio.Task[int]:
        run_stream, backend = self._check("close")
        run_stream.closed = True
        request = stream_pb.CloseStreamRequest(stream=self._ref())
        if result is not None:
            request.result.CopyFrom(_payload(result))
        if failure is not None:
            temporalio.converter.DataConverter.default.failure_converter.to_failure(
                failure, temporalio.workflow.payload_converter(), request.failure
            )
        return _write(self.id, CLOSE_ACTIVITY, request, backend)

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
    handle with no call. The create is one Local Activity, and its error raises here.

    Args:
        definition: The Stream definition class.
        name: The Stream name. Defaults to the definition's name. Give one Stream per turn,
            for example, its own name.
        retention: How long the Records stay after the close. Defaults to the backend's.
        auto_close: Close the Stream with no result when no Record was appended for this
            long, so a Stream that nobody closes does not stay open. Defaults to never.
        close_with_run: Close the Stream when the run chain closes, with the result or the
            failure of its last run. A run that continues as new passes it on.
        backend: The name of the backend. Defaults to the Worker's default backend: the
            Stream plugin with ``default=True``, or else the first. The default is part of
            the Workflow's code, since a replay must create on the same backend. To move a
            Workflow to another backend, gate it with a patch, for example
            ``backend="native" if workflow.patched("streams-native") else "redis"``.

    Raises:
        StreamError: The backend refused the create, such as for another Retention.
        ReadOnlyContextError: The code must not change state, such as a query handler.
        StreamConfigError: No Stream plugin is on the Worker, or it has no backend of that name.
            It fails the Workflow Task, not the run.
        ValueError: ``close_with_run=False`` for a Stream that already closes with the run
            chain.
    """
    stream, task = _create(
        definition, name, retention, auto_close, close_with_run, backend
    )
    await task
    return stream


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
    is for code that can't await ``put``. It takes the options of :py:func:`create_stream`. A
    backend error fails the Workflow Task.
    """
    stream, task = _create(
        definition, name, retention, auto_close, close_with_run, backend
    )
    _write_nowait(task)
    return stream


def _create(
    definition: type[E],
    name: str | None,
    retention: timedelta | None,
    auto_close: timedelta | None,
    close_with_run: bool,
    backend: str | None,
) -> tuple[WorkflowStream[E], asyncio.Task[int]]:
    backend_name = _check_backend(backend, "create")
    stream = _own_handle(definition, name, backend_name)
    run_stream, _ = stream._check("create", backend=backend_name)
    if run_stream.close_with_run and not close_with_run:
        raise ValueError(
            f"The Stream {stream.id} already closes with the run chain, and "
            "close_with_run=False can't turn that off"
        )
    run_id = temporalio.workflow.info().run_id
    # The owner makes the Stream the run chain's, and names a retry run's withdrawal.
    request = stream_pb.CreateStreamRequest(
        stream_id=stream.id,
        owner=stream_pb.Producer(
            kind=stream_pb.ProducerKind.PRODUCER_KIND_WORKFLOW,
            id=_ids.workflow_producer_id(run_id),
            run_id=run_id,
        ),
    )
    if retention is not None:
        request.retention.FromTimedelta(retention)
    if auto_close is not None:
        request.auto_close.FromTimedelta(auto_close)
    run_stream.close_with_run = close_with_run
    run_stream.backend = stream.backend
    return stream, _write(stream.id, CREATE_ACTIVITY, request, backend_name)


def stream_handle(
    definition: type[E], *, name: str | None = None, backend: str | None = None
) -> WorkflowStream[E]:
    """A Stream that this run chain created, by its definition and name. It makes no call.

    A later run of the chain, such as one after continue-as-new, uses it to write the Stream
    again.

    Args:
        definition: The Stream definition class.
        name: The Stream name. Defaults to the definition's name.
        backend: The backend of the Stream. Defaults to the one that the run chain created it
            on, and else to the Worker's.
    """
    stream = _own_handle(definition, name, backend)
    if backend is None:
        known = _state().streams.get(stream.id)
        stream._backend = (known.backend if known else None) or _backend_name() or None
    return stream
