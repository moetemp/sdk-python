"""Streams in Workflow code.

Workflow code creates a Stream with :py:func:`create_stream` and gets one again with
:py:func:`stream_handle`. A Stream that the Workflow receives as an argument, such as a
:py:class:`WorkflowStream` that a run passes on through continue-as-new, works the same.

Each create, publish and close is a Local Activity, so it has a place in the run's commands,
as an Activity call has. To add, remove or move one in a Workflow that has open runs, gate the
change with ``workflow.patched``, else the replay of those runs fails as nondeterministic.
Publishes that wait behind a write to the same Stream go as one Local Activity.

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
from temporalio.common.streams import _errors, _ids
from temporalio.common.streams._defn import TopicDefinition, definition_of
from temporalio.common.streams._handle import (
    Creating,
    StreamHandle,
    TopicBase,
    exit_failure,
)
from temporalio.common.streams._items import close_status
from temporalio.common.streams.system import models

E = TypeVar("E")
T = TypeVar("T")

# The plugin's Local Activities that write for Workflow code. The marker keeps the answer only,
# since a Local Activity's input is not in History.
CREATE_ACTIVITY = "__temporal_streams_create"
APPEND_ACTIVITY = "__temporal_streams_append"
CLOSE_ACTIVITY = "__temporal_streams_close"
_WRITE_TIMEOUT = timedelta(seconds=30)
# The most that one merged append takes before the codec. The Local Activity checks the size
# after the codec, and sends each call as its own batch when the codec grew the merge past the
# Store's 4 MiB batch limit. Half of it leaves room for most codecs.
_MERGE_RECORDS = 1000
_MERGE_BYTES = 2 * 1024 * 1024

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
class _Write:
    # One Local Activity that writes to a Stream, and the calls it answers. Each call gets the
    # Records up to its own last one, and its future.
    activity: str
    request: google.protobuf.message.Message
    calls: list[tuple[int, asyncio.Future[int]]]
    size: int = 0
    started: bool = False

    def take(self, request: google.protobuf.message.Message) -> bool:
        # An append that waits for the write before it takes the appends that come after it,
        # so a burst of awaits costs two markers and not one for each await.
        if self.started or not isinstance(request, stream_pb.AppendRecordsRequest):
            return False
        mine = self.request
        if (
            not isinstance(mine, stream_pb.AppendRecordsRequest)
            or mine.stream != request.stream
        ):
            return False
        size = sum(record.ByteSize() for record in request.records)
        count = len(mine.records) + len(request.records)
        if count > _MERGE_RECORDS or self.size + size > _MERGE_BYTES:
            return False
        mine.records.extend(request.records)
        self.size += size
        return True


@dataclass
class _RunState:
    streams: dict[str, _RunStream] = field(default_factory=dict)
    # The last write of each Stream. Each write waits for it, so writes keep their order.
    tails: dict[str, asyncio.Task[None]] = field(default_factory=dict)
    # The last write of each Stream that has not started, which later appends may join.
    waiting: dict[str, _Write] = field(default_factory=dict)
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
        raise _errors.StreamConfigError(
            "No Stream plugin is on the Worker, such as RedisStreamPlugin"
        )
    if backend and backend != worker_backend:
        raise _errors.StreamConfigError(
            f"Can't {action} a Stream of the backend {backend}. The Worker has only "
            f"{worker_backend}."
        )
    return worker_backend


def _records(request: google.protobuf.message.Message) -> int:
    if isinstance(request, stream_pb.AppendRecordsRequest):
        return len(request.records)
    return 0


def _write(
    stream_id: str, activity: str, request: google.protobuf.message.Message
) -> asyncio.Future[int]:
    state = _state()
    loop = asyncio.get_running_loop()
    future: asyncio.Future[int] = loop.create_future()
    waiting = state.waiting.get(stream_id)
    if waiting is not None and waiting.take(request):
        waiting.calls.append((_records(waiting.request), future))
        return future
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
    write = _Write(activity, request, [(_records(request), future)])
    if isinstance(request, stream_pb.AppendRecordsRequest):
        write.size = sum(record.ByteSize() for record in request.records)
    previous = state.tails.get(stream_id)

    async def run() -> None:
        if previous is not None:
            try:
                await previous
            except BaseException:
                pass
        write.started = True
        if state.waiting.get(stream_id) is write:
            del state.waiting[stream_id]
        try:
            if isinstance(write.request, stream_pb.AppendRecordsRequest):
                # The Local Activity answers each merged call with its last offset or its error.
                counts = [
                    count - before
                    for (before, _), (count, _) in zip(
                        [(0, None), *write.calls], write.calls
                    )
                ]
                answers = await temporalio.workflow.execute_local_activity(
                    activity,
                    args=[write.request, counts],
                    result_type=list[Any],
                    start_to_close_timeout=_WRITE_TIMEOUT,
                )
            else:
                answers = [
                    await temporalio.workflow.execute_local_activity(
                        activity,
                        write.request,
                        result_type=int,
                        start_to_close_timeout=_WRITE_TIMEOUT,
                    )
                ]
        except (
            temporalio.exceptions.ActivityError,
            temporalio.exceptions.ApplicationError,
        ) as err:
            for _, call in write.calls:
                if not call.done():
                    call.set_exception(_errors.from_activity_error(err))
            return
        except BaseException:
            for _, call in write.calls:
                call.cancel()
            raise
        for (_, call), answer in zip(write.calls, answers):
            if call.done():
                continue
            if isinstance(answer, dict):
                call.set_exception(
                    _errors.from_reason(str(answer["reason"]), str(answer["message"]))
                )
            else:
                call.set_result(int(answer))

    task = loop.create_task(run())
    state.tails[stream_id] = task
    state.waiting[stream_id] = write
    return future


def _write_nowait(future: asyncio.Future[int]) -> None:
    runtime = temporalio.workflow._Runtime.current()

    def done(future: asyncio.Future[int]) -> None:
        # No caller awaits the write, so its error fails the Workflow Task.
        if not future.cancelled() and future.exception() is not None:
            setattr(runtime, "_current_activation_error", future.exception())

    future.add_done_callback(done)


async def drain() -> None:
    """Wait for the run's writes that nobody awaits, before the run ends."""
    for task in list(_state().tails.values()):
        try:
            await task
        except BaseException:
            # A cancelled write already cancelled its callers.
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

        The write is a Local Activity, so History has its answer and not the values. Publishes
        that wait behind a write to the same Stream go as one Local Activity, and each call
        gets the offset of its own last Record. A list is one value. Writes to one Stream keep
        the order of the calls.

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

    def _append(self, topic: str, values: tuple[Any, ...]) -> asyncio.Future[int]:
        if not values:
            raise TypeError("publish takes one or more values")
        self._check("write to")
        records = [
            stream_pb.AppendRecord(topic=topic, body=_payload(v)) for v in values
        ]
        return _write(
            self.id,
            APPEND_ACTIVITY,
            stream_pb.AppendRecordsRequest(stream=self._ref(), records=records),
        )

    async def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Close the Stream with one Local Activity, after the writes before it.

        Readers get the Records before it, then a ``Closed`` item with ``result`` or
        ``failure``. A ``CancelledError`` failure closes it as canceled. Later writes in this run
        raise :py:class:`StreamClosedError`.

        Raises:
            StreamNotOwnerError: This run chain didn't create the Stream.
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

    def _close(self, result: Any, failure: BaseException | None) -> asyncio.Future[int]:
        info = temporalio.workflow.info()
        if (self.workflow_id, self.first_execution_run_id) != (
            info.workflow_id,
            info.first_execution_run_id,
        ):
            # Only the run chain that created a Stream ends it for every reader. The backend
            # refuses it too, and this error comes before a Local Activity is spent.
            raise _errors.StreamNotOwnerError(
                f"Can't close the Stream {self.id}, since this run chain didn't create it",
                reason="stream_not_owner",
            )
        self._check("close").closed = True
        request = stream_pb.CloseStreamRequest(stream=self._ref())
        if result is not None:
            request.result.CopyFrom(_payload(result))
        if failure is not None:
            temporalio.converter.DataConverter.default.failure_converter.to_failure(
                failure, temporalio.workflow.payload_converter(), request.failure
            )
        status = close_status(failure)
        if status is not None:
            request.status = status
        return _write(self.id, CLOSE_ACTIVITY, request)

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
        close, failure = exit_failure(exc)
        if close:
            await self.close(failure=failure)


def _own_handle(definition: type[E], backend: str | None) -> WorkflowStream[E]:
    info = temporalio.workflow.info()
    return WorkflowStream(
        id=_ids.scoped_stream_id(
            info.workflow_id,
            info.first_execution_run_id,
            definition_of(definition).name,
        ),
        workflow_id=info.workflow_id,
        first_execution_run_id=info.first_execution_run_id,
        backend=backend,
        definition=definition,
    )


def create_stream(
    definition: type[E],
    *,
    retention: timedelta | None = None,
    auto_close: timedelta | None = None,
    close_with_run: bool = True,
    backend: str | None = None,
) -> Creating[WorkflowStream[E]]:
    """Create a Stream that this run chain owns.

    Its id is ``<workflow id>:<first run id>:<name>``, so a client finds it from the Workflow
    handle with no call. The create is one Local Activity, and its error raises here.

    Await the call for the handle, or use it in ``async with``, which closes the Stream at the
    end of the block::

        async with create_stream(streams.with_name(Events, "turn-3")) as turn:
            await turn.topic(Events.tokens).publish("hi")

    Args:
        definition: The Stream definition class. Its name is the Stream name. For one Stream
            per turn, for example, pass ``streams.with_name(Events, f"turn-{n}")``.
        retention: How long the Records stay after the close. Defaults to the backend's.
        auto_close: Close the Stream with no result when no Record was appended for this
            long, so a Stream that nobody closes does not stay open. Defaults to never.
        close_with_run: Close the Stream when the run chain closes, with the result or the
            failure of its last run. A run that continues as new passes it on.
        backend: The name of the backend. Defaults to the Worker's. The default is part of
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

    async def create() -> WorkflowStream[E]:
        stream, task = _create(
            definition, retention, auto_close, close_with_run, backend
        )
        await task
        return stream

    return Creating(create)


def create_stream_nowait(
    definition: type[E],
    *,
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
    stream, task = _create(definition, retention, auto_close, close_with_run, backend)
    _write_nowait(task)
    return stream


def _create(
    definition: type[E],
    retention: timedelta | None,
    auto_close: timedelta | None,
    close_with_run: bool,
    backend: str | None,
) -> tuple[WorkflowStream[E], asyncio.Future[int]]:
    stream = _own_handle(definition, _check_backend(backend, "create"))
    run_stream = stream._check("create", backend_checked=True)
    if run_stream.close_with_run and not close_with_run:
        raise ValueError(
            f"The Stream {stream.id} already closes with the run chain, and "
            "close_with_run=False can't turn that off"
        )
    run_id = temporalio.workflow.info().run_id
    # The owner makes the Stream the run chain's, and names a retry run's withdrawal.
    # The ref names the run chain, which the Store keeps for its owner check of a close.
    request = stream_pb.CreateStreamRequest(
        stream_id=stream.id,
        owner=stream_pb.Producer(
            kind=stream_pb.ProducerKind.PRODUCER_KIND_WORKFLOW,
            id=_ids.workflow_producer_id(run_id),
            run_id=run_id,
        ),
        ref=stream._ref(),
    )
    if retention is not None:
        request.retention.FromTimedelta(retention)
    if auto_close is not None:
        request.auto_close.FromTimedelta(auto_close)
    run_stream.close_with_run = close_with_run
    run_stream.backend = stream.backend
    return stream, _write(stream.id, CREATE_ACTIVITY, request)


def stream_handle(
    definition: type[E], *, backend: str | None = None
) -> WorkflowStream[E]:
    """A Stream that this run chain created, by its definition. It makes no call.

    A later run of the chain, such as one after continue-as-new, uses it to write the Stream
    again.

    Args:
        definition: The Stream definition class, renamed as for the create.
        backend: The backend of the Stream. Defaults to the one that the run chain created it
            on, and else to the Worker's.
    """
    stream = _own_handle(definition, backend)
    if backend is None:
        known = _state().streams.get(stream.id)
        stream._backend = (known.backend if known else None) or _backend_name() or None
    return stream
