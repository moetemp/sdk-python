"""Streams in Workflow code.

Workflow code creates a Stream with :py:func:`temporalio.workflow.create_stream` and gets one
again with :py:func:`temporalio.workflow.stream_handle`. A Stream that the Workflow receives as
an argument, such as a :py:class:`WorkflowStream` that a run passes on through continue-as-new,
works the same.

Each create and publish is a Local Activity, and each close is a command that the backend
carries out once the Workflow Task commits: a regular Activity on Redis, and a System Nexus
command on native. So each has a place in the run's commands. To add, remove or move one in a
Workflow that has open runs, gate the change with ``workflow.patched``, else the replay of those
runs fails as nondeterministic. Publishes that wait behind a write to the same Stream go as one
Local Activity. A task that fails, or a crash, before the close commits leaves the Stream open
for the task's retry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from types import TracebackType
from typing import Any, NamedTuple, TypeVar, overload

import google.protobuf.message

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
import temporalio.exceptions
import temporalio.workflow
from temporalio.common.streams import _errors, _ids, _workflow_reader
from temporalio.common.streams._access import read_spec
from temporalio.common.streams._defn import TopicDefinition, definition_of
from temporalio.common.streams._handle import (
    Creating,
    StreamHandle,
    TopicBase,
    exit_failure,
)
from temporalio.common.streams._items import End, close_status
from temporalio.common.streams._workflow_reader import WorkflowItems
from temporalio.common.streams.system import models

E = TypeVar("E")
T = TypeVar("T")

# The backend's Activities that write for Workflow code. The create and the append are Local
# Activities, whose marker keeps the answer only, since a Local Activity's input is not in
# History. The Redis close is a regular Activity on the Workflow's task queue.
CREATE_ACTIVITY = "__temporal_streams_create"
APPEND_ACTIVITY = "__temporal_streams_append"
CLOSE_ACTIVITY = "__temporal_streams_close"
_WRITE_TIMEOUT = timedelta(seconds=30)
# The most that one merged append takes before the codec. The Local Activity checks the size
# after the codec, and sends each call as its own batch when the codec grew the merge past the
# Store's 4 MiB batch limit. Half of it leaves room for most codecs.
_MERGE_RECORDS = 1000
_MERGE_BYTES = 2 * 1024 * 1024


@dataclass
class _RunStream:
    # What this run knows about one Stream. Many handles may name it, so it is kept by id.
    ref: models.StreamRef
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
    # Whether the run created, bound or received a Stream. Only then do its Activities carry
    # a Producer key, so a run with no Stream adds nothing to History.
    uses_streams: bool = False

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


class WorkerBackend(NamedTuple):
    """A Stream backend of the Worker that runs the Workflow."""

    name: str
    default: bool
    kind: str


def worker_backends() -> list[WorkerBackend]:
    """The Worker's backends, each of which marks one of the run's outbound interceptors."""
    backends = []
    outbound = getattr(temporalio.workflow._Runtime.current(), "_outbound", None)
    while outbound is not None:
        if getattr(outbound, "_temporal_stream_transport", False):
            backends.append(
                WorkerBackend(
                    getattr(outbound, "_temporal_stream_backend_name", ""),
                    getattr(outbound, "_temporal_stream_default", False),
                    getattr(outbound, "_temporal_stream_kind", ""),
                )
            )
        outbound = getattr(outbound, "next", None)
    # Each backend's inbound interceptor wraps the outbound chain that the backends before it
    # built, so the chain has the last backend first.
    backends.reverse()
    return backends


async def _close_in_workflow(
    request: stream_pb.CloseStreamRequest, backend: str
) -> int:
    # The Stream's backend carries out the close once the Workflow Task commits.
    outbound = _workflow_reader.reader_of(backend)
    return await outbound._temporal_stream_backend._close_in_workflow(request)


def no_worker_backend() -> _errors.StreamConfigError:
    """The error of Workflow code on a Worker with no Stream backend."""
    return _errors.StreamConfigError(
        "No Stream backend is on the Worker. Register one on its client with "
        "stream_backends=."
    )


def _check_backend(backend: str | None, action: str) -> str:
    # The Worker's config gives these errors, not the history, so they fail the Workflow Task
    # and a Worker with the right config retries it. An ApplicationError would fail the run.
    backends = worker_backends()
    if not backends:
        raise no_worker_backend()
    names = [b.name for b in backends]
    if not backend:
        # The default is the backend with default=True, else native, which every Worker has.
        return next((b.name for b in backends if b.default), _ids.NATIVE_BACKEND)
    if backend not in names:
        raise _errors.StreamConfigError(
            f"Can't {action} a Stream of the backend {backend}. The Worker has "
            f"{', '.join(names)}."
        )
    return backend


def _records(request: google.protobuf.message.Message) -> int:
    if isinstance(request, stream_pb.AppendRecordsRequest):
        return len(request.records)
    return 0


def _write(
    stream_id: str,
    activity: str,
    request: google.protobuf.message.Message,
    backend: str,
) -> asyncio.Future[int]:
    # The Local Activity writes to the backend that the request's ref names.
    ref = (
        request.ref
        if isinstance(request, stream_pb.CreateStreamRequest)
        else getattr(request, "stream")
    )
    ref.backend = backend
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
            elif activity == CLOSE_ACTIVITY:
                assert isinstance(write.request, stream_pb.CloseStreamRequest)
                answers = [await _close_in_workflow(write.request, backend)]
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
                    call.set_exception(
                        _errors.in_workflow(_errors.from_activity_error(err))
                    )
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
                    _errors.in_workflow(
                        _errors.from_reason(
                            str(answer["reason"]), str(answer["message"])
                        )
                    )
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
    """Wait for the run's writes and closes that nobody awaits, before the run ends."""
    for task in list(_state().tails.values()):
        try:
            await task
        except BaseException:
            # A cancelled write already cancelled its callers.
            pass


def _payload(value: Any) -> Any:
    # The Workflow's converter runs here, in the sandbox. The backend runs only the codec.
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
            StreamConfigError: No Stream backend is on the Worker, or the Stream is of a
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
    """A Stream in Workflow code. Workflow code writes, reads and closes it.

    Pass it to an Activity, a child Workflow or the next run of continue-as-new as an
    argument. Use it in ``async with`` to close it at the end of the block. The native server
    closes it when the run chain ends. On Redis a run end closes nothing, so a Stream that the
    run chain never closes stays open until its ``safety_ttl``.

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
        # A server that nests a Stream in its owner finds it only by the owner's fields.
        return stream_pb.StreamRef(
            id=self.id,
            workflow_id=self.workflow_id or "",
            first_execution_run_id=self.first_execution_run_id or "",
            activity_id=self.activity_id or "",
            activity_run_id=self.activity_run_id or "",
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

    def subscribe(
        self,
        *topics: str | TopicDefinition[Any] | WorkflowTopic[Any],
        offset: int | End | None = None,
        wait_new_records: bool = True,
        topic_prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
    ) -> WorkflowItems:
        """Read the Stream's items in order, in Workflow code.

        It is an async iterator, as on the client, and it yields the same ``Record``,
        ``Superseded`` and ``Closed`` items. Each page is a Local Activity whose answer is in
        History, so a replay reads the same items. On a native Stream the marker holds only
        the page's offsets, and a replay reads the page again from the server. A read at the
        end of a native Stream waits on the server until a Record comes or the Stream closes.
        Such waits take at most 20 of the run's 30 pending Nexus operations, and a read past
        them waits as on Redis. On Redis the marker holds the page, the Local Activity waits a
        short time for new Records, and a page with none is followed by a durable timer from
        1 s up to 60 s.

        A read sees this run's writes that it awaited, since each write is a Local Activity
        that ends before the next read starts.

        The read also ends when this run should continue as new. Check
        ``continue_as_new_suggested`` after the loop, and pass ``next_offset`` to the next
        run::

            items = stream.subscribe(offset=offset)
            async for item in items:
                ...
            if items.continue_as_new_suggested:
                workflow.continue_as_new(items.next_offset)

        Args:
            topics: Read these Topics, by Topic of the definition, name or Topic object.
                With no Topics and no ``topic_prefixes``, read every Topic.
            offset: Start at this offset, inclusive, or at ``streams.END`` for new Records
                only. Defaults to the first Record.
            wait_new_records: Wait for new Records at the end of the Stream. When False,
                the read ends at the newest Record.
            topic_prefixes: Also read the Topics whose names start with one of these.
            create_wait: How long to wait for a Stream that nobody created yet. Defaults to
                no wait.

        Raises:
            StreamNotFoundError: The Stream does not exist, or it was not created in time.
            ReadOnlyContextError: The code must not change state, such as a query handler.
            StreamConfigError: The Stream is of a backend that the Worker does not have. It
                fails the Workflow Task, not the run.
        """
        if not temporalio.workflow.unsafe.is_read_only():
            _state().uses_streams = True
        read = read_spec(
            self,
            topics,
            topic_prefixes=topic_prefixes,
            default_type=None,
            offset=offset,
            last=None,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        reader = _workflow_reader.reader_of(_check_backend(self.backend, "read"))
        return WorkflowItems(
            reader,
            self._ref(),
            topics=read.topics,
            topic_prefixes=topic_prefixes,
            types=read.types,
            offset=offset,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )

    def _check(
        self, action: str, *, backend: str | None = None
    ) -> tuple[_RunStream, str]:
        # The run's state of the Stream, and the backend that its calls go to.
        if temporalio.workflow.unsafe.is_read_only():
            raise _errors.ReadOnlyContextError(
                f"Can't {action} a Stream in read-only code"
            )
        state = _state()
        state.uses_streams = True
        run_stream = state.stream(self._model_ref())
        if run_stream.closed:
            raise _errors.StreamClosedError(
                f"Can't {action} the Stream {self.id}, since this run closed it",
                reason="stream_closed",
            )
        return run_stream, backend or _check_backend(self.backend, action)

    def _append(self, topic: str, values: tuple[Any, ...]) -> asyncio.Future[int]:
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
        """Close the Stream after the writes before it, once the Workflow Task commits.

        A task that fails before then, or a crash, leaves the Stream open, and the task's retry
        writes to it again. On Redis the close is a regular Activity on the Workflow's task
        queue, which closes as the run chain, the Stream's owner. So every Worker on that queue
        registers the Redis backend. On the native backend it is a System Nexus command, which
        the server carries out. Readers get the Records before it, then a ``Closed`` item with
        ``result`` or ``failure``. A ``CancelledError`` failure closes it as canceled. Later
        writes in this run raise :py:class:`StreamClosedError`.

        Raises:
            StreamNotOwnerError: This run chain didn't create the Stream.
        """
        await self._close(result, failure)

    def close_nowait(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Close the Stream with no await, for sync Workflow code.

        It is to :py:meth:`close` what ``asyncio.Queue.put_nowait`` is to ``put``. It starts the
        close, and the end of the run waits for it. A backend error fails the Workflow Task.
        """
        _write_nowait(self._close(result, failure))

    def _close(self, result: Any, failure: BaseException | None) -> asyncio.Future[int]:
        info = temporalio.workflow.info()
        if (self.workflow_id, self.first_execution_run_id) != (
            info.workflow_id,
            info.first_execution_run_id,
        ):
            # Only the run chain that created a Stream ends it for every reader. The backend
            # refuses it too, and this error comes before a command is spent.
            raise _errors.StreamNotOwnerError(
                f"Can't close the Stream {self.id}, since this run chain didn't create it",
                reason="stream_not_owner",
            )
        run_stream, backend = self._check("close")
        run_stream.closed = True
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

        A cancel closes it with a ``CancelledError`` failure. The block end leaves the Stream
        open in a cron run, since the Stream spans the cron chain and the next run creates it
        again. It also leaves it open on an exception that fails the run when the run has a
        retry policy, since a retry run writes on to the same Stream, and on an exception that
        fails only the Workflow Task, since the task's retry runs the block again. Another
        ``BaseException``, such as an eviction, leaves it open, since the block did not end in
        Workflow code. An explicit :py:meth:`close` always closes.
        """
        if _state().stream(self._model_ref()).closed:
            return
        close, failure = exit_failure(exc)
        if close and _closes_at_block_end(exc):
            await self.close(failure=failure)


def _closes_at_block_end(exc: BaseException | None) -> bool:
    info = temporalio.workflow.info()
    if info.cron_schedule:
        return False
    if exc is None or isinstance(exc, asyncio.CancelledError):
        return True
    if not temporalio.workflow._Runtime.current().workflow_is_failure_exception(exc):
        return False
    return info.retry_policy is None


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
    backend: str | None = None,
) -> Creating[WorkflowStream[E]]:
    """Create a Stream that this run chain owns. See
    :py:func:`temporalio.workflow.create_stream`.
    """

    async def create() -> WorkflowStream[E]:
        stream, task = _create(definition, retention, backend)
        await task
        return stream

    return Creating(create)


def create_stream_nowait(
    definition: type[E],
    *,
    retention: timedelta | None = None,
    backend: str | None = None,
) -> WorkflowStream[E]:
    """Create a Stream that this run chain owns, with no await. See
    :py:func:`temporalio.workflow.create_stream_nowait`.
    """
    stream, task = _create(definition, retention, backend)
    _write_nowait(task)
    return stream


def _create(
    definition: type[E],
    retention: timedelta | None,
    backend: str | None,
) -> tuple[WorkflowStream[E], asyncio.Future[int]]:
    backend_name = _check_backend(backend, "create")
    stream = _own_handle(definition, backend_name)
    run_stream, _ = stream._check("create", backend=backend_name)
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
    # The native server closes the Stream when the run chain ends. Redis ignores it.
    request.close_with_run = True
    run_stream.backend = stream.backend
    return stream, _write(stream.id, CREATE_ACTIVITY, request, backend_name)


def stream_handle(
    definition: type[E], *, backend: str | None = None
) -> WorkflowStream[E]:
    """A Stream that this run chain created, by its definition. See
    :py:func:`temporalio.workflow.stream_handle`.
    """
    stream = _own_handle(definition, backend)
    if not temporalio.workflow.unsafe.is_read_only():
        _state().uses_streams = True
    if backend is None:
        known = _state().streams.get(stream.id)
        stream._backend = (known.backend if known else None) or _check_backend(
            None, "get"
        )
    return stream
