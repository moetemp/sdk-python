"""Streams in Workflow code.

Workflow code creates a Stream with :py:data:`temporalio.workflow.create_stream`. A ref that the
Workflow receives as data works the same through :py:func:`temporalio.workflow.to_stream`, such
as a ref that a run passes on through continue-as-new.

Each create and publish is a Local Activity. Each close is a command that the backend runs after
the Workflow Task commits, and on Redis it's a regular Activity. So each one has a place in the
run's commands. To add, remove or move one in a Workflow that has open runs, gate the change
with ``workflow.patched``, or the replay of those runs fails as nondeterministic. Publishes that
wait behind a write to the same Stream are sent as one Local Activity. If a task fails or the
Worker crashes before the close commits, the Stream stays open for the task's retry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, NamedTuple, TypeVar

import google.protobuf.message

import temporalio.api.stream.v1 as stream_v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.exceptions
import temporalio.nexus.system._support as _support
import temporalio.workflow
from temporalio.streams import _errors
from temporalio.streams._conflict import StreamIDConflictPolicy
from temporalio.streams._offset import StreamOffset
from temporalio.streams._ref import StreamRef
from temporalio.streams._writer import WriteFuture, unretrieved_error
from temporalio.streams.system import models
from temporalio.streams.system.workflow._resources import (
    stream as generated_stream,
)
from temporalio.streams.system.workflow.operations import (
    create_stream as generated_create,
)
from temporalio.workflow._streams import WorkflowStream

T = TypeVar("T")
OutputT = TypeVar("OutputT")

# The create and the append are Local Activities, so History has only their results. The Redis
# close is a regular Activity on the Workflow's task queue.
CREATE_ACTIVITY = "__temporal_streams_create"
APPEND_ACTIVITY = "__temporal_streams_append"
CLOSE_ACTIVITY = "__temporal_streams_close"
_WRITE_TIMEOUT = timedelta(seconds=30)
# Limits of one merged append before the codec. Half of the Store's 4 MiB batch limit leaves room
# for most codecs. The Local Activity splits a merge by call if the codec grows it past the limit.
_MERGE_RECORDS = 1000
_MERGE_BYTES = 2 * 1024 * 1024


@dataclass
class _RunStream:
    # What this run knows about one Stream. Many refs can identify it, so the state is keyed by
    # id.
    ref: models.StreamRef
    closed: bool = False


@dataclass
class _Write:
    # One Local Activity that writes to a Stream, and the calls that it answers. Each call has the
    # Record count up to its own last Record, and its future.
    activity: str
    request: google.protobuf.message.Message
    calls: list[tuple[int, WriteFuture]]
    size: int = 0
    started: bool = False

    def take(self, request: google.protobuf.message.Message) -> bool:
        # An append that waits behind the previous write merges the appends that come after it.
        # So a burst of awaits costs two markers, not one for each await.
        if self.started or not isinstance(request, stream_pb.AppendRecordsRequest):
            return False
        mine = self.request
        if (
            not isinstance(mine, stream_pb.AppendRecordsRequest)
            or mine.ref != request.ref
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
    # The last write of each Stream that hasn't started, which later appends can join.
    waiting: dict[str, _Write] = field(default_factory=dict)
    # Whether the run created, bound or received a Stream. Only then do its Activities get a
    # Producer key, so a run with no Stream adds nothing to History.
    uses_streams: bool = False
    # The async with blocks whose close waits for the run's outcome.
    deferred_closes: list[StreamRef[Any]] = field(default_factory=list)
    # The appends that failed before a caller got the error. A caller can still get it, so
    # the run's end checks them again.
    failed_publishes: list[asyncio.Future[StreamOffset]] = field(default_factory=list)
    # The calls of the generated Workflow API in flight. A call ends after its write, so the
    # run's end waits for both.
    calls: set[asyncio.Future[Any]] = field(default_factory=set)
    # The first error of a write that no caller awaits, and whether the run raised it.
    nowait_error: BaseException | None = None
    nowait_raised: bool = False
    # The error of each Stream's failed create, with the write that carries it on to the writes
    # queued behind it.
    failed_creates: dict[str, tuple[asyncio.Task[None], BaseException]] = field(
        default_factory=dict
    )

    def stream(self, ref: models.StreamRef) -> _RunStream:
        known = self.streams.get(ref.id)
        if known is None:
            known = _RunStream(ref)
            self.streams[ref.id] = known
        return known


def _state() -> _RunState:
    # Keep the state on the run, since its tasks refer to the run's loop and would keep a weak
    # map entry alive.
    runtime = temporalio.workflow._Runtime.current()
    state = getattr(runtime, "_temporal_streams_state", None)
    if state is None:
        state = _RunState()
        setattr(runtime, "_temporal_streams_state", state)
    return state


class WorkerBackend(NamedTuple):
    """The Stream backend of the Worker that runs the Workflow."""

    name: str
    default: bool


def _transport() -> Any:
    # The run's outbound interceptor that the Worker's backend marks, or None.
    outbound = getattr(temporalio.workflow._Runtime.current(), "_outbound", None)
    while outbound is not None:
        if getattr(outbound, "_temporal_stream_transport", False):
            return outbound
        outbound = getattr(outbound, "next", None)
    return None


def worker_backend() -> WorkerBackend | None:
    """The Worker's backend, which marks the run's outbound interceptor, or None."""
    outbound = _transport()
    if outbound is None:
        return None
    return WorkerBackend(
        getattr(outbound, "_temporal_stream_backend_name", ""),
        getattr(outbound, "_temporal_stream_default", False),
    )


async def _close_in_workflow(request: stream_pb.CloseStreamRequest) -> str:
    # The Worker's backend runs the close after the Workflow Task commits. A close writes no
    # Record, so it carries no offset.
    outbound = _transport()
    if outbound is None:
        raise no_worker_backend()
    await outbound._temporal_stream_backend._close_in_workflow(request)
    return ""


def no_worker_backend() -> _errors.StreamConfigError:
    """The error of Workflow code on a Worker with no Stream backend."""
    return _errors.StreamConfigError(
        "No Stream backend is on the Worker. Register one on its client with "
        "stream_backends=."
    )


def _check_backend(backend: str | None, action: str) -> str:
    # These errors come from the Worker's config, not from History. So they fail the Workflow
    # Task, and a Worker with the right config retries it.
    worker = worker_backend()
    if worker is None:
        raise no_worker_backend()
    if not backend:
        if not worker.default:
            raise _errors.no_default_backend(
                "Worker",
                [worker.name],
                "workflow.create_stream[T](id, backend=...)",
            )
        return worker.name
    if backend != worker.name:
        raise _errors.StreamConfigError(
            f"Can't {action} a Stream of the backend {backend}. The Worker has only "
            f"{worker.name}."
        )
    return worker.name


def _records(request: google.protobuf.message.Message) -> int:
    if isinstance(request, stream_pb.AppendRecordsRequest):
        return len(request.records)
    return 0


def _write(
    stream_id: str, activity: str, request: google.protobuf.message.Message
) -> asyncio.Future[StreamOffset]:
    state = _state()
    loop = asyncio.get_running_loop()
    future = WriteFuture(loop=loop)
    waiting = state.waiting.get(stream_id)
    if waiting is not None and waiting.take(request):
        waiting.calls.append((_records(waiting.request), future))
        return future
    write = _Write(activity, request, [(_records(request), future)])
    if isinstance(request, stream_pb.AppendRecordsRequest):
        write.size = sum(record.ByteSize() for record in request.records)
    previous = state.tails.get(stream_id)
    # A write queued behind a running create fails if the create fails. A write that starts
    # after the create failed, such as a create-or-get, goes out.
    behind = previous if previous is not None and not previous.done() else None

    async def run() -> None:
        if previous is not None:
            try:
                await previous
            except BaseException:
                pass
        write.started = True
        if state.waiting.get(stream_id) is write:
            del state.waiting[stream_id]
        failed = state.failed_creates.get(stream_id)
        if failed is not None and failed[0] is behind:
            # The create failed, so the Stream can be another owner's. The write sends nothing,
            # and the writes queued behind it fail the same way.
            state.failed_creates[stream_id] = (task, failed[1])
            for _, call in write.calls:
                if not call.done():
                    call.set_exception(failed[1])
            return
        try:
            if isinstance(write.request, stream_pb.AppendRecordsRequest):
                # The Local Activity returns each merged call's last offset or error.
                counts = [
                    count - before
                    for (before, _), (count, _) in zip(
                        [(0, None), *write.calls], write.calls
                    )
                ]
                answers: list[Any] = await temporalio.workflow.execute_local_activity(
                    activity,
                    args=[write.request, counts],
                    result_type=list[Any],
                    start_to_close_timeout=_WRITE_TIMEOUT,
                )
            elif activity == CLOSE_ACTIVITY:
                assert isinstance(write.request, stream_pb.CloseStreamRequest)
                answers = [await _close_in_workflow(write.request)]
            else:
                answers = [
                    await temporalio.workflow.execute_local_activity(
                        activity,
                        write.request,
                        result_type=str,
                        start_to_close_timeout=_WRITE_TIMEOUT,
                    )
                ]
        except (
            temporalio.exceptions.ActivityError,
            temporalio.exceptions.ApplicationError,
        ) as err:
            if activity == CREATE_ACTIVITY:
                state.failed_creates[stream_id] = (
                    task,
                    _errors.from_failure(err) or err,
                )
            for _, call in write.calls:
                if not call.done():
                    call.set_exception(_errors.from_failure(err) or err)
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
                    _errors.from_reason(int(answer["reason"]), str(answer["message"]))
                )
            else:
                call._set_wire(bytes.fromhex(answer))

    task = loop.create_task(run())
    task.add_done_callback(lambda _: _raise_nowait_error(state))
    state.tails[stream_id] = task
    state.waiting[stream_id] = write
    return future


def _write_nowait(future: asyncio.Future[StreamOffset]) -> None:
    state = _state()

    def done(future: asyncio.Future[StreamOffset]) -> None:
        if future.cancelled() or future.exception() is None:
            return
        if state.nowait_error is None:
            state.nowait_error = future.exception()
        _raise_nowait_error(state)

    future.add_done_callback(done)


def _raise_nowait_error(state: _RunState) -> None:
    # No caller awaits the write, so the run raises its error after this callback, as an error
    # that the run doesn't catch. The result of a write's Local Activity is a command, and no
    # command may follow the run's fail command. So the error waits until no write is in flight.
    if state.nowait_error is None or state.nowait_raised:
        return
    if any(not task.done() for task in (*state.tails.values(), *state.calls)):
        return
    state.nowait_raised = True
    runtime = temporalio.workflow._Runtime.current()
    setattr(runtime, "_current_activation_error", state.nowait_error)


async def drain() -> None:
    """Wait for the run's writes and closes that nobody awaits, before the run ends.

    A publish error that no caller got raises here, as an error that the run doesn't catch. A
    cancel of the run doesn't stop the writes. The run waits for them, then ends as canceled.
    """
    state = _state()
    cancel: asyncio.CancelledError | None = None
    # A Signal or Update handler can start a write while the run waits here.
    while pending := [
        task for task in (*state.tails.values(), *state.calls) if not task.done()
    ]:
        for task in pending:
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as err:
                if not task.done():
                    cancel = err
            except BaseException:
                # A failed write already gave its error to its callers.
                pass
    if cancel is not None:
        raise cancel
    if state.nowait_error is not None and not state.nowait_raised:
        # A write task can end after the run's own end, so its callback would raise the error
        # too late. The run must not complete with the error held.
        state.nowait_raised = True
        raise state.nowait_error
    for future in state.failed_publishes:
        error = unretrieved_error(future)
        if error is not None:
            raise error


def _payload(value: Any) -> Any:
    # Records get no serialization context, so Workflows, Activities and clients encode them
    # alike. The backend runs only the codec.
    runtime = temporalio.workflow._Runtime.current()
    return getattr(runtime, "_context_free_payload_converter").to_payload(value)


def _model_ref(ref: StreamRef[Any]) -> models.StreamRef:
    return models.StreamRef(id=ref.id)


def _proto_ref(ref: StreamRef[Any]) -> stream_v1.StreamRef:
    return models._StreamRefTransferTypeConverter().to_transfer_type(_model_ref(ref))


def _generated_ref(ref: StreamRef[Any]) -> models.StreamRef:
    return models.StreamRef(id=ref.id, backend=ref.backend or None)


def _generated(ref: StreamRef[Any]) -> generated_stream.Stream[Any]:
    # The generated factory takes the ref's fields.
    return generated_stream.get_stream(id=ref.id, backend=ref.backend or None)


def _check(
    ref: StreamRef[Any], action: str, *, backend_checked: bool = False
) -> _RunStream:
    if temporalio.workflow.unsafe.is_read_only():
        raise temporalio.workflow.ReadOnlyContextError(
            f"Can't {action} a Stream in read-only code"
        )
    state = _state()
    state.uses_streams = True
    run_stream = state.stream(_model_ref(ref))
    if run_stream.closed:
        raise _errors.closed(
            f"Can't {action} the Stream {ref.id}, since this run closed it"
        )
    if not backend_checked:
        _check_backend(ref.backend, action)
    return run_stream


def _start_append(
    ref: StreamRef[Any], values: tuple[Any, ...]
) -> asyncio.Future[StreamOffset]:
    # The checks raise at the call, as the write's place in the order does.
    if not values:
        raise TypeError("append takes one or more values")
    _check(ref, "write to")
    call = _support.start_now(
        generated_stream.append_records(ref=_generated_ref(ref), records=list(values))
    )
    return _call_future(call, lambda offset: offset)


def _append(
    ref: StreamRef[Any], values: tuple[Any, ...]
) -> asyncio.Future[StreamOffset]:
    if not values:
        raise TypeError("append takes one or more values")
    _check(ref, "write to")
    records = [stream_v1.AppendRecord(body=_payload(v)) for v in values]
    return _write(
        ref.id,
        APPEND_ACTIVITY,
        stream_pb.AppendRecordsRequest(ref=_proto_ref(ref), records=records),
    )


def _start_close(ref: StreamRef[Any]) -> asyncio.Future[StreamOffset]:
    _closing(ref)
    call = _support.start_now(generated_stream.close_stream(ref=_generated_ref(ref)))
    return _call_future(call, lambda _: b"")


def _closing(ref: StreamRef[Any]) -> _RunStream:
    return _check(ref, "close")


def _close(ref: StreamRef[Any]) -> asyncio.Future[StreamOffset]:
    _closing(ref).closed = True
    return _write(
        ref.id, CLOSE_ACTIVITY, stream_pb.CloseStreamRequest(ref=_proto_ref(ref))
    )


def append(
    ref: StreamRef[Any], values: tuple[Any, ...]
) -> asyncio.Future[StreamOffset]:
    """Start the write Local Activity. An error that no caller gets fails the run.

    See :py:meth:`temporalio.workflow.WorkflowStream.append`.
    """
    state = _state()
    future = _start_append(ref, values)
    loop = asyncio.get_running_loop()

    def warn(future: asyncio.Future[StreamOffset]) -> None:
        error = unretrieved_error(future)
        if error is not None:
            temporalio.workflow.logger.warning(
                "An append to the Stream %s failed, and no caller has its error. If no "
                "caller gets the error, it fails the run when the run ends: %s",
                ref.id,
                error,
            )

    def failed(future: asyncio.Future[StreamOffset]) -> None:
        if unretrieved_error(future) is not None:
            state.failed_publishes.append(future)
            # A caller that already waits for the future gets the error before warn runs.
            loop.call_soon(warn, future)

    future.add_done_callback(failed)
    return future


async def close(ref: StreamRef[Any]) -> None:
    """Close the Stream after the writes before it, once the Workflow Task commits."""
    # The generated Workflow API makes the call, and the Worker's interceptor runs it as the
    # close write.
    await _generated(ref).close()


async def exit_block(ref: StreamRef[Any], exc: BaseException | None) -> None:
    """Close the Stream at the end of an ``async with`` block, or defer the close."""
    state = _state()
    if state.stream(_model_ref(ref)).closed:
        return
    # A run that continues as new has no retry run, so its block end closes the Stream.
    continued = isinstance(exc, temporalio.workflow.ContinueAsNewError)
    if temporalio.workflow.info().cron_schedule:
        return
    if continued or _closes_at_block_end(exc):
        await close(ref)
    else:
        # A refused close raises here, as it does for a close at the block end.
        _closing(ref)
        state.deferred_closes.append(ref)


def append_nowait(ref: StreamRef[Any], values: tuple[Any, ...]) -> None:
    """Start the write. Its error raises in the run as an error that the run doesn't catch."""
    _write_nowait(_start_append(ref, values))


def close_nowait(ref: StreamRef[Any]) -> None:
    """Start the close. The end of the run waits for it."""
    _write_nowait(_start_close(ref))


def _closes_at_block_end(exc: BaseException | None) -> bool:
    # For other exceptions, only the run's end shows whether a retry run will write to the Stream.
    if exc is None or isinstance(exc, asyncio.CancelledError) or _cancelled(exc):
        return True
    runtime = temporalio.workflow._Runtime.current()
    return runtime.workflow_is_failure_exception(exc) and not _retried(exc)


def close_at_run_end(err: BaseException | None) -> None:
    """Close the Streams whose block end deferred the close, unless a retry run comes next.

    ``err`` is the exception that ends the run, or None if the run returns.
    """
    state = _state()
    runtime = temporalio.workflow._Runtime.current()
    if (
        isinstance(err, Exception)
        and not _cancelled(err)
        and runtime.workflow_is_failure_exception(err)
        and _retried(err)
    ):
        return
    for ref in state.deferred_closes:
        if not state.stream(_model_ref(ref)).closed:
            _write_nowait(_start_close(ref))


def _retried(exc: BaseException) -> bool:
    # Whether the server starts a retry run after the run fails with exc, by the server's rule.
    info = temporalio.workflow.info()
    policy = info.retry_policy
    if policy is None or 0 < policy.maximum_attempts <= info.attempt:
        return False
    failure_type: str | None
    if isinstance(exc, temporalio.exceptions.ApplicationError):
        if exc.non_retryable:
            return False
        failure_type = exc.type
    elif isinstance(
        exc,
        (temporalio.exceptions.CancelledError, temporalio.exceptions.TerminatedError),
    ):
        return False
    elif isinstance(exc, temporalio.exceptions.TimeoutError):
        return exc.type in (
            temporalio.exceptions.TimeoutType.START_TO_CLOSE,
            temporalio.exceptions.TimeoutType.HEARTBEAT,
        )
    elif isinstance(exc, temporalio.exceptions.FailureError):
        return True
    else:
        # For any other exception, the run's failure has the class name as its type.
        failure_type = type(exc).__name__
    return failure_type not in (policy.non_retryable_error_types or ())


def _cancelled(exc: BaseException) -> bool:
    # A cancelled run ends as canceled with no retry, even when the cancel reached the block as
    # the error of an Activity, a Local Activity or a child.
    return (
        temporalio.workflow.cancellation_reason() is not None
        and temporalio.exceptions.is_cancelled_exception(exc)
    )


def _models_policy(
    policy: StreamIDConflictPolicy,
) -> models.StreamIdConflictPolicy:
    return models.StreamIdConflictPolicy(int(policy))


async def create_stream(
    payload_type: type[T] | None,
    id: str,
    *,
    backend: str | None,
    id_conflict_policy: StreamIDConflictPolicy,
) -> WorkflowStream[T]:
    """Create a standalone Stream.

    See :py:data:`temporalio.workflow.create_stream`.
    """
    ref: StreamRef[T] = _creating(id, backend)
    await generated_create.create_stream[payload_type or object](
        id=id,
        backend=ref.backend,
        id_conflict_policy=_models_policy(id_conflict_policy),
    )
    return WorkflowStream(ref)


def create_stream_nowait(
    payload_type: type[T] | None,
    id: str,
    *,
    backend: str | None,
    id_conflict_policy: StreamIDConflictPolicy,
) -> WorkflowStream[T]:
    """Create a standalone Stream without an await.

    See :py:data:`temporalio.workflow.create_stream_nowait`.
    """
    ref: StreamRef[T] = _creating(id, backend)
    call = _support.start_now(
        generated_create.create_stream[payload_type or object](
            id=id,
            backend=ref.backend,
            id_conflict_policy=_models_policy(id_conflict_policy),
        )
    )
    _write_nowait(_call_future(call, lambda _: b""))
    return WorkflowStream(ref)


def _creating(id: str, backend: str | None) -> StreamRef[Any]:
    # The checks raise before the generated call. A Workflow creates only standalone Streams, so
    # the ref has no owner.
    if not id:
        raise ValueError("The Stream id is empty")
    ref: StreamRef[Any] = StreamRef(id=id, backend=_check_backend(backend, "create"))
    _check(ref, "create", backend_checked=True)
    return ref


def _call_future(
    call: asyncio.Task[Any], result: Callable[[Any], bytes]
) -> asyncio.Future[StreamOffset]:
    # start_now runs a call of the generated Workflow API up to its first wait, so its write
    # takes its place in the order at once. The call gives the write's result, or the handle
    # that gives it. The caller's future follows both, and the run's end waits for them.
    loop = asyncio.get_running_loop()
    future = WriteFuture(loop=loop)

    async def follow() -> None:
        # The task sets the future before it ends, so the run's end sees a failed append.
        try:
            started = await call
            if isinstance(started, temporalio.workflow.NexusOperationHandle):
                started = await started
        except asyncio.CancelledError:
            future.cancel()
            raise
        except BaseException as err:
            future.set_exception(_errors.from_failure(err) or err)
        else:
            future._set_wire(result(started))

    task = loop.create_task(follow())
    state = _state()
    state.calls.add(task)
    task.add_done_callback(state.calls.discard)
    task.add_done_callback(lambda _: _raise_nowait_error(state))
    return future


def _start_create(
    ref: StreamRef[Any], request: models.CreateStreamRequest
) -> asyncio.Future[StreamOffset]:
    _check(ref, "create", backend_checked=True)
    proto = models._CreateStreamRequestTransferTypeConverter().to_transfer_type(request)
    proto.stream_ref.CopyFrom(_proto_ref(ref))
    # A later run of the chain repeats the same create, so the id is the same in each run. The
    # Store then gives a retry run or a cron run the Stream that the first run made.
    info = temporalio.workflow.info()
    proto.request_id = f"{info.first_execution_run_id}:{ref.id}"
    return _write(ref.id, CREATE_ACTIVITY, proto)


class _WriteHandle(temporalio.workflow.NexusOperationHandle[OutputT]):
    # The interceptor runs a call of the generated Workflow API as a write Local Activity, so
    # the call's handle waits for the write.
    def __init__(
        self, write: asyncio.Future[StreamOffset], result: Callable[[bytes], OutputT]
    ) -> None:
        self._write = write
        self._result = result

    def cancel(self) -> bool:
        # A write keeps its place in the Stream's order, so it can't be cancelled.
        return False

    def __await__(self) -> Generator[Any, Any, OutputT]:
        return self._wait().__await__()

    async def _wait(self) -> OutputT:
        return self._result((await self._write)._wire)

    @property
    def operation_token(self) -> str | None:
        return None


def _from_model(ref: models.StreamRef, backend: str | None) -> StreamRef[Any]:
    return StreamRef(id=ref.id, backend=backend)


# The Worker's interceptor runs the Stream calls of the generated Workflow API with these. They
# build the same Local Activity requests as the Streams, so History doesn't change. The write
# Activity sends its own writer.


def run_create_stream(
    request: models.CreateStreamRequest,
) -> temporalio.workflow.NexusOperationHandle[models.CreateStreamResult]:
    """Run a create of the generated Workflow API as the create Local Activity."""
    model_ref = request.stream_ref
    stream = _from_model(model_ref, _check_backend(model_ref.backend, "create"))
    write = _start_create(stream, request)
    return _WriteHandle(
        write, lambda _: models.CreateStreamResult(ref=_generated_ref(stream))
    )


def run_append_records(
    request: models.AppendRecordsRequest[Any],
) -> temporalio.workflow.NexusOperationHandle[models.AppendRecordsResponse]:
    """Run an append of the generated Workflow API as the append Local Activity."""
    stream = _from_model(request.ref, request.ref.backend)
    write = _append(stream, tuple(record.body for record in request.records))
    return _WriteHandle(
        write, lambda offset: models.AppendRecordsResponse(last_offset=offset)
    )


def run_close_stream(
    request: models.CloseStreamRequest,
) -> temporalio.workflow.NexusOperationHandle[models.CloseStreamResponse]:
    """Run a close of the generated Workflow API as the close write."""
    stream = _from_model(request.ref, request.ref.backend)
    write = _close(stream)
    return _WriteHandle(write, lambda _: models.CloseStreamResponse())


def no_workflow_reads() -> _errors.StreamError:
    """The error of a read or a describe in Workflow code."""
    return _errors.unsupported(
        "Workflow code can't read a Stream. Read it from an Activity."
    )


def get_stream(ref: StreamRef[T]) -> WorkflowStream[T]:
    """Get the Stream of a ref that came as data.

    See :py:func:`temporalio.workflow.to_stream`.
    """
    if not temporalio.workflow.unsafe.is_read_only():
        _state().uses_streams = True
    return WorkflowStream(ref)
