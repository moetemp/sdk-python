"""The native Stream plugin, whose Streams the Temporal server keeps.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import asyncio
import dataclasses
import itertools
import weakref
<<<<<<< HEAD
from collections.abc import Coroutine, Generator
from datetime import timedelta
=======
from collections.abc import Callable, Coroutine, Generator
>>>>>>> moe/jetty/v2/j2-native-plugin
from typing import Any, NoReturn, TypeVar

import google.protobuf.message

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
import temporalio.converter
import temporalio.exceptions
import temporalio.service
import temporalio.worker
import temporalio.workflow
from temporalio.contrib.streams import _activity, _errors, _workflow_reader
from temporalio.contrib.streams import workflow as streams_workflow
from temporalio.contrib.streams._backend import _REQUESTS, _RESPONSES, StreamBackend
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import (
    _SERVICE,
    _activity_streams,
    _close_quietly,
    _DoneHandle,
    _fails_run,
    _owned_ref,
    _take_owned_streams,
    _targets,
    chain_continues,
    check_worker_backends,
    enter_attempt,
    pass_owned_streams,
)
from temporalio.contrib.streams.system import client as service_client
from temporalio.contrib.streams.system import models
from temporalio.plugin import SimplePlugin
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

_M = TypeVar("_M", bound=google.protobuf.message.Message)

# How long a Workflow read asks the server to park when the Stream has no new Record. The
# operation is async, so the wait costs no Worker resource.
_READ_WAIT = timedelta(seconds=60)
# The writer ids of this process. A native writer lives only in the process.
_WRITER_IDS = itertools.count(1)

_BY_CODE: dict[temporalio.service.RPCStatusCode, str] = {
    temporalio.service.RPCStatusCode.NOT_FOUND: "stream_not_found",
    temporalio.service.RPCStatusCode.UNIMPLEMENTED: "stream_unsupported",
    temporalio.service.RPCStatusCode.OUT_OF_RANGE: "stream_offset",
    temporalio.service.RPCStatusCode.UNAVAILABLE: "stream_storage",
}


def _from_rpc_error(err: temporalio.service.RPCError) -> _errors.StreamError:
    # The server puts a StreamErrorDetails in the status details. With none, the code gives
    # the reason.
    mapped = _errors.from_rpc_error(err)
    if isinstance(mapped, _errors.StreamError):
        return mapped
    # A server may also lead the message with the reason, such as `stream_closed: ...`.
    token = err.message.split(":", 1)[0].strip()
    reason = token if token in _errors._BY_REASON else _BY_CODE.get(err.status, "")
    return _errors._BY_REASON.get(reason, _errors.StreamError)(
        err.message, reason=reason, code=err.status
    )


@dataclasses.dataclass
class _Writer:
    namespace: str
    stream: stream_pb.StreamRef
    producer: stream_pb.Producer
    sequence: int = 0


class NativeStreamBackend(StreamBackend):
    """Carries the Stream calls of Activities and clients to the server's Stream service.

    It calls ``temporal.api.streamservice.v1.StreamService`` on the frontend over the client's
    connection. A writer is a Producer and a batch number, kept in the process.

    .. warning::
        This API is experimental.
    """

    def __init__(  # pyright: ignore[reportMissingSuperCall]
        self, client: temporalio.client.Client | None = None
    ) -> None:
        """Create a backend over a client's connection."""
        self._client = client
        self._writers: dict[int, _Writer] = {}

    def for_client(self, client: temporalio.client.Client) -> NativeStreamBackend:
        """The backend over this client's connection."""
        if self._client is client:
            return self
        return NativeStreamBackend(client)

    def service(
        self,
        namespace: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> service_client.StreamServiceClient:
        """A generated Stream service client for ``namespace``, over the frontend."""
        return service_client.StreamServiceClient(
            _GrpcTransport(self, namespace), data_converter=data_converter
        )

    def call(self, method: str, request: google.protobuf.message.Message) -> bytes:
        """Native Streams have no blocking calls, so this raises."""
        raise _errors.StreamUnsupportedError(
            f"Native Streams have no blocking call {method}",
            reason="stream_unsupported",
        )

    async def call_async(
        self, method: str, request: google.protobuf.message.Message, response: type[_M]
    ) -> _M:
        """Call one of the five Stream service methods on the frontend."""
        if method not in _REQUESTS:
            raise _errors.StreamUnsupportedError(
                f"Native Streams have no call {method}", reason="stream_unsupported"
            )
        client = self._client
        if client is None:
            raise _errors.StreamConfigError(
                "This native backend has no client. Use the plugin on a client."
            )
        namespace = getattr(request, "namespace", "") or client.namespace
        try:
            return await client.service_client.call_unary(
                f"/{_SERVICE}/{method}",
                request,
                response,
                metadata={"temporal-namespace": namespace},
            )
        except temporalio.service.RPCError as err:
            raise _from_rpc_error(err) from None

    async def open_writer(self, request: ext_pb.OpenWriterRequest) -> int:
        """Keep a Producer for a Stream, and return its writer id. It makes no call."""
        writer_id = next(_WRITER_IDS)
        self._writers[writer_id] = _Writer(
            request.namespace, request.stream, request.producer
        )
        return writer_id

    async def write_records(
        self, writer_id: int, records: list[stream_pb.AppendRecord]
    ) -> int:
        """Append Records as the writer's next batch. Returns the last Record's offset."""
        writer = self._writers[writer_id]
        # The Producer and the batch number are the server's dedupe key for a retried call.
        writer.sequence += 1
        response = await self.call_async(
            "AppendRecords",
            stream_pb.AppendRecordsRequest(
                namespace=writer.namespace,
                stream=writer.stream,
                producer=writer.producer,
                sequence=writer.sequence,
                records=records,
            ),
            stream_pb.AppendRecordsResponse,
        )
        return response.last_offset

    async def flush_run_buffer(
        self, namespace: str, run_id: str, workflow_task: stream_pb.WorkflowTaskEpoch
    ) -> int:
        """Workflow calls go to the server as commands, so a run has no buffer."""
        return 0

    async def close_writer(self, writer_id: int) -> int:
        """Drop a writer. Each write already went out, so it writes nothing."""
        self._writers.pop(writer_id, None)
        return 0

    async def close_writers(self) -> int:
        """Drop every writer of the backend."""
        self._writers.clear()
        return 0


class _GrpcTransport:
    # The generated client takes a transport, so the five operations reach the frontend as
    # binary protobuf.
    def __init__(self, backend: NativeStreamBackend, namespace: str) -> None:
        self._backend = backend
        self._namespace = namespace

    async def call(
        self,
        *,
        service: str,  # pyright: ignore[reportUnusedParameter]
        operation: str,
        input: temporalio.api.common.v1.Payload,
    ) -> temporalio.api.common.v1.Payload:
        request = _REQUESTS[operation].FromString(input.data)
        # The IDL leaves the namespace out, and the frontend routes by it.
        setattr(request, "namespace", self._namespace)
        response = await self._backend.call_async(
            operation, request, _RESPONSES[operation]
        )
        return temporalio.api.common.v1.Payload(
            metadata={
                "encoding": b"binary/protobuf",
                "messageType": response.DESCRIPTOR.full_name.encode(),
            },
            data=response.SerializeToString(),
        )


class NativeStreamPlugin(SimplePlugin):
    """Carries Stream calls to the Temporal server, which keeps the Streams.

    Workflow code sends each call to the server as a System Nexus command, with no Worker
    extension, so its results are in History. Activities and clients call the frontend's
    ``temporal.api.streamservice.v1.StreamService``. It registers the backend name ``native``.
    Put it on the client next to ``RedisStreamPlugin`` to keep old Streams on Redis and create
    new ones on the server.

    .. warning::
        This API is experimental.
    """

    def __init__(self, *, name: str = "native", default: bool = False) -> None:
        """Create the plugin. It makes no call.

        Args:
            name: The backend name that Stream handles carry.
            default: Make this the default backend when the client or Worker has more than
                one Stream plugin. With none marked, the first one is the default.
        """
        self._backend_name = name
        self._default = default
        self._temporal_stream_backend = NativeStreamBackend()
        self._worker_client: temporalio.client.Client | None = None
        self._converter = temporalio.converter.DataConverter.default
        super().__init__(
            "temporalio.NativeStreamPlugin", interceptors=[_NativeInterceptor(self)]
        )

    @property
    def backend_name(self) -> str:
        """The name that Stream handles carry for this backend."""
        return self._backend_name

    @property
    def default(self) -> bool:
        """Whether this is the default backend when there is more than one."""
        return self._default

    def configure_worker(
        self, config: temporalio.worker.WorkerConfig
    ) -> temporalio.worker.WorkerConfig:
        """Add the plugin's interceptors. A native backend needs no extension."""
        config = super().configure_worker(config)
        check_worker_backends(config)
        # A sync Activity has no client, so the Worker's client carries its calls.
        client = config.get("client")
        if client is not None:
            self._worker_client = client
            self._converter = client.data_converter
        runner = config.get("workflow_runner")
        if isinstance(runner, SandboxedWorkflowRunner):
            # The interceptor and the facade must share the run's Stream state.
            config["workflow_runner"] = dataclasses.replace(
                runner,
                restrictions=runner.restrictions.with_passthrough_modules(
                    "temporalio.contrib.streams"
                ),
            )
        return config


class _NativeInterceptor(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    def __init__(self, plugin: NativeStreamPlugin) -> None:
        super().__init__()
        self._plugin = plugin
        self._inbound_class = type(
            "_NativeWorkflowInbound",
            (_NativeWorkflowInbound,),
            {"_backend_name": plugin.backend_name, "_default": plugin.default},
        )

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        return self._inbound_class

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _NativeActivityInbound(next, self._plugin)


class _NativeWorkflowInbound(temporalio.worker.WorkflowInboundInterceptor):
    _backend_name = ""
    _default = False

    def __init__(self, next: temporalio.worker.WorkflowInboundInterceptor) -> None:
        super().__init__(next)
        self._next_outbound: temporalio.worker.WorkflowOutboundInterceptor | None = None

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        self._next_outbound = outbound
        super().init(
            _NativeWorkflowOutbound(outbound, self._backend_name, self._default)
        )

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        _take_owned_streams(input.headers)
        try:
            result = await super().execute_workflow(input)
        except temporalio.workflow.ContinueAsNewError:
            # The next run goes on with the Streams, after this run's calls went out.
            await _drain()
            raise
        except BaseException as err:
            if not isinstance(err, (Exception, asyncio.CancelledError)):
                raise
            if (
                isinstance(err, asyncio.CancelledError)
                and temporalio.workflow.cancellation_reason() is not None
            ):
                self._close_with_run(
                    failure=temporalio.exceptions.CancelledError("Workflow cancelled")
                )
            elif not _fails_run(err):
                # The Workflow Task fails and the run goes on, so nothing is sent.
                raise
            elif not chain_continues(
                temporalio.workflow.info(), temporalio.workflow.now(), err
            ):
                self._close_with_run(failure=err)
            await _drain()
            raise
        if not chain_continues(
            temporalio.workflow.info(), temporalio.workflow.now(), None
        ):
            self._close_with_run(result=result)
        await _drain()
        return result

    def _close_with_run(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        assert self._next_outbound is not None
        default = streams_workflow._backend_name()
        for run_stream in streams_workflow._state().streams.values():
            if not run_stream.close_with_run or run_stream.closed:
                continue
            if (run_stream.backend or default) != self._backend_name:
                continue
            run_stream.closed = True
            _enqueue(
                self._next_outbound,
                self._backend_name,
                run_stream.ref.id,
                _operation_input(
                    "CloseStream",
                    models.CloseStreamRequest(
                        stream=run_stream.ref, result=result, failure=failure
                    ),
                    models.CloseStreamResponse,
                ),
            )


@dataclasses.dataclass
class _Queue:
    # One Stream's calls of a run. The server runs each operation on its own, so one at a
    # time keeps the run's order.
    waiting: list[temporalio.worker.StartNexusOperationInput[Any, Any]] = (
        dataclasses.field(default_factory=list)
    )
    busy: bool = False


# One set of queues per Workflow instance, as the facade keeps its run state.
_queues: weakref.WeakKeyDictionary[Any, dict[str, _Queue]] = weakref.WeakKeyDictionary()


def _run_queues() -> dict[str, _Queue]:
    runtime = temporalio.workflow._Runtime.current()
    queues = _queues.get(runtime)
    if queues is None:
        queues = {}
        _queues[runtime] = queues
    return queues


def _enqueue(
    next_outbound: temporalio.worker.WorkflowOutboundInterceptor,
    backend: str,
    stream_id: str,
    input: temporalio.worker.StartNexusOperationInput[Any, Any],
) -> None:
    queue = _run_queues().setdefault(stream_id, _Queue())
    queue.waiting.append(input)
    if not queue.busy:
        _send_next(next_outbound, backend, queue)


def _send_next(
    next_outbound: temporalio.worker.WorkflowOutboundInterceptor,
    backend: str,
    queue: _Queue,
) -> None:
    if not queue.waiting:
        return
    input = _merged(queue.waiting)
    queue.busy = True

    async def call() -> None:
        try:
            handle = await next_outbound.start_system_nexus_operation(input)
            await handle
        except Exception:
            # A live run and its replay get the same answer from History, so a failed
            # write is dropped in both, as the Redis backend drops a write to a Stream
            # closed elsewhere.
            temporalio.workflow.logger.warning(
                "A native Stream call of the Workflow failed", exc_info=True
            )
        queue.busy = False
        _send_next(next_outbound, backend, queue)

    # Another Stream plugin further down the chain passes a call of this backend on.
    token = streams_workflow._target.set(backend)
    try:
        _schedule_now(call())
    finally:
        streams_workflow._target.reset(token)


def _merged(
    waiting: list[temporalio.worker.StartNexusOperationInput[Any, Any]],
) -> temporalio.worker.StartNexusOperationInput[Any, Any]:
    # The appends that waited behind a call go as one, in order.
    first = waiting.pop(0)
    if first.operation_name != "AppendRecords":
        return first
    records = list(first.input.records or [])
    while waiting and waiting[0].operation_name == "AppendRecords":
        records.extend(waiting.pop(0).input.records or [])
    return dataclasses.replace(
        first, input=dataclasses.replace(first.input, records=records)
    )


def _idle() -> bool:
    return all(not queue.busy and not queue.waiting for queue in _run_queues().values())


async def _drain() -> None:
    # Waits until the run's Stream calls completed, so a later step does not pass them.
    if not _idle():
        await temporalio.workflow.wait_condition(_idle)


def _operation_input(
    operation: str, request: Any, output_type: type[Any]
) -> temporalio.worker.StartNexusOperationInput[Any, Any]:
    from temporalio.nexus.system import TEMPORAL_SYSTEM_ENDPOINT

    return temporalio.worker.StartNexusOperationInput(
        endpoint=TEMPORAL_SYSTEM_ENDPOINT,
        service=_SERVICE,
        operation=operation,
        input=request,
        output_type=output_type,
        schedule_to_close_timeout=None,
        schedule_to_start_timeout=None,
        start_to_close_timeout=None,
        cancellation_type=temporalio.workflow.NexusOperationCancellationType.WAIT_COMPLETED,
        headers=None,
        summary=None,
    )


class _NativeWorkflowOutbound(temporalio.worker.WorkflowOutboundInterceptor):
    _temporal_stream_transport = True
    # The server parks a read at the end of the Stream, and completes it on an append or a
    # close, so a reader needs no timer.
    _temporal_stream_parks_reads = True
    _temporal_stream_read_wait = _READ_WAIT

    def __init__(
        self,
        next: temporalio.worker.WorkflowOutboundInterceptor,
        backend_name: str,
        default: bool,
    ) -> None:
        super().__init__(next)
        self._temporal_stream_backend_name = backend_name
        self._temporal_stream_default = default

    async def start_system_nexus_operation(
        self, input: temporalio.worker.StartNexusOperationInput[Any, Any]
    ) -> temporalio.workflow.NexusOperationHandle[Any]:
        if input.service != _SERVICE or not _targets(
            self._temporal_stream_backend_name
        ):
            return await super().start_system_nexus_operation(input)
        operation = input.operation_name
        if operation in ("ReadRecords", "DescribeStream"):
            # A read's answer is in History, so Workflow code awaits it.
            return await super().start_system_nexus_operation(input)
        if operation == "CreateStream":
            result: Any = models.CreateStreamResponse(
                stream=_owned_ref(input.input.stream_id)
            )
        elif operation == "AppendRecords":
            result = models.AppendRecordsResponse()
        elif operation == "CloseStream":
            result = models.CloseStreamResponse()
        else:
            raise _errors.StreamError(f"Workflow code can't call {operation}")
        # The facade answers at once, as on Redis. The call goes out now, or after the
        # Stream's call in flight. Its answer has nothing that Workflow code needs.
        stream_id = (
            input.input.stream_id
            if operation == "CreateStream"
            else input.input.stream.id
        )
        _enqueue(self.next, self._temporal_stream_backend_name, stream_id, input)
        return _DoneHandle(result)

    async def _temporal_stream_read_page(
        self, request: stream_pb.ReadRecordsRequest
    ) -> stream_pb.ReadRecordsResponse:
        # The read sees the run's own writes, since it starts after they completed.
        await _drain()
        # Another Stream plugin further down the chain passes a call of this backend on.
        token = streams_workflow._target.set(self._temporal_stream_backend_name)
        try:
            handle = await self.next.start_system_nexus_operation(
                _operation_input("ReadRecords", request, stream_pb.ReadRecordsResponse)
            )
        finally:
            streams_workflow._target.reset(token)
        try:
            return await handle
        except temporalio.exceptions.NexusOperationError as err:
            raise _workflow_reader.error_of(err) from None

    def continue_as_new(self, input: temporalio.worker.ContinueAsNewInput) -> NoReturn:
        pass_owned_streams(input)
        super().continue_as_new(input)

    # The server runs each Stream operation on its own, after the Workflow Task. So an
    # Activity, a child or a Nexus operation that the run starts next waits until the run's
    # Stream calls completed. It then sees the Stream that the run created, and the Records
    # before its own, as on Redis.

    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        if _idle():
            return super().start_activity(input)
        return _AfterStreamCalls(
            lambda: super(_NativeWorkflowOutbound, self).start_activity(input)
        )

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        if _idle():
            return super().start_local_activity(input)
        return _AfterStreamCalls(
            lambda: super(_NativeWorkflowOutbound, self).start_local_activity(input)
        )

    async def start_child_workflow(
        self, input: temporalio.worker.StartChildWorkflowInput
    ) -> temporalio.workflow.ChildWorkflowHandle[Any, Any]:
        await _drain()
        return await super().start_child_workflow(input)

    async def start_nexus_operation(
        self, input: temporalio.worker.StartNexusOperationInput[Any, Any]
    ) -> temporalio.workflow.NexusOperationHandle[Any]:
        await _drain()
        return await super().start_nexus_operation(input)


class _AfterStreamCalls(temporalio.workflow.ActivityHandle[Any]):
    # Starts the Activity once the run's Stream calls completed. A cancel before then cancels
    # the wait, and after it the Activity.
    def __init__(
        self, start: Callable[[], temporalio.workflow.ActivityHandle[Any]]
    ) -> None:
        async def run() -> Any:
            await _drain()
            return await start()

        super().__init__(run())


def _schedule_now(call: Coroutine[Any, Any, None]) -> None:
    # The SDK adds the schedule command in the call's first step. Taking that step here puts
    # the command in the order of the Workflow's calls, also before a completion in this step.
    try:
        first = call.send(None)
    except StopIteration:
        return
    asyncio.ensure_future(_Resumed(call, first))


class _Resumed:
    # Goes on with a coroutine whose first step already ran, as a task would.
    def __init__(self, call: Coroutine[Any, Any, None], first: Any) -> None:
        self._call = call
        self._first = first

    def __await__(self) -> Generator[Any, Any, None]:
        yielded = self._first
        while True:
            try:
                sent = yield yielded
            except BaseException as err:
                try:
                    yielded = self._call.throw(err)
                except StopIteration:
                    return
            else:
                try:
                    yielded = self._call.send(sent)
                except StopIteration:
                    return


class _NativeActivityInbound(temporalio.worker.ActivityInboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.ActivityInboundInterceptor,
        plugin: NativeStreamPlugin,
    ) -> None:
        super().__init__(next)
        self._plugin = plugin
        self._temporal_stream_backend_name = plugin.backend_name
        self._temporal_stream_default = plugin.default

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        streams = _activity_streams(input, self._plugin)
        client = _activity_client() or self._plugin._worker_client
        if client is not None:
            streams.backend = self._plugin._temporal_stream_backend.for_client(client)
        _attempt, token = enter_attempt(self, streams)
        try:
            result = await super().execute_activity(input)
        except BaseException:
            await _close_quietly(streams)
            raise
        else:
            await streams.close()
            return result
        finally:
            if token is not None:
                _activity.current.reset(token)


def _activity_client() -> temporalio.client.Client | None:
    try:
        return temporalio.activity.client()
    except RuntimeError:
        return None
