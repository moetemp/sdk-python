"""The Stream backends that call the temporalio-streams runtime in the bridge."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import weakref
from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from typing import Any, NoReturn, cast

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.bridge.streams
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.runtime
import temporalio.service
import temporalio.worker
import temporalio.workflow
from temporalio.common._streams import (
    _activity,
    _errors,
    _ids,
    _workflow_reader,
    _writes,
)
from temporalio.common._streams import _workflow as streams_workflow
from temporalio.common._streams._backend import BackendCalls, _ExtensionCalls
from temporalio.common._streams._errors import StreamConfigError
from temporalio.common._streams._handle import StreamHandle
from temporalio.common._streams._plugin import StreamBackend
from temporalio.common._streams.proto import v1 as ext_pb
from temporalio.common._streams.system import models
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)

# How long a Workflow read's Local Activity waits for new Records or the create, below Core's 8 s
# task heartbeat. The Store waits for both as one wait, so the timeout's margin covers the calls.
_READ_WAIT = timedelta(seconds=5)
_READ_MARGIN = timedelta(seconds=10)
_PAGE_TYPE = stream_pb.ReadRecordsResponse.DESCRIPTOR.full_name.encode()
# A Workflow page's marker holds its Records, and the server refuses a marker over 2 MiB. The
# cap leaves room for the marker's envelope.
_PAGE_BYTES = 2 * 1024 * 1024 - 64 * 1024
# Set on a page of a Stream whose owner's run chain ended. Only a Store that doesn't close a
# Stream with its owner sets it.
_OWNER_ENDED_KEY = "temporal.io/stream-owner-ended"
# The backend puts this key into the headers of each Local Activity that a Workflow schedules,
# and of each Activity of a run that uses Streams. It keeps the Producer id the same on retries.
_KEY_HEADER = "__temporal_stream_key"
# Continue-as-new passes the run chain's open Streams to the next run in this header.
_OWNED_STREAMS_HEADER = "__temporal_owned_streams"
_CLOSE_TIMEOUT = timedelta(seconds=30)
# Only a Store error is retryable. Each Stream error sets its own non_retryable flag.
_CLOSE_RETRY = temporalio.common.RetryPolicy(
    initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=30)
)


class ExtensionStreamBackend(StreamBackend):
    """Base class of the Redis, memory and native backends, which call the Stream runtime.

    .. warning::
        This API is experimental.
    """

    _outbound_class: type[_WorkflowOutbound]

    def __init__(
        self,
        name: str,
        memory: bool,
        config: ext_pb.RedisStreamConfig | ext_pb.NativeStreamConfig,
        runtime: temporalio.runtime.Runtime | None,
        backend_name: str,
        default: bool,
    ) -> None:
        """Create the backend over the bridge's Stream runtime, on Redis or in memory.

        ``backend_name`` is the name that Stream handles store for this backend. ``default``
        makes it the backend of a create with no backend name.
        """
        config.backend = backend_name
        self._config = config.SerializeToString()
        self._backends: weakref.WeakSet[_ExtensionCalls] = weakref.WeakSet()
        # Each client gets its own runtime object on Redis. The memory Store has one for the
        # backend, so every client and Worker that shares the backend shares its Streams.
        self._memory: _ExtensionCalls | None = None
        if memory:
            self._memory = self._runtime_backend(
                runtime or temporalio.runtime.Runtime.default(), True
            )
        super().__init__(name, backend_name=backend_name, default=default)
        # The read Activity's name includes the backend name, so this runs after the base __init__.
        self._read_activity = _read_activity(self)

    async def _read_result(
        self,
        run_id: str,  # pyright: ignore[reportUnusedParameter]
        request: stream_pb.ReadRecordsRequest,  # pyright: ignore[reportUnusedParameter]
        page: stream_pb.ReadRecordsResponse,
        owner_ended: bool,
        converter: temporalio.converter.DataConverter,
    ) -> Any:
        # The result of a Workflow read's Local Activity. Workflow code can't run the codec, so the
        # bodies are decoded here, and the codec runs on the whole page as the Activity's result.
        await _decode_page(converter, page)
        payload = page_payload(page)
        if owner_ended:
            payload.metadata[_OWNER_ENDED_KEY] = b"1"
        return temporalio.common.RawValue(payload)

    async def _owner_ended(self, ref: stream_pb.StreamRef) -> bool:
        # A reader at the end of a Stream whose owner ended without a close would wait until
        # the Stream expires. So an empty page tells the reader that the owner ended.
        return await _owner_chain_ended(ref)

    def _activities(self) -> list[Callable[..., Any]]:
        # Workflow reads run these Local Activities, so the Worker must register them.
        return [self._read_activity]

    def _runtime_backend(
        self, runtime: temporalio.runtime.Runtime, memory: bool
    ) -> _ExtensionCalls:
        backend = _ExtensionCalls(
            temporalio.bridge.streams.StreamRuntime(
                runtime._core_runtime, memory, self._config
            )
        )
        self._backends.add(backend)
        return backend

    def _new_backend(
        self, service: temporalio.service.ServiceClient
    ) -> _ExtensionCalls:
        # The calls of one client, on the runtime of its connection.
        if self._memory is not None:
            return self._memory
        return self._runtime_backend(_runtime_of(service), False)

    def _configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        config = super()._configure_client(config)
        backend = self._new_backend(config["service_client"])
        config["interceptors"] = [
            *config.get("interceptors", []),
            _StreamInterceptor(self, backend, config["data_converter"]),
        ]
        return config

    def _backend_for(self, client: temporalio.client.Client) -> BackendCalls:
        # The Stream runtime that this backend gave the client.
        interceptor = self._interceptor_of(
            client.config(active_config=True).get("interceptors", [])
        )
        if interceptor is None or interceptor.backend is None:
            raise StreamConfigError(
                f"The client has no {type(self).__name__}. Register it with "
                "stream_backends=."
            )
        return interceptor.backend

    def _interceptor_of(self, interceptors: Sequence[Any]) -> _StreamInterceptor | None:
        for interceptor in interceptors:
            if (
                isinstance(interceptor, _StreamInterceptor)
                and interceptor.plugin is self
            ):
                return interceptor
        return None

    def _configure_worker(
        self, config: temporalio.worker.WorkerConfig
    ) -> temporalio.worker.WorkerConfig:
        # Add the backend's interceptor if the client has none, and on Redis add the close Activity.
        config = super()._configure_worker(config)
        if config.get("workflows") and self._kind == "redis":
            # The close is a regular Activity that runs after the Workflow Task commits, so this
            # Worker must poll Activities on its task queue.
            activities = list(config.get("activities", []))
            if _writes.close_stream not in activities:
                activities.append(_writes.close_stream)
            config["activities"] = activities
        client = config["client"]  # type: ignore[reportTypedDictNotRequiredAccess]
        interceptors = [
            *client.config(active_config=True).get("interceptors", []),
            *config.get("interceptors", []),
        ]
        mine = self._interceptor_of(interceptors)
        if mine is None:
            mine = _StreamInterceptor(
                self, self._new_backend(client.service_client), None
            )
            config["interceptors"] = [*config.get("interceptors", []), mine]
        if (
            config.get("workflows")
            and self._kind == "redis"
            and config.get("no_remote_activities")
        ):
            # Another Worker on the task queue can run the close Activity, so it isn't an error.
            logger.warning(
                "The Worker on the task queue %s polls no Activities, so the close of a "
                "Workflow's Stream waits for another Worker on that queue with the backend",
                config.get("task_queue"),
            )
        # Records get no serialization context, so Workflows, Activities and clients encode
        # them alike.
        mine.converter = client.data_converter
        runner = config.get("workflow_runner")
        if config.get("workflows") and isinstance(runner, SandboxedWorkflowRunner):
            # The interceptor and temporalio.workflow must share the run's Stream state, even
            # when the restrictions don't pass temporalio through.
            config["workflow_runner"] = dataclasses.replace(
                runner,
                restrictions=runner.restrictions.with_passthrough_modules(
                    "temporalio.common._streams"
                ),
            )
        return config

    def _configure_replayer(
        self, config: temporalio.worker.ReplayerConfig
    ) -> temporalio.worker.ReplayerConfig:
        # A replay runs no Activity, so it needs the Workflow interceptor and no runtime.
        config = super()._configure_replayer(config)
        config["interceptors"] = [
            *config.get("interceptors", []),
            _StreamInterceptor(self, None, None),
        ]
        return config

    async def _close_in_workflow(self, request: stream_pb.CloseStreamRequest) -> int:
        # An Activity runs only after the Workflow Task commits, so a failed task leaves the
        # Stream open for its retry. The Activity closes the Stream as the run chain that owns it.
        return await temporalio.workflow.execute_activity(
            streams_workflow.CLOSE_ACTIVITY,
            request,
            result_type=int,
            start_to_close_timeout=_CLOSE_TIMEOUT,
            retry_policy=_CLOSE_RETRY,
        )

    async def shutdown(self) -> None:
        """Close every open writer, including the client's.

        Call it when the process stops writing to Streams.
        """
        failed = 0
        for backend in list(self._backends):
            failed += await backend.close_writers()
        if failed:
            logger.warning("Stream shutdown left %d failed writers", failed)


def read_activity_name(backend: str) -> str:
    """The name of the Local Activity that reads a page of a backend for Workflow code."""
    return f"__temporal_stream_read:{backend}"


def _read_activity(backend: ExtensionStreamBackend) -> Callable[..., Any]:
    @temporalio.activity.defn(name=read_activity_name(backend.backend_name))
    async def read_page(request: stream_pb.ReadRecordsRequest) -> Any:
        info = temporalio.activity.info()
        request.namespace = info.namespace
        run_id = info.workflow_run_id or ""
        # The Worker's client gave the attempt its Stream runtime.
        streams = _attempt_streams(backend.backend_name)
        try:
            # The read sends the reading run, so a native server keeps the Records that the open
            # run can read again on replay.
            page = await cast(_ExtensionCalls, streams.backend).call_async(
                "WorkflowReadRecords",
                ext_pb.WorkflowReadRecordsRequest(
                    read=request, workflow_id=info.workflow_id or "", run_id=run_id
                ),
                stream_pb.ReadRecordsResponse,
            )
        except _errors.StreamError as err:
            raise _activity_error(err) from None
        owner_ended = (
            not page.records
            and not page.done
            and bool(request.stream.workflow_id)
            and await backend._owner_ended(request.stream)
        )
        return await backend._read_result(
            run_id, request, page, owner_ended, streams.converter
        )

    return read_page


def _activity_error(err: _errors.StreamError) -> temporalio.exceptions.ApplicationError:
    # The reason goes in the details, so Workflow code gets the same Stream error.
    return temporalio.exceptions.ApplicationError(
        str(err.message),
        err.reason,
        type=type(err).__name__,
        non_retryable=err.non_retryable,
    )


async def _owner_chain_ended(ref: stream_pb.StreamRef) -> bool:
    # The run chain ended when the Workflow id's newest run is closed, or is of another chain.
    try:
        description = (
            await temporalio.activity.client()
            .get_workflow_handle(ref.workflow_id)
            .describe()
        )
    except temporalio.service.RPCError as err:
        if err.status == temporalio.service.RPCStatusCode.NOT_FOUND:
            return True
        # The result is only a hint, so the reader keeps waiting.
        logger.debug("Describing the owner of Stream %s failed", ref.id, exc_info=True)
        return False
    first_run_id = description.raw_description.workflow_execution_info.first_run_id
    if first_run_id and first_run_id != ref.first_execution_run_id:
        return True
    return description.status != temporalio.client.WorkflowExecutionStatus.RUNNING


def page_payload(
    page: stream_pb.ReadRecordsResponse,
) -> temporalio.api.common.v1.Payload:
    """A Workflow read's page as the payload that Workflow code takes."""
    return temporalio.api.common.v1.Payload(
        metadata={"encoding": b"binary/protobuf", "messageType": _PAGE_TYPE},
        data=page.SerializeToString(),
    )


def _attempt_streams(backend: str) -> _activity.ActivityStreams:
    attempt = _activity.current.get()
    streams = attempt.get(backend) if attempt is not None else None
    if streams is None:
        raise StreamConfigError(f"No Stream backend {backend} runs this Local Activity")
    return streams


async def _decode_page(
    converter: temporalio.converter.DataConverter, page: stream_pb.ReadRecordsResponse
) -> None:
    codec = converter.payload_codec
    if codec is None:
        return
    payloads = [
        payload
        for record in page.records
        for payload in (record.body, record.close_result)
        if payload.ByteSize()
    ]
    if not payloads:
        return
    for payload, decoded in zip(payloads, await codec.decode(payloads)):
        payload.CopyFrom(decoded)


def _runtime_of(
    service: temporalio.service.ServiceClient,
) -> temporalio.runtime.Runtime:
    # Find the runtime of a client's connection the same way the Worker does.
    service = getattr(service, "worker_service_client", service)
    runtime = getattr(service, "_runtime", None) or getattr(
        getattr(service, "config", None), "runtime", None
    )
    return runtime or temporalio.runtime.Runtime.default()


class _StreamInterceptor(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    # There is one for each client of the backend, with that client's Stream runtime. The
    # client's Workers get it from the client.
    def __init__(
        self,
        plugin: ExtensionStreamBackend,
        backend: _ExtensionCalls | None,
        converter: temporalio.converter.DataConverter | None,
    ) -> None:
        super().__init__()
        self.plugin = plugin
        self.backend = backend
        self.converter = converter or temporalio.converter.DataConverter.default

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        # Workflow code reads the Worker's backends from the interceptor classes.
        return type(
            "_WorkflowInbound",
            (_WorkflowInbound,),
            {
                "_backend": self.plugin,
                "_backend_name": self.plugin.backend_name,
                "_default": self.plugin.default,
                "_kind": self.plugin._kind,
                "_outbound_class": self.plugin._outbound_class,
            },
        )

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInbound(next, self)


class _WorkflowInbound(temporalio.worker.WorkflowInboundInterceptor):
    _backend: ExtensionStreamBackend | None = None
    _backend_name = ""
    _default = False
    _kind = ""
    # Each backend's interceptor sets it on its own class.
    _outbound_class: type[_WorkflowOutbound]  # pyright: ignore[reportUninitializedInstanceVariable]

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        assert self._backend is not None
        super().init(
            self._outbound_class(
                outbound, self._backend, self._backend_name, self._default, self._kind
            )
        )

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        _take_owned_streams(input.headers)
        _receive(input.args)
        try:
            result = await super().execute_workflow(input)
        except BaseException as err:
            if _ends_run(err):
                await _end_run(err)
            raise
        await _end_run(None)
        return result

    async def handle_signal(self, input: temporalio.worker.HandleSignalInput) -> None:
        _receive(input.args)
        await super().handle_signal(input)

    async def handle_update_handler(
        self, input: temporalio.worker.HandleUpdateInput
    ) -> Any:
        _receive(input.args)
        return await super().handle_update_handler(input)


def _receive(args: Sequence[Any]) -> None:
    # A run that receives a Stream uses Streams. The inputs, Signals and Updates are in
    # History, so a replay sets the same flag.
    if _stream_arguments(args):
        streams_workflow._state().uses_streams = True


async def _end_run(err: BaseException | None) -> None:
    # Cancel the run's reads, so the server drops their parked reads. Then run the deferred closes
    # by the run's outcome, and send the unawaited writes before the run ends.
    await _workflow_reader.cancel_wakes()
    streams_workflow.close_at_run_end(err)
    await streams_workflow.drain()


def _ends_run(err: BaseException) -> bool:
    # True if err ends the run. Any other exception fails the Workflow Task, and an eviction
    # closes the coroutine outside the Workflow's loop.
    if isinstance(err, temporalio.workflow.ContinueAsNewError):
        return True
    if isinstance(err, asyncio.CancelledError):
        return temporalio.workflow.cancellation_reason() is not None
    return isinstance(
        err, Exception
    ) and temporalio.workflow._Runtime.current().workflow_is_failure_exception(err)


def _owned_ref(stream_id: str) -> models.StreamRef:
    # The run chain owns each Stream that it creates.
    info = temporalio.workflow.info()
    return models.StreamRef(
        id=stream_id,
        workflow_id=info.workflow_id,
        first_execution_run_id=info.first_execution_run_id,
    )


def _take_owned_streams(
    headers: Mapping[str, temporalio.api.common.v1.Payload],
) -> None:
    payload = headers.get(_OWNED_STREAMS_HEADER)
    if payload is None:
        return
    state = streams_workflow._state()
    state.uses_streams = True
    converter = temporalio.converter.PayloadConverter.default
    for stream_id, backend, *_ in converter.from_payload(payload, list[Any]):
        state.stream(_owned_ref(stream_id)).backend = backend


class _WorkflowOutbound(temporalio.worker.WorkflowOutboundInterceptor):
    _temporal_stream_transport = True
    # The runtime can't park a read, so a reader waits on a timer after an empty page.
    _temporal_stream_parks_reads = False
    _temporal_stream_read_wait = _READ_WAIT
    # The Local Activity's timeout bounds its wait for the create too.
    _temporal_stream_create_wait = _READ_WAIT
    _temporal_stream_page_bytes = _PAGE_BYTES

    def __init__(
        self,
        next: temporalio.worker.WorkflowOutboundInterceptor,
        backend: ExtensionStreamBackend,
        backend_name: str,
        default: bool,
        kind: str,
    ) -> None:
        super().__init__(next)
        self._temporal_stream_backend = backend
        self._temporal_stream_backend_name = backend_name
        self._temporal_stream_default = default
        self._temporal_stream_kind = kind
        # Counts the run's Activity calls, so a key is the same on replay and on a retry. It
        # doesn't call workflow.uuid4(), so the user's random values don't change.
        self._activity_calls = 0

    def _activity_headers(self) -> dict[str, temporalio.api.common.v1.Payload]:
        self._activity_calls += 1
        # Both ends use the default converter, so a custom user converter can't break the
        # header.
        converter = temporalio.converter.PayloadConverter.default
        return {_KEY_HEADER: converter.to_payload(str(self._activity_calls))}

    async def _temporal_stream_read_page(
        self, request: stream_pb.ReadRecordsRequest
    ) -> _workflow_reader.Page:
        # The page is the Local Activity's result, so it is in History.
        if self._temporal_stream_page_bytes:
            request.max_bytes = self._temporal_stream_page_bytes
        try:
            result = await temporalio.workflow.execute_local_activity(
                read_activity_name(self._temporal_stream_backend_name),
                request,
                result_type=temporalio.common.RawValue,
                start_to_close_timeout=_READ_WAIT + _READ_MARGIN,
            )
        except (
            temporalio.exceptions.ActivityError,
            temporalio.exceptions.ApplicationError,
        ) as err:
            # A Local Activity's failure can come as its application error, with no
            # ActivityError around it.
            raise _workflow_reader.error_of(err) from None
        return _workflow_reader.Page(
            response=stream_pb.ReadRecordsResponse.FromString(result.payload.data),
            owner_ended=_OWNER_ENDED_KEY in result.payload.metadata,
        )

    def continue_as_new(self, input: temporalio.worker.ContinueAsNewInput) -> NoReturn:
        # The next run needs each owned Stream's backend for stream_handle.
        owned = [
            [stream_id, run_stream.backend]
            for stream_id, run_stream in streams_workflow._state().streams.items()
            if run_stream.backend is not None and not run_stream.closed
        ]
        # Each backend's interceptor sees the call, and the first one writes the header.
        if owned and _OWNED_STREAMS_HEADER not in input.headers:
            converter = temporalio.converter.PayloadConverter.default
            input.headers = {
                **input.headers,
                _OWNED_STREAMS_HEADER: converter.to_payload(owned),
            }
        super().continue_as_new(input)

    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        # The key is in History, so add it only for a run that uses Streams or passes one to
        # this Activity. Count every call, so the keys are the same on replay.
        headers = self._activity_headers()
        state = streams_workflow._state()
        if not state.uses_streams and _stream_arguments(input.args):
            state.uses_streams = True
        if state.uses_streams:
            input.headers = {**input.headers, **headers}
        return super().start_activity(input)

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        # A Local Activity's headers aren't in History, so every Local Activity gets the key.
        input.headers = {**input.headers, **self._activity_headers()}
        return super().start_local_activity(input)


class _ActivityInbound(temporalio.worker.ActivityInboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.ActivityInboundInterceptor,
        interceptor: _StreamInterceptor,
    ) -> None:
        super().__init__(next)
        self._interceptor = interceptor
        self._temporal_stream_backend_name = interceptor.plugin.backend_name
        self._temporal_stream_default = interceptor.plugin.default

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        streams = _activity_streams(input, self._interceptor)
        attempt, token = enter_attempt(self, streams)
        try:
            if temporalio.activity.info().attempt > 1:
                # Withdraw the Records of earlier attempts, even if this attempt publishes
                # nothing. This creates no Stream.
                handles = {
                    handle.id: handle
                    for handle in _stream_arguments(input.args)
                    if attempt.get(handle.backend) is streams
                }
                for handle in handles.values():
                    await streams.writer(handle).withdraw_earlier_attempts()
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


def enter_attempt(
    inbound: temporalio.worker.ActivityInboundInterceptor,
    streams: _activity.ActivityStreams,
) -> tuple[_activity.AttemptStreams, Any]:
    """Add a backend's state to the attempt, and return the attempt and the token to reset.

    The outermost backend's interceptor creates the attempt and reads the Worker's backends
    from the chain. Each inner interceptor adds its state.
    """
    attempt = _activity.current.get()
    if attempt is not None:
        attempt.backends[streams.backend_name] = streams
        return attempt, None
    backends: list[tuple[str, bool]] = []
    link: Any = inbound
    while link is not None:
        if hasattr(link, "_temporal_stream_backend_name"):
            backends.append(
                (link._temporal_stream_backend_name, link._temporal_stream_default)
            )
        link = getattr(link, "next", None)
    # The default is the backend with default=True, or else native.
    default = next((name for name, marked in backends if marked), _ids.NATIVE_BACKEND)
    attempt = _activity.AttemptStreams(default=default)
    attempt.backends[streams.backend_name] = streams
    return attempt, _activity.current.set(attempt)


def _stream_arguments(args: Sequence[Any]) -> list[StreamHandle[Any]]:
    # Find the Stream handles in call arguments. A handle can be in a field, a slot, a list or
    # a dict.
    handles: list[StreamHandle[Any]] = []
    seen: set[int] = set()

    def walk(value: Any, depth: int) -> None:
        if depth > 4 or id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, StreamHandle):
            handles.append(value)
        elif isinstance(value, (str, bytes, int, float, bool)) or value is None:
            return
        elif isinstance(value, dict):
            for item in value.values():
                walk(item, depth + 1)
        elif isinstance(value, (list, tuple, set, frozenset)):
            for item in value:
                walk(item, depth + 1)
        else:
            for item in _fields_of(value):
                walk(item, depth + 1)

    for arg in args:
        walk(arg, 0)
    return handles


def _fields_of(value: Any) -> list[Any]:
    fields = list(getattr(value, "__dict__", {}).values())
    for klass in type(value).__mro__:
        slots = getattr(klass, "__slots__", ())
        for slot in (slots,) if isinstance(slots, str) else slots:
            if hasattr(value, slot):
                fields.append(getattr(value, slot))
    return fields


async def _close_quietly(streams: _activity.ActivityStreams) -> None:
    try:
        await asyncio.shield(streams.close())
    except Exception:
        temporalio.activity.logger.warning(
            "Failed flushing Stream Records of a failed Activity", exc_info=True
        )


def _activity_streams(
    input: temporalio.worker.ExecuteActivityInput, interceptor: _StreamInterceptor
) -> _activity.ActivityStreams:
    # A sync Activity has no client, so the interceptor gives the backend and the converter.
    assert interceptor.backend is not None
    info = temporalio.activity.info()
    converter = temporalio.converter.PayloadConverter.default
    key = input.headers.get(_KEY_HEADER)
    key_text = converter.from_payload(key, str) if key is not None else ""
    epoch = stream_pb.ProducerEpoch(attempt=info.attempt)
    previous: stream_pb.WorkflowTaskEpoch | None = None
    failed: tuple[int, ...] = ()
    if info.is_local:
        task = info.workflow_task
        if task is not None:
            # Always set it, with 0 if no task completed. If it's unset, the Store doesn't
            # withdraw the run's orphaned Local Activity Records.
            previous = stream_pb.WorkflowTaskEpoch(
                started_event_id=task.previous_started_event_id
            )
            failed = tuple(task.failed_tasks)
            epoch.task_started_event_id = task.started_event_id
            epoch.task_attempt = task.attempt
            if task.started_time is not None:
                epoch.task_started_time.FromDatetime(task.started_time)
        run_id = info.workflow_run_id or ""
        producer = stream_pb.Producer(
            kind=stream_pb.ProducerKind.PRODUCER_KIND_LOCAL_ACTIVITY,
            id=_ids.local_activity_producer_id(run_id, key_text),
            run_id=run_id,
        )
    else:
        epoch.attempt_scheduled_time.FromDatetime(info.current_attempt_scheduled_time)
        if info.in_workflow:
            run_id = info.workflow_run_id or ""
            producer = stream_pb.Producer(
                kind=stream_pb.ProducerKind.PRODUCER_KIND_ACTIVITY,
                id=_ids.activity_producer_id(run_id, info.activity_id, key_text),
                run_id=run_id,
            )
        else:
            producer = stream_pb.Producer(
                kind=stream_pb.ProducerKind.PRODUCER_KIND_STANDALONE_ACTIVITY,
                id=_ids.standalone_activity_producer_id(
                    info.activity_id, info.activity_run_id or ""
                ),
            )
    producer.epoch.CopyFrom(epoch)
    plugin = interceptor.plugin
    return _activity.ActivityStreams(
        backend=interceptor.backend,
        converter=interceptor.converter,
        namespace=info.namespace,
        producer=producer,
        loop=asyncio.get_running_loop(),
        backend_name=plugin.backend_name,
        workflow_id=info.workflow_id,
        previous_completed_task=previous,
        failed_tasks=failed,
        # A Local Activity writes for its Workflow run, so the run's checks cover it.
        task_token=b"" if info.is_local else info.task_token,
        activity_streams=plugin._activity_streams,
        max_batch_bytes=plugin._max_batch_bytes,
        nests_workflow_streams=plugin._nests_workflow_streams,
    )


ExtensionStreamBackend._outbound_class = _WorkflowOutbound
