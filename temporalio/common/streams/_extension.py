"""The Stream backends whose calls go to the temporalio-streams runtime in the bridge."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import weakref
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, NoReturn

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.bridge.streams
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.runtime
import temporalio.service
import temporalio.worker
import temporalio.workflow
from temporalio.common.streams import _activity, _ids, _writes
from temporalio.common.streams import _workflow as streams_workflow
from temporalio.common.streams._backend import BackendCalls, _ExtensionCalls
from temporalio.common.streams._errors import StreamConfigError
from temporalio.common.streams._handle import StreamHandle
from temporalio.common.streams._plugin import StreamBackend
from temporalio.common.streams.proto import v1 as ext_pb
from temporalio.common.streams.system import models
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)

# The backend puts this key into the headers of each Activity and Local Activity that a Workflow
# schedules. It keeps the Producer id the same across retries.
_KEY_HEADER = "__temporal_stream_key"
# Continue-as-new passes the run chain's open Streams to the next run in this header.
_OWNED_STREAMS_HEADER = "__temporal_owned_streams"
_CLOSE_TIMEOUT = timedelta(seconds=30)
# A Store error is retryable and every other Stream error is not, as each one says itself.
_CLOSE_RETRY = temporalio.common.RetryPolicy(
    initial_interval=timedelta(seconds=1), maximum_interval=timedelta(seconds=30)
)


class ExtensionStreamBackend(StreamBackend):
    """Base of the Redis and memory backends, whose calls go to the temporalio-streams runtime.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        name: str,
        memory: bool,
        config: ext_pb.RedisStreamConfig,
        runtime: temporalio.runtime.Runtime | None,
        backend_name: str,
        default: bool,
    ) -> None:
        """Create the backend over the bridge's Stream runtime, on Redis or in memory.

        ``backend_name`` is the name that Stream handles carry for this backend. ``default``
        makes it the backend of a create with no backend name.
        """
        config.backend = backend_name
        self._config = config.SerializeToString()
        self._backends: weakref.WeakSet[_ExtensionCalls] = weakref.WeakSet()
        # Each client gets its own runtime object on Redis. The memory Store has one for the
        # backend, so every client and Worker that shares the backend shares its Streams.
        self._memory: _ExtensionCalls | None = None
        if memory:
            self._memory = self._new_backend(
                runtime or temporalio.runtime.Runtime.default(), True
            )
        super().__init__(
            name,
            backend_name=backend_name,
            default=default,
            activities=_writes.ACTIVITIES,
        )

    def _new_backend(
        self, runtime: temporalio.runtime.Runtime, memory: bool = False
    ) -> _ExtensionCalls:
        if self._memory is not None and not memory:
            return self._memory
        backend = _ExtensionCalls(
            temporalio.bridge.streams.StreamRuntime(
                runtime._core_runtime, memory, self._config
            )
        )
        self._backends.add(backend)
        return backend

    def _configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        # Each client gets its own Stream runtime.
        config = super()._configure_client(config)
        backend = self._new_backend(_runtime_of(config["service_client"]))
        config["interceptors"] = [
            *config.get("interceptors", []),
            _StreamInterceptor(self, backend, config["data_converter"]),
        ]
        return config

    def _backend_for(self, client: temporalio.client.Client) -> BackendCalls:
        # The Stream runtime that this backend gave the client.
        interceptor = self._interceptor_of(
            client.config(active_config=True)["interceptors"]
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
        # Add the backend's Activities. The Worker takes the interceptor from its client.
        config = super()._configure_worker(config)
        client = config["client"]  # type: ignore[reportTypedDictNotRequiredAccess]
        interceptors = [
            *client.config(active_config=True)["interceptors"],
            *config.get("interceptors", []),
        ]
        if any(
            isinstance(other, _StreamInterceptor) and other.plugin is not self
            for other in interceptors
        ):
            raise StreamConfigError(
                "The Worker's client has another Stream backend. For now a Worker serves "
                "one backend."
            )
        mine = self._interceptor_of(interceptors)
        if mine is None:
            raise StreamConfigError(
                f"The Worker's client has no {type(self).__name__}. Register it with "
                "stream_backends=."
            )
        # Records get no serialization context, so Workflows, Activities and clients encode
        # them alike.
        mine.converter = client.data_converter
        runner = config.get("workflow_runner")
        if isinstance(runner, SandboxedWorkflowRunner):
            # The interceptor and the facade must share the run's Stream state, also when the
            # restrictions don't pass temporalio through.
            config["workflow_runner"] = dataclasses.replace(
                runner,
                restrictions=runner.restrictions.with_passthrough_modules(
                    "temporalio.common.streams"
                ),
            )
        return config

    def _configure_replayer(
        self, config: temporalio.worker.ReplayerConfig
    ) -> temporalio.worker.ReplayerConfig:
        # A replay runs no Activity, so it needs the Workflow interceptor and no runtime.
        config = super()._configure_replayer(config)
        if any(
            isinstance(other, _StreamInterceptor) and other.plugin is not self
            for other in config.get("interceptors", [])
        ):
            raise StreamConfigError(
                "The Replayer has another Stream backend. For now a Replayer serves one "
                "backend, as a Worker does."
            )
        config["interceptors"] = [
            *config.get("interceptors", []),
            _StreamInterceptor(self, None, None),
        ]
        return config

    async def _close_in_workflow(self, request: stream_pb.CloseStreamRequest) -> int:
        # Workflow code closes a Workflow's Stream here. The close must run only once the
        # Workflow Task commits, so a task that fails before then leaves the Stream open for its
        # retry. A regular Activity on the Workflow's task queue does that, and it closes as the
        # run chain, the Stream's owner. A backend that closes another way overrides this.
        return await temporalio.workflow.execute_activity(
            streams_workflow.CLOSE_ACTIVITY,
            request,
            result_type=int,
            start_to_close_timeout=_CLOSE_TIMEOUT,
            retry_policy=_CLOSE_RETRY,
        )

    async def shutdown(self) -> None:
        """Drop every open writer, including the client's. Call it once the process no longer
        writes Streams.
        """
        failed = 0
        for backend in list(self._backends):
            failed += await backend.close_writers()
        if failed:
            logger.warning("Stream shutdown left %d failed writers", failed)


def _runtime_of(
    service: temporalio.service.ServiceClient,
) -> temporalio.runtime.Runtime:
    # The runtime of a client's connection, as the Worker finds it.
    service = getattr(service, "worker_service_client", service)
    runtime = getattr(service, "_runtime", None) or getattr(
        getattr(service, "config", None), "runtime", None
    )
    return runtime or temporalio.runtime.Runtime.default()


class _StreamInterceptor(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    # One for each client of the backend, with that client's Stream runtime. The client's
    # Workers take it from the client.
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
        # Workflow code learns the Worker's backend from the interceptor class.
        return type(
            "_WorkflowInbound",
            (_WorkflowInbound,),
            {
                "_backend": self.plugin,
                "_backend_name": self.plugin.backend_name,
                "_default": self.plugin.default,
                "_kind": self.plugin._kind,
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

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        assert self._backend is not None
        super().init(
            _WorkflowOutbound(
                outbound, self._backend, self._backend_name, self._default, self._kind
            )
        )

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        _take_owned_streams(input.headers)
        try:
            result = await super().execute_workflow(input)
        except BaseException as err:
            if _ends_run(err):
                await streams_workflow.drain()
            raise
        await streams_workflow.drain()
        return result


def _ends_run(err: BaseException) -> bool:
    # The run's writes that nobody awaits go out before its end. Any other exception fails the
    # Workflow Task, and an eviction closes the coroutine outside the Workflow's loop.
    if isinstance(err, temporalio.workflow.ContinueAsNewError):
        return True
    if isinstance(err, asyncio.CancelledError):
        return temporalio.workflow.cancellation_reason() is not None
    return isinstance(
        err, Exception
    ) and temporalio.workflow._Runtime.current().workflow_is_failure_exception(err)


def _owned_ref(stream_id: str) -> models.StreamRef:
    # A Stream that the run chain creates is the chain's own.
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
    converter = temporalio.converter.PayloadConverter.default
    for stream_id, backend, *_ in converter.from_payload(payload, list[Any]):
        state.stream(_owned_ref(stream_id)).backend = backend


class _WorkflowOutbound(temporalio.worker.WorkflowOutboundInterceptor):
    _temporal_stream_transport = True

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
        # Numbers each Activity call of the run in order, so the key is the same on replay and
        # a retry keeps it. A counter, not workflow.uuid4(), leaves the user's random values
        # alone.
        self._activity_calls = 0

    def _activity_headers(self) -> dict[str, temporalio.api.common.v1.Payload]:
        self._activity_calls += 1
        # Both ends use the default converter, so a custom user converter can't break the
        # header.
        converter = temporalio.converter.PayloadConverter.default
        return {_KEY_HEADER: converter.to_payload(str(self._activity_calls))}

    def continue_as_new(self, input: temporalio.worker.ContinueAsNewInput) -> NoReturn:
        # The next run needs each owned Stream's backend for stream_handle.
        owned = [
            [stream_id, run_stream.backend]
            for stream_id, run_stream in streams_workflow._state().streams.items()
            if run_stream.backend is not None and not run_stream.closed
        ]
        if owned:
            converter = temporalio.converter.PayloadConverter.default
            input.headers = {
                **input.headers,
                _OWNED_STREAMS_HEADER: converter.to_payload(owned),
            }
        super().continue_as_new(input)

    def start_activity(
        self, input: temporalio.worker.StartActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        input.headers = {**input.headers, **self._activity_headers()}
        return super().start_activity(input)

    def start_local_activity(
        self, input: temporalio.worker.StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
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

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        streams = _activity_streams(input, self._interceptor)
        token = _activity.current.set(streams)
        try:
            if temporalio.activity.info().attempt > 1:
                # The open of a new attempt withdraws the Records of the earlier ones, also
                # when this attempt publishes nothing. It creates no Stream.
                stream_ids = [handle.id for handle in _stream_arguments(input.args)]
                for stream_id in dict.fromkeys(stream_ids):
                    await streams.writer(stream_id).withdraw_earlier_attempts()
            result = await super().execute_activity(input)
        except BaseException:
            await _close_quietly(streams)
            raise
        else:
            await streams.close()
            return result
        finally:
            _activity.current.reset(token)


def _stream_arguments(args: Sequence[Any]) -> list[StreamHandle[Any]]:
    # The Streams that an Activity received, which an earlier attempt may have written. They
    # may sit in fields of its input object, in slots, and in lists and dicts.
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
            # Always set, with 0 for none completed. An unset one turns off the withdrawal of
            # the run's orphaned Local Activity Records.
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
    return _activity.ActivityStreams(
        backend=interceptor.backend,
        converter=interceptor.converter,
        namespace=info.namespace,
        producer=producer,
        loop=asyncio.get_running_loop(),
        backend_name=interceptor.plugin.backend_name,
        default=interceptor.plugin.default,
        workflow_id=info.workflow_id,
        previous_completed_task=previous,
        failed_tasks=failed,
    )
