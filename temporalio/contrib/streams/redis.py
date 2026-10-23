"""The Redis Stream plugin.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import weakref
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, NoReturn, cast

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.failure.v1
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
from temporalio.contrib.streams import _activity, _ids, _writes
from temporalio.contrib.streams import workflow as streams_workflow
from temporalio.contrib.streams._backend import StreamBackend, _ExtensionStreamBackend
from temporalio.contrib.streams._errors import StreamConfigError
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._items import close_status
from temporalio.contrib.streams._plugin import StreamPlugin
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.system import models
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)

# The plugin puts this key into the headers of each Activity and Local Activity that a Workflow
# schedules. It keeps the Producer id the same across retries.
_KEY_HEADER = "__temporal_stream_key"
# Continue-as-new passes the run chain's open Streams to the next run in this header.
_OWNED_STREAMS_HEADER = "__temporal_owned_streams"


class _ExtensionStreamPlugin(StreamPlugin):
    # The Redis, memory and native plugins: the Stream calls go to the temporalio-streams
    # runtime.
    # Whether the Store closes a Stream with its owner's run chain on the server.
    _server_closes_with_run = False

    def __init__(
        self,
        name: str,
        memory: bool,
        config: ext_pb.RedisStreamConfig | ext_pb.NativeStreamConfig,
        runtime: temporalio.runtime.Runtime | None,
        backend_name: str,
        default: bool,
    ) -> None:
        """Create the plugin over the bridge's Stream runtime, on Redis or in memory.

        ``backend_name`` is the name that Stream handles carry for this backend. ``default``
        makes it the client's default backend when the client has more than one.
        """
        config.backend = backend_name
        self._config = config.SerializeToString()
        self._backends: weakref.WeakSet[_ExtensionStreamBackend] = weakref.WeakSet()
        # The closes of the runs that ended on this plugin's Workers, by run id, until the
        # Worker evicts the run.
        self._run_closes: dict[str, _RunClose] = {}
        self._closing: set[asyncio.Task[None]] = set()
        # Each client gets its own runtime object on Redis. The memory Store has one for the
        # plugin, so every client and Worker that shares the plugin shares its Streams.
        self._memory: _ExtensionStreamBackend | None = None
        if memory:
            self._memory = self._runtime_backend(
                runtime or temporalio.runtime.Runtime.default(), True
            )
        super().__init__(name, backend_name=backend_name, default=default)

    def _runtime_backend(
        self, runtime: temporalio.runtime.Runtime, memory: bool
    ) -> _ExtensionStreamBackend:
        backend = _ExtensionStreamBackend(
            temporalio.bridge.streams.StreamRuntime(
                runtime._core_runtime, memory, self._config
            )
        )
        self._backends.add(backend)
        return backend

    def _new_backend(
        self, service: temporalio.service.ServiceClient
    ) -> _ExtensionStreamBackend:
        # The backend of one client, on the runtime of its connection.
        if self._memory is not None:
            return self._memory
        return self._runtime_backend(_runtime_of(service), False)

    def configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        """Check the default backend, and give the client its own Stream runtime."""
        config = super().configure_client(config)
        backend = self._new_backend(config["service_client"])
        config["interceptors"] = [
            *config.get("interceptors", []),
            _StreamInterceptor(self, backend, config["data_converter"]),
        ]
        return config

    def backend_for(self, client: temporalio.client.Client) -> StreamBackend:
        """The Stream runtime that this plugin gave ``client``."""
        interceptor = self._interceptor_of(
            client.config(active_config=True)["interceptors"]
        )
        if interceptor is None or interceptor.backend is None:
            raise StreamConfigError(
                f"The client has no {type(self).__name__}. Put the plugin on the client."
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

    def configure_worker(
        self, config: temporalio.worker.WorkerConfig
    ) -> temporalio.worker.WorkerConfig:
        """Add the plugin's Activities, and its interceptor when the client has none."""
        config = super().configure_worker(config)
        check_worker_backends(config)
        # The write Local Activities route by the backend in each request, so every Stream
        # plugin of the Worker shares one set.
        activities: list[Callable[..., Any]] = list(config.get("activities", []))
        writes = cast(list[Callable[..., Any]], _writes.ACTIVITIES)
        activities.extend(write for write in writes if write not in activities)
        config["activities"] = activities
        client = config["client"]  # type: ignore[reportTypedDictNotRequiredAccess]
        interceptors = [
            *client.config(active_config=True)["interceptors"],
            *config.get("interceptors", []),
        ]
        # Records get no serialization context, so Workflows, Activities and clients encode
        # them alike.
        mine = self._interceptor_of(interceptors)
        if mine is None:
            mine = _StreamInterceptor(
                self, self._new_backend(client.service_client), None
            )
            config["interceptors"] = [*config.get("interceptors", []), mine]
        mine.converter = client.data_converter
        runner = config.get("workflow_runner")
        if isinstance(runner, SandboxedWorkflowRunner):
            # The interceptor and the facade must share the run's Stream state, also when the
            # restrictions don't pass temporalio through.
            config["workflow_runner"] = dataclasses.replace(
                runner,
                restrictions=runner.restrictions.with_passthrough_modules(
                    "temporalio.contrib.streams"
                ),
            )
        return config

    def configure_replayer(
        self, config: temporalio.worker.ReplayerConfig
    ) -> temporalio.worker.ReplayerConfig:
        """Add the Workflow interceptor. A replay runs no Activity, so it needs no runtime."""
        config = super().configure_replayer(config)
        config["interceptors"] = [
            *config.get("interceptors", []),
            _StreamInterceptor(self, None, None),
        ]
        return config

    def on_workflow_evicted(
        self, input: temporalio.worker.WorkflowEvictedInput
    ) -> None:
        """Write the closes of a run that ended, once the server accepted its completion."""
        close = self._run_closes.pop(input.run_id, None)
        if close is None or not input.execution_ending:
            return
        task = asyncio.get_running_loop().create_task(_write_closes(close))
        self._closing.add(task)
        task.add_done_callback(self._closing.discard)

    async def run_worker(
        self,
        worker: temporalio.worker.Worker,
        next: Callable[[temporalio.worker.Worker], Awaitable[None]],
    ) -> None:
        """Run the Worker, then wait for the closes of the runs that ended on it."""
        try:
            await super().run_worker(worker, next)
        finally:
            if self._closing:
                await asyncio.gather(*list(self._closing), return_exceptions=True)

    async def shutdown(self) -> None:
        """Drop every open writer, including the client's. Call it once the process no longer
        writes Streams.
        """
        failed = 0
        for backend in list(self._backends):
            failed += await backend.close_writers()
        if failed:
            logger.warning("Stream shutdown left %d failed writers", failed)


def check_worker_backends(config: temporalio.worker.WorkerConfig) -> None:
    """Raise ValueError when two Stream plugins of a Worker register one backend name."""
    client = config.get("client")
    plugins = [
        *(client.config()["plugins"] if client is not None else []),
        *config.get("plugins", []),
    ]
    names: dict[str, StreamPlugin] = {}
    for plugin in plugins:
        if not isinstance(plugin, StreamPlugin):
            continue
        name = plugin.backend_name
        known = names.setdefault(name, plugin)
        if known is not plugin:
            raise ValueError(
                f"Two Stream plugins of the Worker have the backend {name}"
            )


def _runtime_of(
    service: temporalio.service.ServiceClient,
) -> temporalio.runtime.Runtime:
    # The runtime of a client's connection, as the Worker finds it.
    service = getattr(service, "worker_service_client", service)
    runtime = getattr(service, "_runtime", None) or getattr(
        getattr(service, "config", None), "runtime", None
    )
    return runtime or temporalio.runtime.Runtime.default()


class RedisStreamPlugin(_ExtensionStreamPlugin):
    """Carries Stream calls to Redis, through the temporalio-streams runtime in the bridge.

    Put it on the client. A Worker takes its client's plugins, so one plugin serves Workflow
    code, Activities and the client. Each client of the plugin gets its own runtime object, and
    the client's Workers share it. It registers the backend name ``redis``, which the Stream
    handles it creates carry. A client and a Worker may have more than one Stream plugin, such
    as this one and ``NativeStreamPlugin``. Each call goes to the backend that the handle
    names, and ``backend=`` picks one for a create.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        url: str | Sequence[str],
        *,
        cluster: bool = False,
        key_prefix: str | None = None,
        retention: timedelta | None = None,
        safety_ttl: timedelta | None = None,
        response_timeout: timedelta | None = None,
        name: str = "redis",
        default: bool = False,
    ) -> None:
        """Create the plugin. Each client of the plugin connects to Redis on its first Stream
        call, on the client's runtime.

        Args:
            url: A Redis URL, or the seed URLs of a cluster. Credentials and TLS come from the
                URL.
            cluster: Whether the URLs are of a Redis cluster.
            key_prefix: The prefix of every key. Defaults to ``temporal-streams``.
            retention: The default Retention of a new Stream. Defaults to 7 days.
            safety_ttl: How long an open Workflow-owned Stream stays after its last write.
                Defaults to 90 days.
            response_timeout: How long one Redis call takes at most. Defaults to 5 s.
            name: The backend name that Stream handles carry, so every process routes a
                handle it receives.
            default: Make this the client's default backend. A client with more than one
                Stream plugin marks exactly one, else it raises ``StreamConfigError``.
        """
        config = ext_pb.RedisStreamConfig(
            urls=[url] if isinstance(url, str) else list(url),
            cluster=cluster,
            key_prefix=key_prefix or "",
        )
        for field, value in (
            ("retention", retention),
            ("safety_ttl", safety_ttl),
            ("response_timeout", response_timeout),
        ):
            if value is not None:
                getattr(config, field).FromTimedelta(value)
        super().__init__(
            "temporalio.RedisStreamPlugin", False, config, None, name, default
        )


class _StreamInterceptor(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    # One for each client of the plugin, with that client's Stream runtime. The client's
    # Workers take it from the client.
    def __init__(
        self,
        plugin: _ExtensionStreamPlugin,
        backend: _ExtensionStreamBackend | None,
        converter: temporalio.converter.DataConverter | None,
    ) -> None:
        super().__init__()
        self.plugin = plugin
        self.backend = backend
        self.converter = converter or temporalio.converter.DataConverter.default

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        # Workflow code learns the Worker's backend names from the interceptor classes, and
        # the run gives its closes to the interceptor.
        return type(
            "_WorkflowInbound",
            (_WorkflowInbound,),
            {
                "_backend_name": self.plugin.backend_name,
                "_default": self.plugin.default,
                "_server_closes_with_run": self.plugin._server_closes_with_run,
                "_interceptor": self if self.backend is not None else None,
            },
        )

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInbound(next, self)


@dataclass
class _RunClose:
    # The closes of a run's close-with-run Streams, which the worker writes once the server
    # accepted the run's completion.
    interceptor: _StreamInterceptor
    namespace: str
    run_id: str
    streams: list[models.StreamRef]
    result: temporalio.api.common.v1.Payload | None
    failure: temporalio.api.failure.v1.Failure | None
    status: stream_pb.CloseStatus.ValueType | None


class _WorkflowInbound(temporalio.worker.WorkflowInboundInterceptor):
    _backend_name = ""
    _default = False
    _server_closes_with_run = False
    _interceptor: _StreamInterceptor | None = None

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        super().init(_WorkflowOutbound(outbound, self._backend_name, self._default))

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        _take_owned_streams(input.headers)
        try:
            result = await super().execute_workflow(input)
            await streams_workflow.drain()
        except temporalio.workflow.ContinueAsNewError:
            # The next run continues the run chain's Streams.
            await streams_workflow.drain()
            raise
        except BaseException as err:
            if not isinstance(err, (Exception, asyncio.CancelledError)):
                # A GeneratorExit or another eviction exception closes the coroutine outside
                # the Workflow's loop. The run did not end, so there is nothing to close.
                raise
            if (
                isinstance(err, asyncio.CancelledError)
                and temporalio.workflow.cancellation_reason() is not None
            ):
                # The run ends cancelled.
                await streams_workflow.drain()
                self._close_with_run(
                    failure=temporalio.exceptions.CancelledError("Workflow cancelled")
                )
            elif _fails_run(err) and not chain_continues(
                temporalio.workflow.info(), temporalio.workflow.now(), err
            ):
                await streams_workflow.drain()
                self._close_with_run(failure=err)
            raise
        # A cron run that completes still goes on with the chain.
        if not chain_continues(
            temporalio.workflow.info(), temporalio.workflow.now(), None
        ):
            self._close_with_run(result=result)
        return result

    def _close_with_run(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        # The closes wait until the server accepts the run's completion. A last Workflow Task
        # that fails then leaves the Streams open, so its retry still writes to them.
        interceptor = self._interceptor
        if (
            interceptor is None
            or self._server_closes_with_run
            or temporalio.workflow.unsafe.is_replaying()
        ):
            # A server that closes a Stream with its owner's run chain needs no close from
            # here.
            return
        # Each Stream plugin's inbound interceptor closes the Streams of its own backend.
        default = streams_workflow._backend_name()
        streams = [
            run_stream.ref
            for run_stream in streams_workflow._state().streams.values()
            if run_stream.close_with_run
            and not run_stream.closed
            and (run_stream.backend or default) == self._backend_name
        ]
        if not streams:
            return
        converter = temporalio.workflow.payload_converter()
        failure_proto = None
        if failure is not None:
            failure_proto = temporalio.api.failure.v1.Failure()
            temporalio.converter.DataConverter.default.failure_converter.to_failure(
                failure, converter, failure_proto
            )
        info = temporalio.workflow.info()
        interceptor.plugin._run_closes[info.run_id] = _RunClose(
            interceptor=interceptor,
            namespace=info.namespace,
            run_id=info.run_id,
            streams=streams,
            result=converter.to_payload(result) if result is not None else None,
            failure=failure_proto,
            status=close_status(failure),
        )


def _fails_run(err: BaseException) -> bool:
    # Any other exception fails the Workflow Task, and the run goes on.
    return temporalio.workflow._Runtime.current().workflow_is_failure_exception(err)


def chain_continues(
    info: temporalio.workflow.Info, now: datetime, failure: BaseException | None
) -> bool:
    """Whether the server starts another run of the chain after this run ends.

    Then the next run goes on with the run chain's Streams, so the runtime skips this run's
    closes. A cron run always continues. A failed run continues when its retry policy has
    attempts and time left for the failure.
    """
    if info.cron_schedule:
        return True
    policy = info.retry_policy
    if failure is None or policy is None:
        return False
    if policy.maximum_attempts != 0 and info.attempt >= policy.maximum_attempts:
        return False
    if _non_retryable(failure, policy.non_retryable_error_types):
        return False
    if info.execution_timeout is not None:
        backoff = policy.initial_interval * (
            policy.backoff_coefficient ** (info.attempt - 1)
        )
        if policy.maximum_interval is not None:
            backoff = min(backoff, policy.maximum_interval)
        # The execution timeout covers the whole chain, so this run's start is not its start.
        expiration = info.execution_expiration_time or (
            info.workflow_start_time + info.execution_timeout
        )
        if now + backoff >= expiration:
            return False
    return True


def _non_retryable(err: BaseException, types: Sequence[str] | None) -> bool:
    if isinstance(err, temporalio.exceptions.ApplicationError):
        return err.non_retryable or (err.type or "") in (types or [])
    return type(err).__name__ in (types or [])


async def _write_closes(close: _RunClose) -> None:
    backend, converter = close.interceptor.backend, close.interceptor.converter
    assert backend is not None
    service = backend.service(close.namespace, converter)
    failure = (
        converter.failure_converter.from_failure(
            close.failure, converter.payload_converter
        )
        if close.failure is not None
        else None
    )
    for ref in close.streams:
        try:
            await service.close_stream(
                models.CloseStreamRequest(
                    stream=ref,
                    # The run itself closes its Streams, as their owner.
                    producer=models.Producer(
                        kind=stream_pb.ProducerKind.PRODUCER_KIND_WORKFLOW,
                        id=_ids.workflow_producer_id(close.run_id),
                        run_id=close.run_id,
                    ),
                    result=temporalio.common.RawValue(close.result)
                    if close.result is not None
                    else None,
                    failure=failure,
                    status=close.status,
                )
            )
        except Exception:
            logger.warning(
                "Failed closing the Stream %s with the run %s",
                ref.id,
                close.run_id,
                exc_info=True,
            )


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
    for stream_id, backend, close_with_run in converter.from_payload(
        payload, list[Any]
    ):
        run_stream = state.stream(_owned_ref(stream_id))
        run_stream.backend, run_stream.close_with_run = backend, close_with_run


class _WorkflowOutbound(temporalio.worker.WorkflowOutboundInterceptor):
    _temporal_stream_transport = True

    def __init__(
        self,
        next: temporalio.worker.WorkflowOutboundInterceptor,
        backend_name: str,
        default: bool = False,
    ) -> None:
        super().__init__(next)
        self._temporal_stream_backend_name = backend_name
        self._temporal_stream_default = default
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
            [stream_id, run_stream.backend, run_stream.close_with_run]
            for stream_id, run_stream in streams_workflow._state().streams.items()
            if run_stream.backend is not None and not run_stream.closed
        ]
        # Each Stream plugin's interceptor sees the call, and the first one writes the header.
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
        self._temporal_stream_backend_name = interceptor.plugin.backend_name
        self._temporal_stream_default = interceptor.plugin.default

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        streams = _activity_streams(input, self._interceptor)
        attempt, token = enter_attempt(self, streams)
        try:
            if temporalio.activity.info().attempt > 1:
                # The open of a new attempt withdraws the Records of the earlier ones, also
                # when this attempt publishes nothing. It creates no Stream.
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

    The outermost Stream plugin's interceptor makes the attempt, so it reads the backends of
    every Stream plugin from the chain, and each inner one adds its state.
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
    default = next((name for name, marked in backends if marked), backends[0][0])
    attempt = _activity.AttemptStreams(default=default)
    attempt.backends[streams.backend_name] = streams
    return attempt, _activity.current.set(attempt)


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
        workflow_id=info.workflow_id,
        previous_completed_task=previous,
        failed_tasks=failed,
    )
