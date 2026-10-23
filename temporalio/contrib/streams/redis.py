"""The Redis Stream plugin.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, NoReturn

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
import temporalio.converter
import temporalio.exceptions
import temporalio.runtime
import temporalio.worker
import temporalio.workflow
from temporalio.contrib.streams import _activity, _ids, _writes
from temporalio.contrib.streams import workflow as streams_workflow
from temporalio.contrib.streams._backend import _ExtensionStreamBackend
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._plugin import StreamPlugin
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.system import models
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)

EXTENSION_NAME = "temporalio-streams-redis"

# The plugin puts this key into the headers of each Activity and Local Activity that a Workflow
# schedules. It keeps the Producer id the same across retries.
_KEY_HEADER = "__temporal_stream_key"
# Continue-as-new passes the run chain's open Streams to the next run in this header.
_OWNED_STREAMS_HEADER = "__temporal_owned_streams"


class _ExtensionStreamPlugin(StreamPlugin):
    # The Redis and memory plugins: the Stream calls go to a temporalio-streams extension.

    def __init__(
        self,
        name: str,
        extension_name: str,
        config: ext_pb.RedisStreamConfig,
        runtime: temporalio.runtime.Runtime | None,
        backend_name: str,
        default: bool,
    ) -> None:
        """Create the plugin over the bridge's extension with ``extension_name``.

        ``backend_name`` is the name that Stream handles carry for this backend. ``default``
        makes it the client's default backend when the client has more than one.
        """
        config.backend = backend_name
        self._extension = temporalio.worker.WorkerExtension(
            extension_name, config.SerializeToString(), runtime=runtime
        )
        self._backend = _ExtensionStreamBackend(self._extension)
        self._converter = temporalio.converter.DataConverter.default
        super().__init__(
            name,
            backend_name=backend_name,
            default=default,
            stream_backend=self._backend,
            interceptors=[_StreamInterceptor(self)],
            activities=_writes.ACTIVITIES,
        )

    def configure_worker(
        self, config: temporalio.worker.WorkerConfig
    ) -> temporalio.worker.WorkerConfig:
        """Add the plugin's interceptors and give Core the extension."""
        config = super().configure_worker(config)
        if config.get("extension") is not None:
            raise ValueError("The Worker has another extension")
        config["extension"] = self._extension
        # Records get no serialization context, so Workflows, Activities and clients encode
        # them alike.
        client = config.get("client")
        if client is not None:
            self._converter = client.data_converter
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

    async def shutdown(self) -> None:
        """Drop every open writer, including the client's. Call it once the process no longer
        writes Streams.
        """
        failed = await self._backend.close_writers()
        if failed:
            logger.warning("Stream shutdown left %d failed writers", failed)


class RedisStreamPlugin(_ExtensionStreamPlugin):
    """Carries Stream calls to Redis, through the temporalio-streams runtime in the bridge.

    Put it on the client. A Worker takes its client's plugins, so one plugin serves Workflow
    code, Activities and the client. It registers the backend name ``redis``, which the
    Stream handles it creates carry. For now a Worker takes one Stream plugin, so it serves one
    backend. A client may have more than one, and ``backend=`` picks one.

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
        runtime: temporalio.runtime.Runtime | None = None,
        name: str = "redis",
        default: bool = False,
    ) -> None:
        """Create the plugin. It connects to Redis on the first Stream call.

        Args:
            url: A Redis URL, or the seed URLs of a cluster. Credentials and TLS come from the
                URL.
            cluster: Whether the URLs are of a Redis cluster.
            key_prefix: The prefix of every key. Defaults to ``temporal-streams``.
            retention: The default Retention of a new Stream. Defaults to 7 days.
            safety_ttl: How long an open Workflow-owned Stream stays after its last write.
                Defaults to 90 days.
            response_timeout: How long one Redis call takes at most. Defaults to 5 s.
            runtime: The runtime of the client. Defaults to the default runtime.
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
            "temporalio.RedisStreamPlugin",
            EXTENSION_NAME,
            config,
            runtime,
            name,
            default,
        )


class _StreamInterceptor(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    def __init__(self, plugin: _ExtensionStreamPlugin) -> None:
        super().__init__()
        self._plugin = plugin

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        # Workflow code learns the Worker's backend name from the interceptor class.
        return type(
            "_WorkflowInbound",
            (_WorkflowInbound,),
            {"_backend_name": self._plugin.backend_name},
        )

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInbound(next, self._plugin)


class _WorkflowInbound(temporalio.worker.WorkflowInboundInterceptor):
    _backend_name = ""

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        super().init(_WorkflowOutbound(outbound, self._backend_name))

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
                await _close_with_run(
                    failure=temporalio.exceptions.CancelledError("Workflow cancelled")
                )
            elif _fails_run(err) and not chain_continues(
                temporalio.workflow.info(), temporalio.workflow.now(), err
            ):
                await _close_with_run(failure=err)
            raise
        # A cron run that completes still goes on with the chain.
        if not chain_continues(
            temporalio.workflow.info(), temporalio.workflow.now(), None
        ):
            await _close_with_run(result=result)
        return result


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


async def _close_with_run(
    *, result: Any | None = None, failure: BaseException | None = None
) -> None:
    for stream_id, run_stream in streams_workflow._state().streams.items():
        if not run_stream.close_with_run or run_stream.closed:
            continue
        stream: streams_workflow.WorkflowStream[Any] = streams_workflow.WorkflowStream(
            id=stream_id,
            workflow_id=run_stream.ref.workflow_id,
            first_execution_run_id=run_stream.ref.first_execution_run_id,
            backend=run_stream.backend,
        )
        try:
            await stream.close(result=result, failure=failure)
        except Exception:
            if failure is None:
                raise
            # The run's own failure wins over a close that failed.
            temporalio.workflow.logger.warning(
                "Failed closing the Stream %s with the run", stream_id, exc_info=True
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
        self, next: temporalio.worker.WorkflowOutboundInterceptor, backend_name: str
    ) -> None:
        super().__init__(next)
        self._temporal_stream_backend_name = backend_name
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
        plugin: _ExtensionStreamPlugin,
    ) -> None:
        super().__init__(next)
        self._plugin = plugin

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        streams = _activity_streams(input, self._plugin)
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
    input: temporalio.worker.ExecuteActivityInput, plugin: _ExtensionStreamPlugin
) -> _activity.ActivityStreams:
    # A sync Activity has no client, so the plugin gives the backend and the converter.
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
        backend=plugin._backend,
        converter=plugin._converter,
        namespace=info.namespace,
        producer=producer,
        loop=asyncio.get_running_loop(),
        backend_name=plugin.backend_name,
        previous_completed_task=previous,
        failed_tasks=failed,
    )
