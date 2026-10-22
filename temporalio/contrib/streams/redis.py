"""The Redis Stream plugin.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import Awaitable, Callable, Generator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, NoReturn

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.runtime
import temporalio.worker
import temporalio.workflow
from temporalio.contrib.streams import _activity, _errors, _ids, _workflow
from temporalio.contrib.streams import workflow as streams_workflow
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._stream import StreamRef
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.system import models
from temporalio.plugin import SimplePlugin
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)

EXTENSION_NAME = "temporalio-streams-redis"

_SERVICE = "temporal.api.streamservice.v1.StreamService"
# How long a Worker's shutdown waits for the Stream closes that are still going out.
_DRAIN_TIMEOUT = timedelta(seconds=10)
# The plugin puts these into the headers of each Activity and Local Activity that a Workflow
# schedules. The key keeps the Producer id the same across retries.
_REF_HEADER = "__temporal_stream_ref"
_KEY_HEADER = "__temporal_stream_key"
# Continue-as-new passes the ids of the open Streams that close with the run chain to the next
# run, in this header.
_CLOSE_WITH_RUN_HEADER = "__temporal_stream_close_with_run"


class StreamPlugin(SimplePlugin):
    """Base of the plugins that carry Stream calls to a temporalio-streams extension.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        name: str,
        extension_name: str,
        config: ext_pb.RedisStreamConfig,
        runtime: temporalio.runtime.Runtime | None,
        backend_name: str,
    ) -> None:
        """Create the plugin over the bridge's extension with ``extension_name``.

        ``backend_name`` is the name that Stream handles carry for this backend.
        """
        self._backend_name = backend_name
        self._extension = temporalio.worker.WorkerExtension(
            extension_name, config.SerializeToString(), runtime=runtime
        )
        self._temporal_stream_backend = StreamBackend(self._extension)
        self._converter = temporalio.converter.DataConverter.default
        super().__init__(name, interceptors=[_StreamInterceptor(self)])

    @property
    def backend_name(self) -> str:
        """The name that Stream handles carry for this backend."""
        return self._backend_name

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

    async def run_worker(
        self,
        worker: temporalio.worker.Worker,
        next: Callable[[temporalio.worker.Worker], Awaitable[None]],
    ) -> None:
        """Run the Worker, then wait for the Stream closes that are still going out.

        Each Activity attempt already closed its writers. Client writers may still be in use,
        so the Worker leaves them. Call :py:meth:`shutdown` when the process is done.
        """
        try:
            await super().run_worker(worker, next)
        finally:
            pending = await self._temporal_stream_backend.drain_closes(_DRAIN_TIMEOUT)
            if pending:
                logger.warning(
                    "Worker shutdown left %d Stream closes in flight", pending
                )

    async def shutdown(self) -> None:
        """Flush and drop every open writer, including the client's, and wait for the closes
        in flight. Call it once the process no longer writes Streams.
        """
        backend = self._temporal_stream_backend
        failed = await backend.close_writers()
        pending = await backend.drain_closes(_DRAIN_TIMEOUT)
        if failed or pending:
            logger.warning(
                "Stream shutdown left %d failed writers and %d closes in flight",
                failed,
                pending,
            )

    async def on_workflow_activation_complete(
        self, input: temporalio.worker.WorkflowActivationCompleteInput
    ) -> None:
        """Hand the Activation's Stream calls to the runtime, encoded."""
        calls = [
            item
            for item in input.items
            if getattr(item, "_temporal_stream_call", False)
        ]
        if not calls:
            return
        info = input.info
        request = ext_pb.BufferWorkflowCallsRequest(
            run_id=input.run_id,
            withdraw_run=_withdraw_run(info),
            chain_continues=any(call.chain_continues for call in calls),
        )
        for call in _merge_appends(calls):
            request.calls.append(
                await _encode_call(call, self._converter, info.namespace)
            )
        # The call only adds to the run's buffer under a short lock, so it doesn't block the
        # event loop.
        self._temporal_stream_backend.call("BufferWorkflowCalls", request)


class RedisStreamPlugin(StreamPlugin):
    """Carries Stream calls to Redis, through the temporalio-streams runtime in the bridge.

    Put it on the client. A Worker takes its client's plugins, so one plugin serves Workflow
    code, Activities and the client.

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
        durable_writes: int = 0,
        durable_timeout: timedelta | None = None,
        response_timeout: timedelta | None = None,
        runtime: temporalio.runtime.Runtime | None = None,
        backend_name: str = "redis",
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
            durable_writes: When above zero, each Workflow flush waits for this many replicas.
            durable_timeout: How long that wait takes at most. Defaults to 1 s.
            response_timeout: How long one Redis call takes at most. Defaults to 5 s.
            runtime: The runtime of the client. Defaults to the default runtime.
            backend_name: The name that Stream handles carry for this backend.
        """
        config = ext_pb.RedisStreamConfig(
            urls=[url] if isinstance(url, str) else list(url),
            cluster=cluster,
            key_prefix=key_prefix or "",
            durable_writes=durable_writes,
        )
        for name, value in (
            ("retention", retention),
            ("safety_ttl", safety_ttl),
            ("durable_timeout", durable_timeout),
            ("response_timeout", response_timeout),
        ):
            if value is not None:
                getattr(config, name).FromTimedelta(value)
        super().__init__(
            "temporalio.RedisStreamPlugin",
            EXTENSION_NAME,
            config,
            runtime,
            backend_name,
        )


def _withdraw_run(info: temporalio.workflow.Info) -> str:
    # A retry run withdraws the Records of the run it retries.
    if info.attempt > 1 and info.continued_run_id:
        return info.continued_run_id
    return ""


@dataclass(frozen=True)
class _WorkflowCall:
    # Marks the item by attribute, which holds even if a sandbox imports a second copy of the
    # class.
    _temporal_stream_call = True

    operation: str
    request: Any
    when_run_closes: bool = False
    chain_continues: bool = False


def _merge_appends(calls: list[_WorkflowCall]) -> list[_WorkflowCall]:
    # Each append is one batch with its own sequence, so adjacent appends to one Stream go
    # as one.
    merged: list[_WorkflowCall] = []
    for call in calls:
        last = merged[-1] if merged else None
        if (
            last is not None
            and last.operation == call.operation == "AppendRecords"
            and last.request.stream == call.request.stream
        ):
            records = [*(last.request.records or []), *(call.request.records or [])]
            merged[-1] = dataclasses.replace(
                last, request=dataclasses.replace(last.request, records=records)
            )
        else:
            merged.append(call)
    return merged


async def _encode_call(
    call: _WorkflowCall,
    converter: temporalio.converter.DataConverter,
    namespace: str,
) -> ext_pb.WorkflowCall:
    request = call.request
    if call.operation == "CreateStream":
        create = stream_pb.CreateStreamRequest(
            namespace=namespace, stream_id=request.stream_id
        )
        if request.retention is not None:
            create.retention.FromTimedelta(request.retention)
        return ext_pb.WorkflowCall(create=create)
    ref = request.stream or models.StreamRef(id="")
    stream = stream_pb.StreamRef(
        id=ref.id,
        workflow_id=ref.workflow_id or "",
        first_execution_run_id=ref.first_execution_run_id or "",
    )
    if call.operation == "AppendRecords":
        records = request.records or []
        bodies = await converter.encode([record.body for record in records])
        return ext_pb.WorkflowCall(
            append=stream_pb.AppendRecordsRequest(
                namespace=namespace,
                stream=stream,
                records=[
                    stream_pb.AppendRecord(topic=record.topic, body=body)
                    for record, body in zip(records, bodies)
                ],
            )
        )
    close = stream_pb.CloseStreamRequest(namespace=namespace, stream=stream)
    if request.result is not None:
        [result] = await converter.encode([request.result])
        close.result.CopyFrom(result)
    if request.failure is not None:
        await converter.encode_failure(request.failure, close.failure)
    return ext_pb.WorkflowCall(close=close, when_run_closes=call.when_run_closes)


class _DoneHandle(temporalio.workflow.NexusOperationHandle[Any]):
    # The call is in the Activation's list, so the handle is done at once.
    def __init__(self, result: Any) -> None:
        self._result = result

    def cancel(self) -> bool:
        return False

    def __await__(self) -> Generator[Any, Any, Any]:
        return self._value().__await__()

    async def _value(self) -> Any:
        return self._result

    @property
    def operation_token(self) -> str | None:
        return None


class _StreamInterceptor(temporalio.client.Interceptor, temporalio.worker.Interceptor):
    def __init__(self, plugin: StreamPlugin) -> None:
        super().__init__()
        self._plugin = plugin
        # Workflow code learns the Worker's backend name from the interceptor class.
        self._inbound_class = type(
            "_WorkflowInbound",
            (_WorkflowInbound,),
            {"_backend_name": plugin.backend_name},
        )

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        return self._inbound_class

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
        _take_close_with_run(input.headers)
        try:
            result = await super().execute_workflow(input)
        except temporalio.workflow.ContinueAsNewError:
            # The next run continues the run chain's Streams.
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
                _close_with_run(
                    failure=temporalio.exceptions.CancelledError("Workflow cancelled")
                )
            elif _fails_run(err):
                _close_with_run(
                    failure=err,
                    chain_continues=chain_continues(
                        temporalio.workflow.info(), temporalio.workflow.now(), err
                    ),
                )
            raise
        # A cron run that completes still goes on with the chain.
        _close_with_run(
            result=result,
            chain_continues=chain_continues(
                temporalio.workflow.info(), temporalio.workflow.now(), None
            ),
        )
        return result


def _fails_run(err: BaseException) -> bool:
    # Any other exception fails the Workflow Task, and the run goes on.
    return temporalio.workflow._Runtime.current().workflow_is_failure_exception(err)


def chain_continues(
    info: temporalio.workflow.Info, now: datetime, failure: BaseException | None
) -> bool:
    """Whether the server starts another run of the chain after this run ends.

    Then the next run goes on with the default Stream, so the runtime skips this run's closes.
    A cron run always continues. A failed run continues when its retry policy has attempts and
    time left for the failure.
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
        expiration = info.workflow_start_time + info.execution_timeout
        if now + backoff >= expiration:
            return False
    return True


def _non_retryable(err: BaseException, types: Sequence[str] | None) -> bool:
    if isinstance(err, temporalio.exceptions.ApplicationError):
        return err.non_retryable or (err.type or "") in (types or [])
    return type(err).__name__ in (types or [])


def _close_with_run(
    *,
    result: Any | None = None,
    failure: BaseException | None = None,
    chain_continues: bool = False,
) -> None:
    info = temporalio.workflow.info()
    default_id = _ids.default_stream_id(info.workflow_id, info.first_execution_run_id)
    state = _workflow._state()
    # The default Stream closes with the run even when this run wrote nothing, since an
    # earlier run of the chain may have written it. A close of a Stream with no key does
    # nothing.
    ids = (
        []
        if (default := state.streams.get(default_id)) and default._closed
        else [default_id]
    )
    for stream in state.streams.values():
        if stream.ref.id == default_id or stream._closed:
            continue
        if stream.auto_close and stream._used:
            ids.append(stream.ref.id)
    for stream in state.streams.values():
        if stream.ref.id in ids:
            stream._closed = True
    # A received ref keeps its own owner.
    refs = {
        stream_id: state.streams[stream_id]._model_ref()
        if stream_id in state.streams
        else _owned_ref(stream_id)
        for stream_id in ids
    }
    for run_stream in streams_workflow._state().streams.values():
        if run_stream.close_with_run and not run_stream.closed:
            run_stream.closed = True
            refs[run_stream.ref.id] = run_stream.ref
    for ref in refs.values():
        temporalio.workflow.send_to_plugins(
            _WorkflowCall(
                "CloseStream",
                _convert_in_sandbox(
                    models.CloseStreamRequest(
                        stream=ref,
                        result=result,
                        failure=failure,
                    )
                ),
                when_run_closes=True,
                chain_continues=chain_continues,
            ),
            # A close is idempotent, and its run may have no task left to send it on.
            during_replay=True,
        )


def _convert_in_sandbox(request: Any) -> Any:
    # A value from Workflow code may be of a class that only the sandbox knows, so the
    # Workflow's payload converter turns it into a Payload here. The hook outside runs only
    # the codec.
    converter = temporalio.workflow.payload_converter()
    if isinstance(request, models.AppendRecordsRequest):
        return dataclasses.replace(
            request,
            records=[
                dataclasses.replace(
                    record,
                    body=temporalio.common.RawValue(converter.to_payload(record.body)),
                )
                for record in request.records or []
            ],
        )
    if isinstance(request, models.CloseStreamRequest) and request.result is not None:
        return dataclasses.replace(
            request,
            result=temporalio.common.RawValue(converter.to_payload(request.result)),
        )
    return request


def _owned_ref(stream_id: str) -> models.StreamRef:
    # A Stream that the run creates, or its default Stream, is the run chain's own.
    info = temporalio.workflow.info()
    return models.StreamRef(
        id=stream_id,
        workflow_id=info.workflow_id,
        first_execution_run_id=info.first_execution_run_id,
    )


def _take_close_with_run(
    headers: Mapping[str, temporalio.api.common.v1.Payload],
) -> None:
    payload = headers.get(_CLOSE_WITH_RUN_HEADER)
    if payload is None:
        return
    state = streams_workflow._state()
    converter = temporalio.converter.PayloadConverter.default
    for stream_id in converter.from_payload(payload, list[str]):
        state.stream(_owned_ref(stream_id)).close_with_run = True


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
        info = temporalio.workflow.info()
        # Both ends use the default converter, so a custom user converter can't break the
        # headers.
        converter = temporalio.converter.PayloadConverter.default
        ref = _ids.default_stream_id(info.workflow_id, info.first_execution_run_id)
        return {
            _REF_HEADER: converter.to_payload(ref),
            _KEY_HEADER: converter.to_payload(str(self._activity_calls)),
        }

    async def start_system_nexus_operation(
        self, input: temporalio.worker.StartNexusOperationInput[Any, Any]
    ) -> temporalio.workflow.NexusOperationHandle[Any]:
        if input.service != _SERVICE:
            return await super().start_system_nexus_operation(input)
        operation = input.operation_name
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
        temporalio.workflow.send_to_plugins(
            _WorkflowCall(operation, _convert_in_sandbox(input.input)),
            # The runtime already has the Records of a replayed task. A create or a close is
            # idempotent, so it goes again, for a run whose buffer an eviction dropped.
            during_replay=operation != "AppendRecords",
        )
        return _DoneHandle(result)

    def continue_as_new(self, input: temporalio.worker.ContinueAsNewInput) -> NoReturn:
        open_ids = [
            stream_id
            for stream_id, run_stream in streams_workflow._state().streams.items()
            if run_stream.close_with_run and not run_stream.closed
        ]
        if open_ids:
            converter = temporalio.converter.PayloadConverter.default
            input.headers = {
                **input.headers,
                _CLOSE_WITH_RUN_HEADER: converter.to_payload(open_ids),
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
        plugin: StreamPlugin,
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
                if streams.default_ref is not None:
                    stream_ids.append(streams.default_ref.id)
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
    # The Streams that an Activity received, as arguments or as fields of its one input
    # object, which an earlier attempt may have written.
    handles: list[StreamHandle[Any]] = []
    for arg in args:
        if isinstance(arg, StreamHandle):
            handles.append(arg)
            continue
        fields = getattr(arg, "__dict__", None)
        if isinstance(fields, dict):
            handles.extend(
                value for value in fields.values() if isinstance(value, StreamHandle)
            )
    return handles


async def _close_quietly(streams: _activity.ActivityStreams) -> None:
    try:
        await asyncio.shield(streams.close())
    except Exception:
        temporalio.activity.logger.warning(
            "Failed flushing Stream Records of a failed Activity", exc_info=True
        )


def _activity_streams(
    input: temporalio.worker.ExecuteActivityInput, plugin: StreamPlugin
) -> _activity.ActivityStreams:
    # A sync Activity has no client, so the plugin gives the backend and the converter.
    info = temporalio.activity.info()
    converter = temporalio.converter.PayloadConverter.default
    ref = input.headers.get(_REF_HEADER)
    key = input.headers.get(_KEY_HEADER)
    key_text = converter.from_payload(key, str) if key is not None else ""
    epoch = stream_pb.ProducerEpoch(attempt=info.attempt)
    if info.is_local:
        task = info.workflow_task
        if task is not None:
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
        backend=plugin._temporal_stream_backend,
        converter=plugin._converter,
        namespace=info.namespace,
        producer=producer,
        default_ref=StreamRef(converter.from_payload(ref, str))
        if ref is not None
        else None,
        loop=asyncio.get_running_loop(),
        backend_name=plugin.backend_name,
    )
