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
from temporalio.contrib.streams import _activity, _errors, _ids, _workflow_reader
from temporalio.contrib.streams import workflow as streams_workflow
from temporalio.contrib.streams._backend import _ExtensionStreamBackend
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._plugin import StreamPlugin
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.system import models
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)

EXTENSION_NAME = "temporalio-streams-redis"

_SERVICE = "temporal.api.streamservice.v1.StreamService"
# How long a Worker's shutdown waits for the Stream closes that are still going out.
_DRAIN_TIMEOUT = timedelta(seconds=10)
# How long a Workflow read's Local Activity waits for new Records. Its timeout adds a margin.
_READ_WAIT = timedelta(seconds=10)
_READ_MARGIN = timedelta(seconds=10)
# The plugin puts this key into the headers of each Activity and Local Activity that a Workflow
# schedules. It keeps the Producer id the same across retries.
_KEY_HEADER = "__temporal_stream_key"
# Continue-as-new passes the run chain's open Streams to the next run in this header.
_OWNED_STREAMS_HEADER = "__temporal_owned_streams"


class _ExtensionStreamPlugin(StreamPlugin):
    # The Redis, memory and native plugins: the Stream calls go to a temporalio-streams
    # extension.
    _backend_class = _ExtensionStreamBackend
    _outbound_class: type[_WorkflowOutbound]
    # Whether the Store closes a Stream with its owner's run chain on the server.
    _server_closes_with_run = False

    def __init__(
        self,
        name: str,
        extension_name: str,
        config: ext_pb.RedisStreamConfig | ext_pb.NativeStreamConfig,
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
        self._backend = self._backend_class(self._extension)
        self._converter = temporalio.converter.DataConverter.default
        super().__init__(
            name,
            backend_name=backend_name,
            default=default,
            stream_backend=self._backend,
            interceptors=[_StreamInterceptor(self)],
        )
        # The read activity's name has the backend name, which the base keeps.
        self._read_activity = _read_activity(self)

    def _read_result(
        self,
        request: stream_pb.ReadRecordsRequest,  # pyright: ignore[reportUnusedParameter]
        page: stream_pb.ReadRecordsResponse,
    ) -> Any:
        # The result of a Workflow read's Local Activity, which its marker holds.
        return page

    def configure_worker(
        self, config: temporalio.worker.WorkerConfig
    ) -> temporalio.worker.WorkerConfig:
        """Add the plugin's interceptors and give Core the extension."""
        config = super().configure_worker(config)
        check_worker_backends(config)
        # Core takes one extension, so the extensions of two Stream plugins go as one.
        other = config.get("extension")
        config["extension"] = (
            temporalio.worker.WorkerExtension.combine(other, self._extension)
            if other is not None
            else self._extension
        )
        # Records get no serialization context, so Workflows, Activities and clients encode
        # them alike.
        client = config.get("client")
        if client is not None:
            self._converter = client.data_converter
        # Workflow reads run as this Local Activity, so the Worker must have it.
        config["activities"] = [*config.get("activities", []), self._read_activity]
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
            pending = await self._backend.drain_closes(_DRAIN_TIMEOUT)
            if pending:
                logger.warning(
                    "Worker shutdown left %d Stream closes in flight", pending
                )

    async def shutdown(self) -> None:
        """Flush and drop every open writer, including the client's, and wait for the closes
        in flight. Call it once the process no longer writes Streams.
        """
        backend = self._backend
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
        # Each Stream plugin of the Worker sees every item, and takes its own backend's calls.
        calls = [
            item
            for item in input.items
            if getattr(item, "_temporal_stream_call", False)
            and getattr(item, "backend", "") == self._backend_name
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
        self._backend.call("BufferWorkflowCalls", request)


def read_activity_name(backend: str) -> str:
    """The name of the Local Activity that reads a page of a backend for Workflow code."""
    return f"__temporal_stream_read:{backend}"


def _read_activity(plugin: _ExtensionStreamPlugin) -> Callable[..., Any]:
    @temporalio.activity.defn(name=read_activity_name(plugin.backend_name))
    async def read_page(request: stream_pb.ReadRecordsRequest) -> Any:
        info = temporalio.activity.info()
        request.namespace = info.namespace
        try:
            # The reading run goes with the read, so a native server keeps the Records that
            # the open run may read again on replay.
            page = await plugin._backend.call_async(
                "WorkflowReadRecords",
                ext_pb.WorkflowReadRecordsRequest(
                    read=request,
                    workflow_id=info.workflow_id or "",
                    run_id=info.workflow_run_id or "",
                ),
                stream_pb.ReadRecordsResponse,
            )
        except _errors.StreamError as err:
            # The reason goes in the details, so Workflow code gets the same Stream error.
            raise temporalio.exceptions.ApplicationError(
                str(err.message),
                err.reason,
                type=type(err).__name__,
                non_retryable=err.non_retryable,
            ) from None
        # Workflow code can't run the codec, so the page's bodies go to it decoded. The page
        # as a whole goes through the codec as the Local Activity's result.
        await _decode_page(plugin._converter, page)
        return plugin._read_result(request, page)

    return read_page


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


class RedisStreamPlugin(_ExtensionStreamPlugin):
    """Carries Stream calls to Redis, through the temporalio-streams runtime in the bridge.

    Put it on the client. A Worker takes its client's plugins, so one plugin serves Workflow
    code, Activities and the client. It registers the backend name ``redis``, which the
    Stream handles it creates carry. A client and a Worker may have more than one Stream
    plugin, such as this one and ``NativeStreamPlugin``. Each call goes to the backend that
    the handle names, and ``backend=`` picks one for a create.

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
            durable_writes: When above zero, each Workflow flush waits for this many replicas.
            durable_timeout: How long that wait takes at most. Defaults to 1 s.
            response_timeout: How long one Redis call takes at most. Defaults to 5 s.
            runtime: The runtime of the client. Defaults to the default runtime.
            name: The backend name that Stream handles carry, so every process routes a
                handle it receives.
            default: Make this the client's default backend when the client has more than
                one Stream plugin. With none marked, the first one is the default.
        """
        config = ext_pb.RedisStreamConfig(
            urls=[url] if isinstance(url, str) else list(url),
            cluster=cluster,
            key_prefix=key_prefix or "",
            durable_writes=durable_writes,
        )
        for field, value in (
            ("retention", retention),
            ("safety_ttl", safety_ttl),
            ("durable_timeout", durable_timeout),
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
    backend: str = ""
    # A create's ref with the owner fields, and whether the Stream closes with the run chain.
    owner_ref: models.StreamRef | None = None
    close_with_run: bool = False


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
        owner = call.owner_ref
        if owner is not None:
            create.close_with_run = call.close_with_run
            create.ref.CopyFrom(
                stream_pb.StreamRef(
                    id=owner.id,
                    workflow_id=owner.workflow_id or "",
                    first_execution_run_id=owner.first_execution_run_id or "",
                )
            )
        if request.retention is not None:
            create.retention.FromTimedelta(request.retention)
        if request.auto_close is not None:
            create.auto_close.FromTimedelta(request.auto_close)
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
    def __init__(self, plugin: _ExtensionStreamPlugin) -> None:
        super().__init__()
        self._plugin = plugin

    def workflow_interceptor_class(
        self, input: temporalio.worker.WorkflowInterceptorClassInput
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        # Workflow code learns the Worker's backend names from the interceptor classes.
        return type(
            "_WorkflowInbound",
            (_WorkflowInbound,),
            {
                "_backend_name": self._plugin.backend_name,
                "_default": self._plugin.default,
                "_outbound_class": self._plugin._outbound_class,
                "_server_closes_with_run": self._plugin._server_closes_with_run,
            },
        )

    def intercept_activity(
        self, next: temporalio.worker.ActivityInboundInterceptor
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInbound(next, self._plugin)


class _WorkflowInbound(temporalio.worker.WorkflowInboundInterceptor):
    _backend_name = ""
    _default = False
    # Each plugin's interceptor sets it on its own class.
    _outbound_class: type[_WorkflowOutbound]  # pyright: ignore[reportUninitializedInstanceVariable]
    _server_closes_with_run = False

    def _close_run_streams(self, **options: Any) -> None:
        # A server that closes a Stream with its owner's run chain needs no close from here.
        if not self._server_closes_with_run:
            _close_with_run(self._backend_name, **options)

    def init(self, outbound: temporalio.worker.WorkflowOutboundInterceptor) -> None:
        super().init(self._outbound_class(outbound, self._backend_name, self._default))

    async def execute_workflow(
        self, input: temporalio.worker.ExecuteWorkflowInput
    ) -> Any:
        _take_owned_streams(input.headers)
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
                self._close_run_streams(
                    failure=temporalio.exceptions.CancelledError("Workflow cancelled"),
                )
            elif _fails_run(err):
                self._close_run_streams(
                    failure=err,
                    chain_continues=chain_continues(
                        temporalio.workflow.info(), temporalio.workflow.now(), err
                    ),
                )
            raise
        # A cron run that completes still goes on with the chain.
        self._close_run_streams(
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


def _close_with_run(
    backend: str,
    *,
    result: Any | None = None,
    failure: BaseException | None = None,
    chain_continues: bool = False,
) -> None:
    # Each Stream plugin's inbound interceptor closes the Streams of its own backend.
    default = streams_workflow._backend_name()
    for run_stream in streams_workflow._state().streams.values():
        if not run_stream.close_with_run or run_stream.closed:
            continue
        if (run_stream.backend or default) != backend:
            continue
        run_stream.closed = True
        temporalio.workflow.send_to_plugins(
            _WorkflowCall(
                "CloseStream",
                _convert_in_sandbox(
                    models.CloseStreamRequest(
                        stream=run_stream.ref, result=result, failure=failure
                    )
                ),
                when_run_closes=True,
                chain_continues=chain_continues,
                backend=backend,
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


def _closes_with_run(
    input: temporalio.worker.StartNexusOperationInput[Any, Any],
) -> bool:
    if input.operation_name != "CreateStream":
        return False
    run_stream = streams_workflow._state().streams.get(input.input.stream_id)
    return run_stream is not None and run_stream.close_with_run


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


def pass_owned_streams(input: temporalio.worker.ContinueAsNewInput) -> None:
    """Put the run chain's open Streams in the headers of the next run."""
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


class _WorkflowOutbound(temporalio.worker.WorkflowOutboundInterceptor):
    _temporal_stream_transport = True
    # The runtime can't park a read yet, so a reader pauses on a timer after an empty page.
    _temporal_stream_parks_reads = False
    _temporal_stream_read_wait = _READ_WAIT

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

    async def start_system_nexus_operation(
        self, input: temporalio.worker.StartNexusOperationInput[Any, Any]
    ) -> temporalio.workflow.NexusOperationHandle[Any]:
        if input.service != _SERVICE or not _targets(
            self._temporal_stream_backend_name
        ):
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
            _WorkflowCall(
                operation,
                _convert_in_sandbox(input.input),
                backend=self._temporal_stream_backend_name,
                owner_ref=_owned_ref(input.input.stream_id)
                if operation == "CreateStream"
                else None,
                close_with_run=_closes_with_run(input),
            ),
            # The runtime already has the Records of a replayed task. A create or a close is
            # idempotent, so it goes again, for a run whose buffer an eviction dropped.
            during_replay=operation != "AppendRecords",
        )
        return _DoneHandle(result)

    async def _temporal_stream_read_page(
        self, request: stream_pb.ReadRecordsRequest
    ) -> stream_pb.ReadRecordsResponse:
        # The page is the Local Activity's result, so it is in History.
        try:
            return await temporalio.workflow.execute_local_activity(
                read_activity_name(self._temporal_stream_backend_name),
                request,
                result_type=stream_pb.ReadRecordsResponse,
                start_to_close_timeout=_READ_WAIT + _READ_MARGIN,
            )
        except (
            temporalio.exceptions.ActivityError,
            temporalio.exceptions.ApplicationError,
        ) as err:
            # A Local Activity's failure may come as its application error, with no
            # ActivityError around it.
            raise _workflow_reader.error_of(err) from None

    def continue_as_new(self, input: temporalio.worker.ContinueAsNewInput) -> NoReturn:
        pass_owned_streams(input)
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
        self._temporal_stream_backend_name = plugin.backend_name
        self._temporal_stream_default = plugin.default

    async def execute_activity(
        self, input: temporalio.worker.ExecuteActivityInput
    ) -> Any:
        streams = _activity_streams(input, self._plugin)
        attempt, token = enter_attempt(self, streams)
        try:
            await _flush_run_before_local_activity(self._plugin._backend, streams)
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


def _targets(backend: str) -> bool:
    # Whether the facade's call now is for this backend. A call that the facade did not make
    # goes to the default backend.
    target = streams_workflow._target.get() or streams_workflow._backend_name()
    return target == backend


async def _flush_run_before_local_activity(
    backend: _ExtensionStreamBackend, streams: _activity.ActivityStreams
) -> None:
    # The run's buffered Records go before the Local Activity's, in the epoch of the Workflow
    # Task that dispatched it. A failure fails the attempt, and the run's next completion
    # fails too, so its retry withdraws what was written.
    info = temporalio.activity.info()
    task = info.workflow_task
    if not info.is_local or task is None or not info.workflow_run_id:
        return
    epoch = stream_pb.WorkflowTaskEpoch(
        started_event_id=task.started_event_id, attempt=task.attempt
    )
    if task.started_time is not None:
        epoch.started_time.FromDatetime(task.started_time)
    await backend.flush_run_buffer(streams.namespace, info.workflow_run_id, epoch)


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
        backend=plugin.stream_backend,
        converter=plugin._converter,
        namespace=info.namespace,
        producer=producer,
        loop=asyncio.get_running_loop(),
        backend_name=plugin.backend_name,
    )


_ExtensionStreamPlugin._outbound_class = _WorkflowOutbound
