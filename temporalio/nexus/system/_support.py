import asyncio
import collections.abc
import dataclasses
import typing
import uuid
from datetime import timedelta

import google.protobuf.duration_pb2

import temporalio.api.common.v1.message_pb2 as common_pb2
import temporalio.api.enums.v1.workflow_pb2 as workflow_enums_pb2
import temporalio.api.failure.v1.message_pb2 as failure_pb2
import temporalio.api.taskqueue.v1.message_pb2 as taskqueue_pb2
import temporalio.api.workflow.v1
import temporalio.common
import temporalio.converter
import temporalio.nexus.system


class SignalWithStartWorkflowModelRequest(typing.Protocol):
    namespace: str
    id: str


def retry_policy_from_proto(
    proto: common_pb2.RetryPolicy,
) -> temporalio.common.RetryPolicy:
    return temporalio.common.RetryPolicy.from_proto(proto)


def retry_policy_to_proto(
    retry_policy: temporalio.common.RetryPolicy,
) -> common_pb2.RetryPolicy:
    proto = common_pb2.RetryPolicy()
    retry_policy.apply_to_proto(proto)
    return proto


def workflow_function_name(
    value: str | collections.abc.Callable[..., collections.abc.Awaitable[object]],
) -> str:
    from temporalio.workflow import _Definition  # pyright: ignore[reportPrivateUsage]

    name, _result_type = _Definition.get_name_and_result_type(value)
    return name


def signal_function_to_proto(
    value: str | collections.abc.Callable[..., typing.Any],
) -> str:
    from temporalio.workflow import (
        _SignalDefinition,  # pyright: ignore[reportPrivateUsage]
    )

    return _SignalDefinition.must_name_from_fn_or_str(value)  # pyright: ignore[reportUnknownMemberType]


def workflow_type_to_proto(
    workflow_type: str
    | collections.abc.Callable[..., collections.abc.Awaitable[object]],
) -> common_pb2.WorkflowType:
    return common_pb2.WorkflowType(name=workflow_function_name(workflow_type))


def workflow_type_from_proto(
    proto: common_pb2.WorkflowType,
) -> str:
    return proto.name


def task_queue_from_proto(
    proto: taskqueue_pb2.TaskQueue,
) -> str:
    return proto.name


def task_queue_to_proto(
    task_queue: str,
) -> taskqueue_pb2.TaskQueue:
    return taskqueue_pb2.TaskQueue(name=task_queue)


def workflow_namespace() -> str:
    from temporalio.workflow import info

    return info().namespace


def signal_with_start_workflow_serialization_context(
    request: SignalWithStartWorkflowModelRequest,
) -> temporalio.converter.WorkflowSerializationContext:
    return temporalio.converter.WorkflowSerializationContext(
        namespace=request.namespace,
        workflow_id=request.id,
    )


def payloads_to_proto(
    values: collections.abc.Sequence[typing.Any],
) -> common_pb2.Payloads:
    return (
        temporalio.nexus.system._current_user_payload_converter().to_payloads_wrapper(
            values
        )
    )


def payloads_from_proto(
    proto: common_pb2.Payloads,
) -> list[object]:
    return list(
        temporalio.nexus.system._current_user_payload_converter().from_payloads_wrapper(
            proto
        )
    )


def _clone_payload(payload: common_pb2.Payload) -> common_pb2.Payload:
    clone = common_pb2.Payload()
    clone.CopyFrom(payload)
    return clone


def _value_to_payload(value: object | common_pb2.Payload) -> common_pb2.Payload:
    if isinstance(value, common_pb2.Payload):
        return _clone_payload(value)
    payloads = (
        temporalio.nexus.system._current_user_payload_converter().to_payloads_wrapper(
            [value]
        )
    )
    return _clone_payload(payloads.payloads[0])


def _payload_to_value(
    payload: common_pb2.Payload, type_hint: typing.Any = None
) -> object:
    return temporalio.nexus.system._current_user_payload_converter().from_payloads(
        [_clone_payload(payload)], [type_hint] if type_hint is not None else None
    )[0]


def payload_from_proto(
    proto: common_pb2.Payload,
    type_hint: typing.Any = None,
) -> object:
    return _payload_to_value(proto, type_hint)


def payload_to_proto(
    payload: object,
) -> common_pb2.Payload:
    return _value_to_payload(payload)


def failure_from_proto(
    proto: failure_pb2.Failure,
) -> BaseException:
    return temporalio.nexus.system._current_user_failure_converter().from_failure(
        proto, temporalio.nexus.system._current_user_payload_converter()
    )


def failure_to_proto(
    failure: BaseException,
) -> failure_pb2.Failure:
    proto = failure_pb2.Failure()
    temporalio.nexus.system._current_user_failure_converter().to_failure(
        failure, temporalio.nexus.system._current_user_payload_converter(), proto
    )
    return proto


def memo_from_proto(
    proto: common_pb2.Memo,
) -> collections.abc.Mapping[str, object]:
    return {key: _payload_to_value(value) for key, value in proto.fields.items()}


def memo_to_proto(
    memo: collections.abc.Mapping[str, object],
) -> common_pb2.Memo:
    message = common_pb2.Memo()
    for key, value in memo.items():
        message.fields[key].CopyFrom(_value_to_payload(value))
    return message


def header_from_proto(
    proto: common_pb2.Header,
) -> collections.abc.Mapping[str, object]:
    return {key: _payload_to_value(value) for key, value in proto.fields.items()}


def header_to_proto(
    header: collections.abc.Mapping[str, object],
) -> common_pb2.Header:
    message = common_pb2.Header()
    for key, value in header.items():
        message.fields[key].CopyFrom(_value_to_payload(value))
    return message


def duration_from_proto(proto: google.protobuf.duration_pb2.Duration) -> timedelta:
    return proto.ToTimedelta()


def duration_to_proto(
    duration: timedelta,
) -> google.protobuf.duration_pb2.Duration:
    proto = google.protobuf.duration_pb2.Duration()
    proto.FromTimedelta(duration)
    return proto


def workflow_id_reuse_policy_from_proto(
    policy: workflow_enums_pb2.WorkflowIdReusePolicy.ValueType,
) -> temporalio.common.WorkflowIDReusePolicy:
    return temporalio.common.WorkflowIDReusePolicy(int(policy))


def workflow_id_reuse_policy_to_proto(
    policy: temporalio.common.WorkflowIDReusePolicy,
) -> workflow_enums_pb2.WorkflowIdReusePolicy.ValueType:
    return typing.cast(workflow_enums_pb2.WorkflowIdReusePolicy.ValueType, int(policy))


def workflow_id_conflict_policy_from_proto(
    policy: workflow_enums_pb2.WorkflowIdConflictPolicy.ValueType,
) -> temporalio.common.WorkflowIDConflictPolicy:
    return temporalio.common.WorkflowIDConflictPolicy(int(policy))


def workflow_id_conflict_policy_to_proto(
    policy: temporalio.common.WorkflowIDConflictPolicy,
) -> workflow_enums_pb2.WorkflowIdConflictPolicy.ValueType:
    return typing.cast(
        workflow_enums_pb2.WorkflowIdConflictPolicy.ValueType, int(policy)
    )


def search_attributes_to_proto(
    search_attributes: temporalio.common.TypedSearchAttributes,
) -> common_pb2.SearchAttributes:
    proto = common_pb2.SearchAttributes()
    temporalio.converter.encode_search_attributes(search_attributes, proto)
    return proto


def search_attributes_from_proto(
    proto: common_pb2.SearchAttributes,
) -> temporalio.common.TypedSearchAttributes:
    return temporalio.converter.decode_typed_search_attributes(proto)


def priority_from_proto(
    proto: common_pb2.Priority,
) -> temporalio.common.Priority:
    return temporalio.common.Priority._from_proto(proto)  # pyright: ignore[reportPrivateUsage]


def priority_to_proto(
    priority: temporalio.common.Priority,
) -> common_pb2.Priority:
    return priority._to_proto()  # pyright: ignore[reportPrivateUsage]


def versioning_override_to_proto(
    versioning_override: temporalio.common.VersioningOverride,
) -> temporalio.api.workflow.v1.VersioningOverride:
    return versioning_override._to_proto()  # pyright: ignore[reportPrivateUsage]


def versioning_override_from_proto(
    proto: temporalio.api.workflow.v1.VersioningOverride,
) -> temporalio.common.VersioningOverride:
    if proto.HasField("pinned") and proto.pinned.HasField("version"):
        version = proto.pinned.version
        return temporalio.common.PinnedVersioningOverride(
            temporalio.common.WorkerDeploymentVersion(
                deployment_name=version.deployment_name,
                build_id=version.build_id,
            )
        )
    if proto.HasField("one_time") and proto.one_time.HasField(
        "target_deployment_version"
    ):
        version = proto.one_time.target_deployment_version
        return temporalio.common.OneTimeVersioningOverride(
            temporalio.common.WorkerDeploymentVersion(
                deployment_name=version.deployment_name,
                build_id=version.build_id,
            )
        )
    if proto.pinned_version:
        return temporalio.common.PinnedVersioningOverride(
            temporalio.common.WorkerDeploymentVersion.from_canonical_string(
                proto.pinned_version
            )
        )
    if proto.auto_upgrade:
        return temporalio.common.AutoUpgradeVersioningOverride()
    raise ValueError("unknown versioning override proto shape")


class _ReadRequest(typing.Protocol):
    __dataclass_fields__: typing.ClassVar[dict[str, typing.Any]]

    @property
    def from_offset(self) -> bytes | None: ...

    @property
    def wait(self) -> timedelta | None: ...


class _ReadResponse(typing.Protocol):
    @property
    def records(self) -> collections.abc.Sized | None: ...

    @property
    def last_offset(self) -> bytes | None: ...

    @property
    def done(self) -> bool | None: ...


_ReadRequestT = typing.TypeVar("_ReadRequestT", bound=_ReadRequest)


def next_read_request(
    request: _ReadRequestT, response: _ReadResponse
) -> _ReadRequestT | None:
    """The request after one page of a Stream read, or None to stop.

    A read ends when the Stream is closed and the read gave its last Record. A read with no wait
    also ends at an empty page that didn't move, since the Store had nothing past the newest
    Record.
    """
    if response.done or response.last_offset is None:
        return None
    moved = response.last_offset > (request.from_offset or b"")
    if not response.records and not moved and request.wait is None:
        return None
    return _read_after(request, response.last_offset)


class _ReadItem(typing.Protocol):
    @property
    def offset(self) -> bytes: ...


def resume_read_request(request: _ReadRequestT, item: _ReadItem) -> _ReadRequestT:
    """The request that resumes a Stream read after ``item``."""
    return _read_after(request, item.offset)


def _read_after(request: _ReadRequestT, offset: bytes) -> _ReadRequestT:
    # A from-offset is exclusive, so the read starts after the Record at that offset. It
    # replaces the first page's start.
    return dataclasses.replace(request, from_offset=offset)


class _AppendResponse(typing.Protocol):
    @property
    def last_offset(self) -> bytes | None: ...


def last_offset(response: _AppendResponse) -> bytes:
    """The offset of the last Record that an append wrote, or empty when it wrote none."""
    return response.last_offset or b""


_T = typing.TypeVar("_T")


def start_now(call: collections.abc.Awaitable[_T]) -> asyncio.Task[_T]:
    """Run ``call`` up to its first wait now, in a task that gives its result.

    A Workflow call adds its command before it first waits, so the command keeps its place in
    the order of the caller's commands, as ``workflow.start_activity`` does.
    """

    async def run() -> _T:
        return await call

    # A task runs only when the loop gets to it, so step the call here and let a task drive the
    # rest. Every Python version takes this path, so a replay on another version gives the same
    # commands.
    loop = asyncio.get_running_loop()
    coroutine = run()
    try:
        waiting = coroutine.send(None)
    except StopIteration as finished:
        return loop.create_task(_finished(typing.cast(_T, finished.value)))
    except BaseException as error:
        return loop.create_task(_raised(error))
    return loop.create_task(_resume(coroutine, waiting))


async def _finished(value: _T) -> _T:
    return value


async def _raised(error: BaseException) -> typing.NoReturn:
    raise error


async def _resume(
    coroutine: collections.abc.Coroutine[object, None, _T], waiting: object
) -> _T:
    # Drive a started coroutine as a task does. A cancel goes to the future that it waits on.
    while True:
        cancelled: asyncio.CancelledError | None = None
        try:
            if isinstance(waiting, asyncio.Future):
                future = typing.cast("asyncio.Future[object]", waiting)
                done = asyncio.get_running_loop().create_future()

                def wake(_: object, done: asyncio.Future[None] = done) -> None:
                    if not done.done():
                        done.set_result(None)

                future.add_done_callback(wake)
                await done
            else:
                await asyncio.sleep(0)
        except asyncio.CancelledError as error:
            if isinstance(waiting, asyncio.Future):
                typing.cast("asyncio.Future[object]", waiting).cancel()
            cancelled = error
        try:
            waiting = (
                coroutine.send(None)
                if cancelled is None
                else coroutine.throw(cancelled)
            )
        except StopIteration as finished:
            return typing.cast(_T, finished.value)


def request_id() -> str:
    """The id of one create call, which its retries send again.

    Outside a Workflow it's a random UUID. In a Workflow it's empty, since the Workflow create
    sets an id that is the same in each run of the chain.
    """
    from temporalio.workflow import in_workflow

    # A workflow.uuid4() here would move the user's workflow.random() sequence, and the
    # Workflow create replaces the id anyway.
    return "" if in_workflow() else str(uuid.uuid4())


# The Stream sources. Each one checks where it runs and gives the value of that context, or
# raises where it has none to give. The Streams API imports this module, so the sources import
# the API only when they run.


def workflow_producer() -> typing.Any:
    """The Producer of the Workflow run's writes."""
    from temporalio.streams import _ids
    from temporalio.workflow import info

    return _ids.workflow_producer(info().run_id)


def client_producer() -> typing.Any:
    """The Producer of a write outside a Workflow, which is the Activity attempt's Producer."""
    from temporalio.streams import _activity
    from temporalio.streams.system import models

    streams = _activity.current.get()
    if streams is None:
        # Each client has its own Producer, and a generated call doesn't say which client makes
        # it. So the SDK's Streams send their Producer, and a call without one fails.
        raise RuntimeError("A Stream write outside a Workflow needs its Producer")
    return models._ProducerTransferTypeConverter().from_transfer_type(
        streams.producer, models.Producer
    )
