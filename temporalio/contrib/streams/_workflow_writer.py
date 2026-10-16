"""Publishing to a Workflow's own stream from its Workflow Task.

Workflow code can't do I/O. :func:`workflow_writer` converts and hashes each
value on the Workflow thread and adds the record to the activation's
``WorkflowOutputStreamCommit``. The Worker's completion encoder runs each
body through the payload codec, and Core appends the records to the store
when the activation completes, before it reports the completion to the
server. Nothing about them goes to History.

The run is the producer ``@<run id>``. Core gives each Workflow Task a
producer attempt from History, so a task that fails and runs again writes a
higher attempt, and readers see ``SUPERSEDED`` for the records of the failed
task only. Records of tasks that completed stay.

Replay sends no commit, since the records are in the store already.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Generic, TypeVar, overload

import temporalio.activity
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams import CloseRequest
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.bridge.proto.workflow_commands import OutputRecord
from temporalio.common import RawValue, RetryPolicy
from temporalio.contrib.streams._body import content_hash, encode_bodies
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamError,
    StreamNotFoundError,
    StreamProducerError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._handles import activity_handle
from temporalio.contrib.streams._local_activity import _WorkflowInbound
from temporalio.contrib.streams._plugin import call
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.converter._payload_codec import _apply_to_failure_payloads
from temporalio.exceptions import ApplicationError

__all__ = [
    "CLOSE_ACTIVITY",
    "WorkflowStreamWriter",
    "close_activity",
    "close_workflow_stream",
    "workflow_writer",
]

T = TypeVar("T")

CLOSE_ACTIVITY = "__temporal_streams_close"
"""The name the built-in close Activity is registered under."""

_DEFAULT_START_TO_CLOSE = timedelta(seconds=10)

# The same per-record limit the stream service keeps, checked in the Workflow
# so an oversized value fails at the call.
_MAX_RECORD_BYTES = 3 << 19
_PAYLOADS_ENCODING = b"binary/protobuf"
_CLOSE_TYPE = CloseRequest.DESCRIPTOR.full_name.encode()
# Errors that the same close raises again on every attempt.
_FINAL_ERRORS = (
    ValueError,
    StreamClosedError,
    StreamNotFoundError,
    StreamProducerError,
    StreamUnsupportedError,
)


class _RunWriters:
    """One run's stream output, across all its writers."""

    def __init__(self) -> None:
        # FINISH is a statement about the topic, not about a writer object.
        self.finished: set[str] = set()
        # Topics this run closed, which no writer may publish on again.
        self.closed: set[str] = set()

    def add(self, record: OutputRecord) -> None:
        """Add ``record`` to this activation's commit.

        Replay adds nothing, since Core stored the record when the
        activation first ran. No limit on the activation's bytes is checked
        here, since replay can't check it the same way, and a caught error
        would then give other commands on replay.
        """
        if workflow.unsafe.is_replaying():
            return
        workflow._Runtime.current().workflow_stream_commit().records.append(record)


# Held on the run's runtime, so the state goes with the run at eviction and is
# rebuilt in order on replay.
_STATE_ATTRIBUTE = "_temporal_stream_writers"


def _run_writers(*, create: bool) -> _RunWriters | None:
    runtime = workflow._Runtime.current()
    writers: _RunWriters | None = getattr(runtime, _STATE_ATTRIBUTE, None)
    if writers is None and create:
        if not _has_streams_interceptor(runtime):
            raise StreamUnsupportedError(
                "workflow_writer needs the stream store's Worker interceptor; build "
                "the Worker from a client that carries the store, or add the store "
                "plugin to the Replayer"
            )
        writers = _RunWriters()
        setattr(runtime, _STATE_ATTRIBUTE, writers)
    return writers


def _has_streams_interceptor(runtime: Any) -> bool:
    inbound = getattr(runtime, "_inbound", None)
    while inbound is not None:
        if isinstance(inbound, _WorkflowInbound):
            return True
        inbound = getattr(inbound, "next", None)
    return False


def _check_writable(action: str) -> None:
    if workflow.unsafe.is_read_only():
        raise workflow.ReadOnlyContextError(
            f"cannot {action} a stream in a query or an update validator"
        )


class WorkflowStreamWriter(Generic[T]):
    """Publishes to one topic of the running Workflow's own stream.

    Each record goes with the completion of the activation that published
    it, and Core appends it before it reports that completion.
    """

    def __init__(self, writers: _RunWriters, topic: str) -> None:
        """Prefer :func:`temporalio.contrib.streams.workflow_writer`."""
        self._writers = writers
        self._topic = topic

    @property
    def topic(self) -> str:
        """The name of the topic this writer is bound to."""
        return self._topic

    def publish(self, value: T) -> None:
        """Publish ``value`` on this topic.

        The value is converted here, with the Workflow's payload converter. A
        :class:`temporalio.common.RawValue` passes through pre-encoded. The
        record is visible when the activation that published it completes,
        which is before the Workflow Task completes. If the task then fails or
        times out, the record stays, and the task's rerun publishes again
        under a higher attempt, which readers see as ``SUPERSEDED``.

        Raises:
            ValueError: The topic was already finished in this run.
            temporalio.contrib.streams.StreamError: The value's payload is
                over 1.5 MiB. Nothing is published.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
        """
        _check_writable("publish to")
        # A close finishes the topic first, so this covers a closed topic too.
        if self._topic in self._writers.finished:
            raise ValueError(f"topic {self._topic!r} was already finished")
        payload = workflow.payload_converter().to_payloads([value])[0]
        if payload.ByteSize() > _MAX_RECORD_BYTES:
            raise StreamError(
                f"a record of {payload.ByteSize()} bytes on {self._topic!r} is over "
                f"the per-record limit of {_MAX_RECORD_BYTES} bytes"
            )
        self._writers.add(
            OutputRecord(
                topic=self._topic,
                kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
                body=payload,
                content_hash=content_hash(payload),
            )
        )

    async def flush(self) -> None:
        """Return at once, since each record goes with its activation.

        Kept so code written for the Local Activity writer runs unchanged.

        Raises:
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
        """
        _check_writable("flush")

    async def finish(self) -> None:
        """Write ``FINISH`` on this topic. Idempotent.

        Says this run publishes nothing more on the topic. The record goes
        with this activation, after what the run published before. A run
        that follows by Continue-as-New can publish on the topic again. It
        does not say the Workflow succeeded, and it does not end anyone's
        read.

        Raises:
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
        """
        _check_writable("finish")
        if self._topic in self._writers.finished:
            return
        self._writers.add(
            OutputRecord(
                topic=self._topic, kind=StreamRecordKind.STREAM_RECORD_KIND_FINISH
            )
        )
        self._writers.finished.add(self._topic)


@overload
def workflow_writer(topic: StreamTopic[T]) -> WorkflowStreamWriter[T]: ...


@overload
def workflow_writer(topic: str | None = None) -> WorkflowStreamWriter[Any]: ...


def workflow_writer(
    topic: str | StreamTopic[Any] | None = None,
) -> WorkflowStreamWriter[Any]:
    """A writer on ``topic`` of the running Workflow's own stream.

    Call it from Workflow code. All writers on one topic of a run share one
    ``FINISH``. Publishing adds no event to History. A store that fails the
    append fails the Workflow Task, which retries. A topic the store reports
    closed drops the activation's records on it, since no retry could land.

    Args:
        topic: A :func:`temporalio.contrib.streams.topic` definition, whose
            value type the writer's ``publish`` takes, or a plain string.
            Without one, the writer is on
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.

    Raises:
        ValueError: ``topic`` is empty, or :func:`close_workflow_stream`
            closed the topic in this run.
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator.
        temporalio.contrib.streams.StreamUnsupportedError: The Worker has no
            stream store interceptor.
    """
    _check_writable("open a writer on")
    name, _ = resolve_topic(topic)
    writers = _run_writers(create=True)
    assert writers is not None
    if name in writers.closed:
        raise ValueError(f"topic {name!r} was already closed in this run")
    return WorkflowStreamWriter(writers, name)


def _closer(run_id: str | None) -> str:
    """The ``closed_by`` a Workflow run's close names it with."""
    return f"@{run_id or ''}"


async def close_workflow_stream(
    topic: str | StreamTopic[Any] | None = None,
    *,
    result: Any = None,
    error: BaseException | None = None,
    start_to_close_timeout: timedelta | None = _DEFAULT_START_TO_CLOSE,
    retry_policy: RetryPolicy | None = None,
) -> None:
    """Close ``topic`` of the running Workflow's own stream, with ``result`` or ``error``.

    Call it from Workflow code, before the run ends. The run's ``FINISH`` on
    the topic goes with this activation, after what the run published
    before. Core appends it before it runs the close's Local Activity, which
    closes the stream in the store and on the server. Without the store's
    Worker interceptor, that Local Activity writes ``FINISH`` itself. Every
    read ends after the last record, and each reader that sleeps on the
    stream wakes and sees the outcome: a Workflow reader raises ``error``
    after its last batch.

    ``result`` goes through the Workflow's payload converter here and the
    codec in the Local Activity. ``error`` goes through the Workflow's
    failure converter. Without a close, a stream closes when its owner's run
    chain ends: the server's notifier checks the owner and completes its
    readers, and a client reader ends at its next read.

    The close names the run as its ``closed_by``, ``@<run id>``, which is
    also the run's producer id. After a failed Workflow Task, the task runs
    again and publishes again, and the store refuses those appends as closed
    by this run. Everything the run published before the close landed before
    it, so Core drops those records, and the close's rerun takes the
    refusal as done. The first close's outcome stays.

    Args:
        topic: The topic to close. Without one,
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        result: What the stream closes with. Without one, a null result.
        error: A failure to close the stream with, instead of a result.
        start_to_close_timeout: How long one attempt of the close may run.
            10 seconds by default.
        retry_policy: How the close retries. Without one, it is Temporal's
            default for Activities.

    Raises:
        ValueError: Both ``result`` and ``error`` are set.
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator.
        BaseException: The close's Local Activity failure.

    .. warning::
        This API is experimental.
    """
    _check_writable("close")
    if result is not None and error is not None:
        raise ValueError("a stream closes with a result or an error, not both")
    name, _ = resolve_topic(topic)
    outcome = CloseRequest()
    if error is not None:
        runtime: Any = workflow._Runtime.current()
        runtime._workflow_context_failure_converter.to_failure(
            error, workflow.payload_converter(), outcome.failure
        )
    elif result is not None:
        outcome.result.CopyFrom(workflow.payload_converter().to_payloads([result])[0])
    # Without the interceptor no writer can open, so none has to be refused.
    writers = _run_writers(create=_has_streams_interceptor(workflow._Runtime.current()))
    if writers is not None:
        await WorkflowStreamWriter(writers, name).finish()
        writers.closed.add(name)
    await workflow.execute_local_activity(
        CLOSE_ACTIVITY,
        args=[
            name,
            writers is None,
            RawValue(
                Payload(
                    metadata={
                        "encoding": _PAYLOADS_ENCODING,
                        "messageType": _CLOSE_TYPE,
                    },
                    data=outcome.SerializeToString(),
                )
            ),
        ],
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        summary=f"stream close {name}",
    )


@temporalio.activity.defn(name=CLOSE_ACTIVITY)
async def close_activity(topic: str, finish: bool, outcome: RawValue) -> None:
    """Close a Workflow's own stream, as :func:`close_workflow_stream` asks.

    It writes ``FINISH`` first when asked, as this Local Activity's
    producer. Then it sends Core's close with the outcome through the
    codec, which closes the store and then the stream's notifier.
    """
    if outcome.payload.metadata.get("messageType") != _CLOSE_TYPE:
        raise ApplicationError(
            "the stream close input is not a CloseRequest", non_retryable=True
        )
    request = CloseRequest.FromString(outcome.payload.data)
    stream = activity_handle()
    handle = stream._inner
    try:
        if finish:
            try:
                await stream.producer(topic=topic).finish()
            except StreamClosedError as closed:
                # A rerun after a failed Workflow Task finds its own close.
                if closed.closed_by != _closer(
                    temporalio.activity.info().workflow_run_id
                ):
                    raise
        converter = handle._converter
        if request.HasField("result"):
            [payload] = await encode_bodies(converter, [request.result])
            request.result.CopyFrom(payload)
        if request.HasField("failure"):
            await _apply_to_failure_payloads(
                request.failure, converter._transform_outbound_payloads
            )
        request.stream.CopyFrom(handle._address(topic))
        request.closed_by = _closer(temporalio.activity.info().workflow_run_id)
        service = await handle._plugin._service_for(handle._client)
        await call(service.close(request))
    except _FINAL_ERRORS as error:
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error
