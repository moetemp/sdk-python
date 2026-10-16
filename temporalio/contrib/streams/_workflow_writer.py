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

from typing import Any, Generic, TypeVar, overload

from temporalio import workflow
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.bridge.proto.workflow_commands import OutputRecord
from temporalio.contrib.streams._body import content_hash
from temporalio.contrib.streams._errors import StreamError, StreamUnsupportedError
from temporalio.contrib.streams._local_activity import _WorkflowInbound
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic

__all__ = [
    "WorkflowStreamWriter",
    "workflow_writer",
]

T = TypeVar("T")

# The same per-record limit the stream service keeps, checked in the Workflow
# so an oversized value fails at the call.
_MAX_RECORD_BYTES = 3 << 19


class _RunWriters:
    """One run's stream output, across all its writers."""

    def __init__(self) -> None:
        # FINISH is a statement about the topic, not about a writer object.
        self.finished: set[str] = set()

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
    ``FINISH``. Publishing adds no event to History. A store that refuses
    the append fails the Workflow Task, which retries.

    Args:
        topic: A :func:`temporalio.contrib.streams.topic` definition, whose
            value type the writer's ``publish`` takes, or a plain string.
            Without one, the writer is on
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.

    Raises:
        ValueError: ``topic`` is empty.
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator.
        temporalio.contrib.streams.StreamUnsupportedError: The Worker has no
            stream store interceptor.
    """
    _check_writable("open a writer on")
    name, _ = resolve_topic(topic)
    writers = _run_writers(create=True)
    assert writers is not None
    return WorkflowStreamWriter(writers, name)
