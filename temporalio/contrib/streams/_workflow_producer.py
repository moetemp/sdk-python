"""Publishing to a Workflow's own stream from its Workflow Task.

Workflow code can't do I/O. The Workflow Producer of :func:`stream_producer`
converts and hashes each value on the Workflow thread and adds the record to
the activation's output. When the activation ends, the Worker runs each body through the
payload codec and appends the records to the store with one
``PublishWorkflowTask`` call, before it gives the completion to Core.
Nothing about them goes to History.

The run is the producer ``@<run id>``. The stream runtime gives each
Workflow Task a producer attempt from the task facts Core puts on the
activation, which come from History. So a task that fails and runs again
writes a higher attempt, and readers see ``SUPERSEDED`` for the records of
the failed task only. Records of tasks that completed stay.

Replay publishes nothing, since the records are in the store already.
"""

from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar, cast

import temporalio.activity
from temporalio import workflow
from temporalio.bridge.proto.streams import OutputRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.contrib.streams._body import content_hash
from temporalio.contrib.streams._errors import StreamError, StreamUnsupportedError
from temporalio.contrib.streams._handles import activity_handle
from temporalio.contrib.streams._local_activity import _WorkflowInbound
from temporalio.contrib.streams._record import Cursor, LinkEvent, StreamLink
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import to_wire_link

__all__ = [
    "WorkflowStreamProducer",
    "stream_producer",
]

T = TypeVar("T")

# The same per-record limit the stream service keeps, checked in the Workflow
# so an oversized value fails at the call.
_MAX_RECORD_BYTES = 3 << 19


class _RunProducers:
    """One run's stream output, across all its producers."""

    def __init__(self) -> None:
        # FINISH is a statement about the topic, not about a producer object.
        self.finished: set[str] = set()

    def add(self, record: OutputRecord) -> None:
        """Add ``record`` to this activation's output.

        Replay adds nothing, since the Worker stored the record when the
        activation first ran. No limit on the activation's bytes is checked
        here, since replay can't check it the same way, and a caught error
        would then give other commands on replay.
        """
        if workflow.unsafe.is_replaying():
            return
        workflow._Runtime.current().workflow_stream_records().append(record)


# Held on the run's runtime, so the state goes with the run at eviction and is
# rebuilt in order on replay.
_STATE_ATTRIBUTE = "_temporal_stream_producers"


def _run_producers(*, create: bool) -> _RunProducers | None:
    runtime = workflow._Runtime.current()
    producers: _RunProducers | None = getattr(runtime, _STATE_ATTRIBUTE, None)
    if producers is None and create:
        if not _has_streams_interceptor(runtime):
            raise StreamUnsupportedError(
                "stream_producer needs the stream store's Worker interceptor; build "
                "the Worker from a client that carries the store, or add the store "
                "plugin to the Replayer"
            )
        producers = _RunProducers()
        setattr(runtime, _STATE_ATTRIBUTE, producers)
    return producers


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


class WorkflowStreamProducer(Generic[T]):
    """Appends to one topic of the running Workflow's own stream.

    Each record goes with the completion of the activation that appended
    it, and the Worker appends it before it gives that completion to Core.
    """

    def __init__(self, producers: _RunProducers, topic: str) -> None:
        """Prefer :func:`temporalio.contrib.streams.stream_producer`."""
        self._producers = producers
        self._topic = topic

    @property
    def topic(self) -> str:
        """The name of the topic this producer is bound to."""
        return self._topic

    def append(self, value: T) -> None:
        """Append ``value`` on this topic and return at once.

        The value is converted here, with the Workflow's payload converter. A
        :class:`temporalio.common.RawValue` passes through pre-encoded. The
        record is visible when the activation that appended it completes,
        which is before the Workflow Task completes. If the task then fails or
        times out, the record stays, and the task's rerun appends again under
        a higher attempt, which readers see as ``SUPERSEDED``.

        Raises:
            ValueError: The topic was already finished in this run.
            temporalio.contrib.streams.StreamError: The value's payload is
                over 1.5 MiB. Nothing is appended.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
        """
        _check_writable("append to")
        if self._topic in self._producers.finished:
            raise ValueError(f"topic {self._topic!r} was already finished")
        payload = workflow.payload_converter().to_payloads([value])[0]
        if payload.ByteSize() > _MAX_RECORD_BYTES:
            raise StreamError(
                f"a record of {payload.ByteSize()} bytes on {self._topic!r} is over "
                f"the per-record limit of {_MAX_RECORD_BYTES} bytes"
            )
        self._producers.add(
            OutputRecord(
                topic=self._topic,
                kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
                body=payload,
                content_hash=content_hash(payload),
            )
        )

    def link(
        self,
        ref: StreamRef,
        *,
        name: str,
        after: Cursor | None = None,
        value: Any = None,
    ) -> None:
        """Append a ``LINK`` that opens ``ref``'s stream on this topic.

        A follower reads the linked stream from strictly after ``after``, or
        from its oldest record, in one order with this stream. ``name`` names
        the link within this run, and :meth:`join` names it again. ``ref``
        is stored as plaintext. ``value``, when given, is converted as
        :meth:`append` converts it. The record goes with this activation, as
        :meth:`append` sends it.

        Raises:
            ValueError: The topic was already finished in this run,
                ``name`` is empty or over 256 bytes, or ``ref`` names a
                stream that no Workflow owns.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
        """
        self._add_link(StreamLink(LinkEvent.OPEN, ref, name, after=after), value)

    def join(self, *, name: str, until: Cursor | None = None) -> None:
        """Append a ``LINK`` that says this run waited for link ``name``.

        A follower holds this stream's later records until the linked stream
        reached ``until``, or its end when ``until`` is ``None``.

        Raises:
            ValueError: The topic was already finished in this run, or
                ``name`` is empty or over 256 bytes.
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
        """
        self._add_link(StreamLink(LinkEvent.JOIN, None, name, until=until), None)

    def _add_link(self, link: StreamLink, value: Any) -> None:
        _check_writable("link on")
        if self._topic in self._producers.finished:
            raise ValueError(f"topic {self._topic!r} was already finished")
        record = OutputRecord(
            topic=self._topic,
            kind=StreamRecordKind.STREAM_RECORD_KIND_LINK,
            link=to_wire_link(link),
        )
        if value is not None:
            payload = workflow.payload_converter().to_payloads([value])[0]
            if payload.ByteSize() > _MAX_RECORD_BYTES:
                raise StreamError(
                    f"a link value of {payload.ByteSize()} bytes on {self._topic!r} "
                    f"is over the per-record limit of {_MAX_RECORD_BYTES} bytes"
                )
            record.body.CopyFrom(payload)
            record.content_hash = content_hash(payload)
        self._producers.add(record)

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

        Says this run appends nothing more on the topic. The record goes
        with this activation, after what the run appended before. A run
        that follows by Continue-as-New can append on the topic again. It
        does not say the Workflow succeeded, and it does not end anyone's
        read.

        Raises:
            temporalio.workflow.ReadOnlyContextError: Called from a query or
                an update validator.
        """
        _check_writable("finish")
        if self._topic in self._producers.finished:
            return
        self._producers.add(
            OutputRecord(
                topic=self._topic, kind=StreamRecordKind.STREAM_RECORD_KIND_FINISH
            )
        )
        self._producers.finished.add(self._topic)


# The result's type depends on the calling context, which a static type can't name.
def stream_producer(
    topic: str | StreamTopic[Any] | None = None,
    scope: Literal["activity", "workflow"] | None = None,
) -> Any:
    """A producer on ``topic`` of the running Workflow's or Activity's stream.

    Call it from Workflow code or from Activity code. In a Workflow it
    returns a :class:`WorkflowStreamProducer`, whose ``append`` returns at
    once. All Workflow Producers on one topic of a run share one ``FINISH``.
    Appending adds no event to History. A store that fails the append fails
    the Workflow Task, which retries. A topic the store reports closed drops
    the activation's records on it, since no retry could land.

    In an Activity it returns ``activity_handle(scope).producer(topic=topic)``, a
    :class:`temporalio.contrib.streams.StreamProducer` whose ``append`` is
    awaited. See :meth:`temporalio.contrib.streams.ActivityStreamHandle.producer`.

    Args:
        topic: A :func:`temporalio.contrib.streams.topic` definition, whose
            value type the producer's ``append`` takes, or a plain string.
            Without one, the producer is on
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        scope: In an Activity, which stream it writes, as
            :func:`temporalio.contrib.streams.activity_handle` picks it. In a
            Workflow only ``"workflow"`` or ``None``.

    Raises:
        ValueError: ``topic`` is empty, or ``scope`` is not one the context
            has.
        RuntimeError: Called outside both Workflow code and an Activity.
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator.
        temporalio.contrib.streams.StreamUnsupportedError: The Worker has no
            stream store interceptor, or the Activity has no stream in
            ``scope``.
    """
    if not workflow.in_workflow():
        if not temporalio.activity.in_activity():
            raise RuntimeError("stream_producer needs Workflow code or an Activity")
        return activity_handle(scope).producer(topic=topic)
    # A caller without a type checker can pass any value.
    if cast(Any, scope) not in (None, "workflow"):
        raise ValueError(f"a Workflow writes only its own stream, not scope={scope!r}")
    _check_writable("open a producer on")
    name, _ = resolve_topic(topic)
    producers = _run_producers(create=True)
    assert producers is not None
    return WorkflowStreamProducer(producers, name)
