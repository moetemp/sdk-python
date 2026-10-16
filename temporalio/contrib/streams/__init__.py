"""Streams: an ordered log a Workflow or an Activity owns, written from many places.

.. warning::
    This package is experimental and may change in future versions.

A Workflow or an Activity owns a stream. Producers outside
Workflow code, such as the
Workflow's Activities and clients, append to it, and outside readers consume
it from a cursor. Records live in a store the application runs, which
Core reaches for every SDK. Record bodies never reach the server.

The contract:

1. **Producers write with an identity.** A producer has an id, an attempt
   and a sequence that starts at one for each attempt. A retry of the
   newest batch with the same content is written once and returns the
   original position. A retry with different content, or with a lower
   sequence, is refused with :class:`StreamProducerError`. Content is
   compared over the payloads the converter produced, before the codec, so
   a codec with a fresh nonce per call still deduplicates a retry.
2. **A new attempt supersedes the old one.** When a reader sees the first
   record of a producer's newer attempt, it yields a
   :attr:`RecordKind.SUPERSEDED` record first. It withdraws only what that
   producer wrote in its earlier attempts, not the whole stream and not the
   records of other producers. No store holds that record, so every store
   reports a retry the same way. :class:`SupersessionTracker` names the
   records each one withdraws.
3. **A cursor belongs to one stream.** Hand it back to resume strictly after
   the record it names. A cursor from another store or another stream is
   refused with :class:`StreamCursorError`, and one whose record
   retention dropped with :class:`StreamExpiredError`. A read with no cursor
   starts at :data:`BEGINNING` or :data:`END`.
4. **Topics are defined once.** :func:`topic` defines a topic with the type
   its records decode to, and every party shares that definition. A call
   that names no topic addresses :data:`DEFAULT_TOPIC`.

A store is configured as a plugin:
``Client.connect(..., plugins=[RedisStreams(...)])`` registers it on the
client and on every Worker built from that client. Each
context then reaches a stream the same way:

- Workflow code publishes to its own stream with :func:`workflow_writer`.
  Core appends each activation's records when the activation completes,
  and nothing goes to History.
- An Activity reaches the stream of the Workflow that scheduled it with
  :func:`activity_handle`, and writes as itself: its Activity id with the
  scheduling run id, and an attempt from its start time and its Temporal
  attempt. A Local Activity writes as its call number in the run, with an
  attempt that grows across its reruns. In a standalone Activity,
  :func:`activity_handle` is the Activity's own stream, and the Activity
  writes as its id with its run id. An Activity that Workflow code started
  with :func:`start_activity_with_stream` owns a stream too, which
  :func:`activity_handle` gives it, and which closes when the Workflow sees
  the Activity end.
- Any process holding a client reaches a stream with
  :func:`get_stream_handle`, and writes with a producer id and an attempt of
  its own.

Every read of a Workflow's stream follows the Workflow's run chain across
Continue-as-New and ends when the chain closes. A reader can't pin one run.
A read of a standalone Activity's stream ends when the Activity run ends. A
read of the stream of an Activity that a Workflow scheduled ends when the
scheduling run sees the Activity end.

Workflow code reads a stream with :func:`workflow_reader`, one Local
Activity per batch, and sleeps on the stream's notifier while the stream is
idle. Only a Workflow or an Activity owns a stream. The other owner kinds
raise :class:`StreamUnsupportedError`.

The stored record is ``temporal.sdk.streams.v1.StreamRecord``, in
:mod:`temporalio.bridge.proto.streams.v1`, with the user's value in ``body``
as an ordinary payload.
"""

from temporalio.contrib.streams._cursor import BEGINNING, END
from temporalio.contrib.streams._driver import StreamDriver
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamOutcomeUnknownError,
    StreamProducerError,
    StreamRecordError,
    StreamRefusedError,
    StreamReplayMismatchError,
    StreamReplayRangeLostError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._follow import (
    FollowedItem,
    FollowPosition,
    LinkState,
    LinkStateKind,
)
from temporalio.contrib.streams._handles import (
    ActivityStreamHandle,
    StreamHandle,
    StreamProducer,
    activity_handle,
    get_stream_handle,
)
from temporalio.contrib.streams._plugin import StreamStorePlugin
from temporalio.contrib.streams._record import (
    Cursor,
    LinkEvent,
    RecordKind,
    StreamLink,
    StreamRecord,
    Supersession,
)
from temporalio.contrib.streams._ref import StreamOwnerKind, StreamRef
from temporalio.contrib.streams._supersession import (
    SupersessionTracker,
    track_supersession,
)
from temporalio.contrib.streams._topic import DEFAULT_TOPIC, StreamTopic, topic
from temporalio.contrib.streams._workflow import (
    StreamBatch,
    StreamPosition,
    TaskCutoff,
    WorkflowStreamReader,
    start_activity_with_stream,
    workflow_reader,
)
from temporalio.contrib.streams._workflow_writer import (
    WorkflowStreamWriter,
    close_workflow_stream,
    workflow_writer,
)

__all__ = [
    "ActivityStreamHandle",
    "BEGINNING",
    "Cursor",
    "DEFAULT_TOPIC",
    "END",
    "FollowPosition",
    "FollowedItem",
    "LinkEvent",
    "LinkState",
    "LinkStateKind",
    "RecordKind",
    "StreamBatch",
    "StreamClosedError",
    "StreamCursorError",
    "StreamDriver",
    "StreamError",
    "StreamExpiredError",
    "StreamHandle",
    "StreamLink",
    "StreamNotFoundError",
    "StreamOutcomeUnknownError",
    "StreamOwnerKind",
    "StreamPosition",
    "StreamProducer",
    "StreamProducerError",
    "StreamRecord",
    "StreamRecordError",
    "StreamRef",
    "StreamRefusedError",
    "StreamReplayMismatchError",
    "StreamReplayRangeLostError",
    "StreamStorageError",
    "StreamStorePlugin",
    "StreamTopic",
    "StreamUnsupportedError",
    "Supersession",
    "SupersessionTracker",
    "TaskCutoff",
    "WorkflowStreamReader",
    "WorkflowStreamWriter",
    "activity_handle",
    "close_workflow_stream",
    "get_stream_handle",
    "start_activity_with_stream",
    "topic",
    "track_supersession",
    "workflow_reader",
    "workflow_writer",
]
