"""Streams of Records that Workflows, Activities and clients write and read.

A Stream definition, a class with ``@streams.defn``, gives the Stream name and the type of each
Topic. Workflow code creates a Stream with :py:func:`temporalio.workflow.create_stream`. A
client gets a Workflow's Stream with :py:meth:`temporalio.client.WorkflowHandle.stream`, and
any Stream with :py:meth:`temporalio.client.Client.stream`. A Stream is data: a Workflow
passes it to an Activity, a child Workflow or the next run as an argument, and the
parameter's type gives the handle class of that context, such as :py:class:`ActivityStream`.

A :py:class:`StreamBackend` carries the calls and registers a backend name, such as
:py:class:`NativeStreamBackend` (``native``), whose Streams the Temporal server keeps,
:py:class:`temporalio.contrib.stream.RedisStreamBackend` (``redis``) and
:py:class:`temporalio.contrib.stream.testing.MemoryStreamBackend` (``memory``). Register
backends with ``Client.connect(..., stream_backends=[...])``. A handle carries the name of its
backend. With more than one registered backend, exactly one has ``default=True``, the one
that a create with no ``backend=`` uses. A Worker serves every backend of its client.

.. warning::
    This package is experimental.
"""

from temporalio.common.streams._activity_stream import (
    ActivityStream,
    ActivityTopic,
    SyncActivityStream,
    SyncActivityTopic,
    SyncSubscription,
)
from temporalio.common.streams._buffered import BufferedPublisher
from temporalio.common.streams._defn import (
    StreamDefinition,
    TopicDefinition,
    defn,
    topic,
    with_name,
)
from temporalio.common.streams._errors import (
    ReadOnlyContextError,
    StreamClosedError,
    StreamConfigError,
    StreamError,
    StreamExpiredError,
    StreamFencedError,
    StreamNotFoundError,
    StreamNotOwnerError,
    StreamOffsetError,
    StreamSeqDivergentError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.common.streams._handle import Creating, StreamHandle
from temporalio.common.streams._items import (
    END,
    Closed,
    CloseStatus,
    Record,
    Superseded,
)
from temporalio.common.streams._native import NativeStreamBackend
from temporalio.common.streams._plugin import StreamBackend
from temporalio.common.streams._reader import Subscription
from temporalio.common.streams._stream_client import ClientStream, ClientTopic
from temporalio.common.streams._workflow import WorkflowStream, WorkflowTopic
from temporalio.common.streams._workflow_reader import WorkflowItems

__all__ = [
    "END",
    "ActivityStream",
    "ActivityTopic",
    "BufferedPublisher",
    "ClientStream",
    "ClientTopic",
    "CloseStatus",
    "Closed",
    "Creating",
    "NativeStreamBackend",
    "ReadOnlyContextError",
    "Record",
    "StreamClosedError",
    "StreamConfigError",
    "StreamDefinition",
    "StreamBackend",
    "StreamError",
    "StreamExpiredError",
    "StreamFencedError",
    "StreamHandle",
    "StreamNotFoundError",
    "StreamNotOwnerError",
    "StreamOffsetError",
    "StreamSeqDivergentError",
    "StreamStorageError",
    "StreamUnsupportedError",
    "Subscription",
    "Superseded",
    "SyncActivityStream",
    "SyncActivityTopic",
    "SyncSubscription",
    "TopicDefinition",
    "WorkflowItems",
    "WorkflowStream",
    "WorkflowTopic",
    "defn",
    "topic",
    "with_name",
]
