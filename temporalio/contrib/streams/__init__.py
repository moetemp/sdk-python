"""Streams of Records that Workflows, Activities and clients write and read.

A Stream definition, a class with ``@streams.defn``, gives the Stream name and the type of each
Topic. Workflow code creates a Stream with
:py:func:`temporalio.contrib.streams.workflow.create_stream`. A client creates and gets Streams
with :py:class:`StreamClient`. A Stream is data: a Workflow passes it to an Activity, a child
Workflow or the next run as an argument, and the parameter's type gives the handle class of
that context, such as :py:class:`ActivityStream`.

A Stream plugin carries the calls and registers a backend name with ``name=``, such as
:py:class:`temporalio.contrib.streams.redis.RedisStreamPlugin` (``redis``) and
:py:class:`temporalio.contrib.streams.testing.MemoryStreamPlugin` (``memory``). A handle
carries the name of its backend. A client may have two plugins, and ``default=True`` marks the
one that a create with no ``backend=`` uses. For now a Worker has one Stream plugin, so it
serves one backend.

.. warning::
    This package is experimental.
"""

from temporalio.contrib.streams import workflow
from temporalio.contrib.streams._activity_stream import (
    ActivityStream,
    ActivityTopic,
    SyncActivityStream,
    SyncActivityTopic,
)
from temporalio.contrib.streams._buffered import BufferedPublisher
from temporalio.contrib.streams._defn import (
    StreamDefinition,
    TopicDefinition,
    defn,
    topic,
)
from temporalio.contrib.streams._errors import (
    ReadOnlyContextError,
    StreamClosedError,
    StreamConfigError,
    StreamError,
    StreamExpiredError,
    StreamFencedError,
    StreamNotFoundError,
    StreamOffsetError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._items import END, Closed, Record, Superseded
from temporalio.contrib.streams._plugin import StreamPlugin
from temporalio.contrib.streams._stream_client import (
    ClientStream,
    ClientTopic,
    StreamClient,
)
from temporalio.contrib.streams.workflow import WorkflowStream, WorkflowTopic

__all__ = [
    "END",
    "ActivityStream",
    "ActivityTopic",
    "BufferedPublisher",
    "ClientStream",
    "ClientTopic",
    "Closed",
    "ReadOnlyContextError",
    "Record",
    "StreamClient",
    "StreamClosedError",
    "StreamConfigError",
    "StreamDefinition",
    "StreamError",
    "StreamExpiredError",
    "StreamFencedError",
    "StreamHandle",
    "StreamNotFoundError",
    "StreamOffsetError",
    "StreamPlugin",
    "StreamStorageError",
    "StreamUnsupportedError",
    "Superseded",
    "SyncActivityStream",
    "SyncActivityTopic",
    "TopicDefinition",
    "WorkflowStream",
    "WorkflowTopic",
    "defn",
    "topic",
    "workflow",
]
