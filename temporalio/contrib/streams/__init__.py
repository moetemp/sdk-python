"""Streams of Records that Workflows, Activities and clients write and read.

Workflow code writes with :py:func:`workflow_stream` and :py:func:`create_workflow_stream`.
Activities use :py:func:`activity_stream`. Clients read with :py:func:`for_handle` or :py:func:`for_ref` and create
standalone Streams with :py:func:`create_stream`. A Stream plugin, such as
:py:class:`temporalio.contrib.streams.redis.RedisStreamPlugin`, carries the calls.

.. warning::
    This package is experimental.
"""

from temporalio.contrib.streams._activity import activity_stream
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams._client import (
    ClientStream,
    create_stream,
    for_handle,
    for_ref,
)
from temporalio.contrib.streams._errors import (
    ReadOnlyContextError,
    StreamBufferFullError,
    StreamClosedError,
    StreamError,
    StreamExpiredError,
    StreamFencedError,
    StreamNotFoundError,
    StreamOffsetError,
    StreamStorageError,
    StreamTransportMissingError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._items import Closed, Idle, Record, Superseded
from temporalio.contrib.streams._outside import OutsideStream
from temporalio.contrib.streams._stream import StreamRef, Topic
from temporalio.contrib.streams._workflow import (
    WorkflowStream,
    create_workflow_stream,
    workflow_stream,
)

__all__ = [
    "ClientStream",
    "Closed",
    "Idle",
    "OutsideStream",
    "ReadOnlyContextError",
    "Record",
    "StreamBackend",
    "StreamBufferFullError",
    "StreamClosedError",
    "StreamError",
    "StreamExpiredError",
    "StreamFencedError",
    "StreamNotFoundError",
    "StreamOffsetError",
    "StreamRef",
    "StreamStorageError",
    "StreamTransportMissingError",
    "StreamUnsupportedError",
    "Superseded",
    "Topic",
    "WorkflowStream",
    "activity_stream",
    "create_stream",
    "create_workflow_stream",
    "for_handle",
    "for_ref",
    "workflow_stream",
]
