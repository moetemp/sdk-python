"""The Streams API, which Workflows, Activities and clients share.

A Stream is an ordered log of Records that Workflows, Activities and clients write, and that
clients and Activities read. A Stream has one payload type, and a call takes it as a type
argument, as in ``client.get_stream[Turn]("chat-42")``. With no type argument, the payload type
is ``Any``, and a read gives JSON values.

Code creates a Stream before its first publish. :py:data:`temporalio.workflow.create_stream` and
:py:attr:`temporalio.client.Client.create_stream` fail with :py:class:`StreamAlreadyExistsError` if
the id exists, and ``create_or_get_stream`` gets the Stream instead. In Workflow code, each run of a
chain sends the same create for an id, so a retry run or a cron run gets the Stream that an earlier
run made. Workflow code gets a :py:class:`temporalio.workflow.WorkflowStream`, and other code a
:py:class:`temporalio.client.Stream`. A client gets any Stream by its id with
:py:attr:`temporalio.client.Client.get_stream`, with no call.

A Stream's :py:class:`StreamRef` is data with no calls. Code passes it as an argument to a
Workflow, an Activity, a child Workflow or the next run, or hands it out through a query.
:py:func:`temporalio.workflow.to_stream` and :py:meth:`temporalio.client.Client.to_stream` give
the Stream of a ref. In an Activity, a Stream from ``activity.client()`` writes as the attempt.

A read gives :py:class:`Record` and :py:class:`Superseded` items, and the ``kind`` of each item
narrows its type. A close carries no value. A read of a closed Stream ends once it gives the last
item.

A :py:class:`StreamBackend` makes the Stream calls and registers a backend name. Examples are
:py:class:`temporalio.contrib.stream.RedisStreamBackend` (``redis``) and
:py:class:`temporalio.contrib.stream.testing.MemoryStreamBackend` (``memory``). Register
backends with ``Client.connect(..., stream_backends=[...])``. A ref keeps the name of its
backend. With more than one registered backend, exactly one must have ``default=True``. A create
with no ``backend=`` uses that backend. A Worker serves only one backend.

.. warning::
    The Streams API is experimental.
"""

# The reason of a StreamError is the proto enum, as in the StreamErrorDetails of a failed call.
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.streams._errors import (
    StreamAlreadyExistsError,
    StreamClosedError,
    StreamConfigError,
    StreamError,
    StreamExpiredError,
    StreamFencedError,
    StreamNotFoundError,
    StreamNotOwnerError,
    StreamOffsetTooHighError,
    StreamSeqDivergentError,
    StreamStorageUnavailableError,
    StreamTooLargeError,
    StreamUnsupportedError,
)
from temporalio.streams._items import END, End, ItemKind, Record, Superseded
from temporalio.streams._plugin import StreamBackend
from temporalio.streams._reader import Subscription
from temporalio.streams._ref import StreamOptions, StreamRef

# nexgen generates these wire models. Refs, Records and Superseded items carry them.
from temporalio.streams.system.models import (
    ActivityEpoch,
    ClientEpoch,
    Execution,
    ExecutionType,
    LocalActivityEpoch,
    Producer,
    ProducerEpoch,
    ProducerEpochKind,
    ProducerEpochKindActivity,
    ProducerEpochKindClient,
    ProducerEpochKindLocalActivity,
    ProducerEpochKindStandaloneActivity,
    ProducerEpochKindWorkflow,
    StreamSuperseded,
    StreamWriter,
    Timestamp,
    WorkflowTaskEpoch,
)

__all__ = [
    "END",
    "ActivityEpoch",
    "ClientEpoch",
    "End",
    "Execution",
    "ExecutionType",
    "ItemKind",
    "LocalActivityEpoch",
    "Producer",
    "ProducerEpoch",
    "ProducerEpochKind",
    "ProducerEpochKindActivity",
    "ProducerEpochKindClient",
    "ProducerEpochKindLocalActivity",
    "ProducerEpochKindStandaloneActivity",
    "ProducerEpochKindWorkflow",
    "Record",
    "StreamAlreadyExistsError",
    "StreamBackend",
    "StreamClosedError",
    "StreamConfigError",
    "StreamError",
    "StreamErrorReason",
    "StreamExpiredError",
    "StreamFencedError",
    "StreamNotFoundError",
    "StreamNotOwnerError",
    "StreamOffsetTooHighError",
    "StreamOptions",
    "StreamRef",
    "StreamSeqDivergentError",
    "StreamStorageUnavailableError",
    "StreamSuperseded",
    "StreamTooLargeError",
    "StreamUnsupportedError",
    "StreamWriter",
    "Subscription",
    "Superseded",
    "Timestamp",
    "WorkflowTaskEpoch",
]
