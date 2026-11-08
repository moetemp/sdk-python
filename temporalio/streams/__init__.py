"""The Streams API, which Workflows, Activities and clients share.

A Stream is an ordered log of Records that Workflows, Activities and clients write, and that
clients and Activities read. A Stream has one payload type, and a call takes it as the
``payload_type`` argument, as in ``client.get_stream("chat-42", payload_type=Turn)``. With no
payload type, a read gives JSON values.

Code creates a Stream before its first append. :py:data:`temporalio.workflow.create_stream` and
:py:meth:`temporalio.client.Client.create_stream` fail with :py:class:`StreamAlreadyExistsError`
if the id exists. Pass ``id_conflict_policy=StreamIDConflictPolicy.USE_EXISTING` to get the
Stream instead. In Workflow code, each run of a chain sends the same create for an id, so a retry
run or a cron run gets the Stream that an earlier run made. Workflow code gets a
:py:class:`temporalio.workflow.WorkflowStream`, and other code a
:py:class:`temporalio.client.Stream`. A client gets any Stream by its id with
:py:meth:`temporalio.client.Client.get_stream`, with no call.

A Stream's :py:class:`StreamRef` is data with no calls. Code passes it as an argument to a
Workflow, an Activity, a child Workflow or the next run, or hands it out through a query.
:py:func:`temporalio.workflow.get_stream` and :py:meth:`temporalio.client.Client.get_stream` give
the Stream of a ref. In an Activity, a Stream from ``activity.client()`` writes as the attempt.

A read gives :py:class:`Record` and :py:class:`Superseded` items, and the ``kind`` of each item
narrows its type. A :py:class:`StreamOffset` is the opaque position of a Record. A close carries
no value. A read of a closed Stream ends once it gives the last item.

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
from temporalio.streams._conflict import StreamIDConflictPolicy
from temporalio.streams._errors import (
    StreamAlreadyExistsError,
    StreamClosedError,
    StreamConfigError,
    StreamError,
    StreamFencedError,
    StreamNotFoundError,
)
from temporalio.streams._items import END, End, ItemKind, Record, Superseded
from temporalio.streams._offset import StreamOffset
from temporalio.streams._plugin import StreamBackend
from temporalio.streams._reader import Reader
from temporalio.streams._ref import StreamRef
from temporalio.streams._stream_client import producer

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
    "Reader",
    "Record",
    "StreamAlreadyExistsError",
    "StreamBackend",
    "StreamClosedError",
    "StreamConfigError",
    "StreamError",
    "StreamErrorReason",
    "StreamFencedError",
    "StreamIDConflictPolicy",
    "StreamNotFoundError",
    "StreamOffset",
    "StreamRef",
    "StreamSuperseded",
    "StreamWriter",
    "Superseded",
    "Timestamp",
    "WorkflowTaskEpoch",
    "producer",
]
