"""The Streams API, which Workflows, Activities and clients share.

A Stream is an ordered log of Records that Workflows, Activities and clients write, and that
clients and Activities read. A Stream has one payload type, and a call takes it as the
``payload_type`` argument, as in ``client.get_stream("chat-42", payload_type=Turn)``. With no
payload type, a read gives JSON values.

Code creates a Stream before its first append. :py:data:`temporalio.workflow.create_stream` and
:py:meth:`temporalio.client.Client.create_stream` fail with a :py:class:`StreamError` whose
``reason`` is ``STREAM_ERROR_REASON_ALREADY_EXISTS`` if the id exists. Pass ``id_conflict_policy=StreamIDConflictPolicy.USE_EXISTING` to get the
Stream instead. In Workflow code, each run of a chain sends the same create for an id, so a retry
run or a cron run gets the Stream that an earlier run made. Workflow code gets a
:py:class:`temporalio.workflow.WorkflowStream`, and other code a
:py:class:`temporalio.client.Stream`. A client gets any Stream by its id with
:py:meth:`temporalio.client.Client.get_stream`, with no call.

A Stream's :py:class:`StreamRef` is data with no calls. Code passes it as an argument to a
Workflow, an Activity, a child Workflow or the next run, or hands it out through a query.
:py:func:`temporalio.workflow.get_stream` and :py:meth:`temporalio.client.Client.get_stream` give
the Stream of a ref. In an Activity, a Stream from ``activity.client()`` writes as the attempt.

A read gives :py:class:`Record` items and a :py:class:`Closed` item last, and the ``kind`` of each
item narrows its type. A :py:class:`StreamOffset` is the opaque position of a Record. For
supersession, the opt-in :py:func:`temporalio.streams.superseded.with_superseded` wraps a reader
and adds ``Superseded`` items from the Producer epochs. A read of a closed Stream ends after its
``Closed`` item.

A :py:class:`StreamBackend` makes the Stream calls and registers a backend name. Examples are
:py:class:`temporalio.contrib.stream.RedisStreamBackend` (``redis``) and
:py:class:`temporalio.contrib.stream.testing.MemoryStreamBackend` (``memory``). Register
backends in a :py:class:`StreamConfig`, as ``ExternalStorage`` registers its drivers::

    client = await Client.connect(
        "localhost:7233",
        stream_config=StreamConfig(backends=[RedisStreamBackend("redis://localhost")]),
    )

With one backend, every Stream goes to it. With more than one, the config's
``backend_selector`` picks the backend of each create and each get by id. A ref keeps the name
of its backend, so a call through a ref finds its backend by name. A Worker serves only one
backend.

.. warning::
    The Streams API is experimental.
"""

# The reason of a StreamError is the proto enum, as in the StreamErrorDetails of a failed call.
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.streams._close import StreamCloseStatus
from temporalio.streams._conflict import StreamIDConflictPolicy
from temporalio.streams._errors import (
    StreamConfigError,
    StreamError,
)
from temporalio.streams._items import END, Closed, End, ItemKind, Record
from temporalio.streams._offset import StreamOffset
from temporalio.streams._plugin import (
    StreamBackend,
    StreamConfig,
    StreamSelectContext,
)
from temporalio.streams._reader import Reader
from temporalio.streams._ref import StreamRef
from temporalio.streams._stream_client import producer

# nexgen generates these wire models. Refs, Records and close items carry them.
from temporalio.streams.system.models import (
    ActivityEpoch,
    ActivityProducer,
    ClientEpoch,
    ClientProducer,
    LocalActivityEpoch,
    LocalActivityProducer,
    Producer,
    ProducerKind,
    ProducerKindClient,
    ProducerKindLocalActivity,
    ProducerKindStandaloneActivity,
    ProducerKindWorkflow,
    ProducerKindWorkflowActivity,
    StreamClose,
    StreamWriter,
    Timestamp,
    WorkflowProducer,
    WorkflowTaskEpoch,
)

__all__ = [
    "END",
    "ActivityEpoch",
    "ActivityProducer",
    "ClientEpoch",
    "ClientProducer",
    "Closed",
    "End",
    "ItemKind",
    "LocalActivityEpoch",
    "LocalActivityProducer",
    "Producer",
    "ProducerKind",
    "ProducerKindClient",
    "ProducerKindLocalActivity",
    "ProducerKindStandaloneActivity",
    "ProducerKindWorkflow",
    "ProducerKindWorkflowActivity",
    "Reader",
    "Record",
    "StreamBackend",
    "StreamClose",
    "StreamCloseStatus",
    "StreamConfig",
    "StreamConfigError",
    "StreamError",
    "StreamErrorReason",
    "StreamIDConflictPolicy",
    "StreamOffset",
    "StreamRef",
    "StreamSelectContext",
    "StreamWriter",
    "Timestamp",
    "WorkflowProducer",
    "WorkflowTaskEpoch",
    "producer",
]
