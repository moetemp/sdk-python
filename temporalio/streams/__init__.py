"""The Streams API, which Workflows, Activities and clients share.

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
    "Superseded",
    "Timestamp",
    "WorkflowTaskEpoch",
]
