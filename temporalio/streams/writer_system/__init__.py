# Written by hand in the shape of nexgen's client output, since the writer has no WIT. Keep it
# close to that output, so a later switch to generated code changes no caller.

from __future__ import annotations

from .models import (
    ActivityEpoch,
    AppendRecord,
    ClientEpoch,
    CloseWriterRequest,
    CloseWriterResponse,
    CloseWritersRequest,
    CloseWritersResponse,
    Execution,
    ExecutionType,
    FreeWriterRequest,
    FreeWriterResponse,
    LocalActivityEpoch,
    OpenWriterRequest,
    Producer,
    ProducerEpoch,
    ProducerEpochKind,
    ProducerEpochKindActivity,
    ProducerEpochKindClient,
    ProducerEpochKindLocalActivity,
    ProducerEpochKindStandaloneActivity,
    ProducerEpochKindWorkflow,
    StreamRef,
    Timestamp,
    WorkflowTaskEpoch,
    WriteRecordsRequest,
    WriteRecordsResponse,
    WriterResult,
)

# The package's StreamWriter is the service. The writer record of the same name stays in models.
from .services import StreamWriter

__all__ = [
    "ActivityEpoch",
    "AppendRecord",
    "ClientEpoch",
    "CloseWriterRequest",
    "CloseWriterResponse",
    "CloseWritersRequest",
    "CloseWritersResponse",
    "Execution",
    "ExecutionType",
    "FreeWriterRequest",
    "FreeWriterResponse",
    "LocalActivityEpoch",
    "OpenWriterRequest",
    "Producer",
    "ProducerEpoch",
    "ProducerEpochKind",
    "ProducerEpochKindActivity",
    "ProducerEpochKindClient",
    "ProducerEpochKindLocalActivity",
    "ProducerEpochKindStandaloneActivity",
    "ProducerEpochKindWorkflow",
    "StreamRef",
    "Timestamp",
    "WorkflowTaskEpoch",
    "WriteRecordsRequest",
    "WriteRecordsResponse",
    "WriterResult",
    "StreamWriter",
]
