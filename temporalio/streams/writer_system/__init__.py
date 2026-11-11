# Written by hand in the shape of nexgen's client output, since the writer has no WIT. Keep it
# close to that output, so a later switch to generated code changes no caller.

from __future__ import annotations

from .models import (
    AppendRecord,
    CloseWriterRequest,
    CloseWriterResponse,
    CloseWritersRequest,
    CloseWritersResponse,
    FreeWriterRequest,
    FreeWriterResponse,
    OpenWriterRequest,
    Producer,
    StreamRef,
    WriteRecordsRequest,
    WriteRecordsResponse,
    WriterResult,
)
from .services import StreamWriter

__all__ = [
    "AppendRecord",
    "CloseWriterRequest",
    "CloseWriterResponse",
    "CloseWritersRequest",
    "CloseWritersResponse",
    "FreeWriterRequest",
    "FreeWriterResponse",
    "OpenWriterRequest",
    "Producer",
    "StreamRef",
    "StreamWriter",
    "WriteRecordsRequest",
    "WriteRecordsResponse",
    "WriterResult",
]
