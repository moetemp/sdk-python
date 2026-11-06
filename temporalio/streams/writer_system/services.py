# Written by hand in the shape of nexgen's client output, since the writer has no WIT. Keep it
# close to that output, so a later switch to generated code changes no caller.

from __future__ import annotations

from nexusrpc import Operation, service

from .models import (
    CloseWriterRequest,
    CloseWriterResponse,
    CloseWritersRequest,
    CloseWritersResponse,
    FreeWriterRequest,
    FreeWriterResponse,
    OpenWriterRequest,
    WriteRecordsRequest,
    WriteRecordsResponse,
    WriterResult,
)


@service(name="coresdk.streams.StreamWriter")
class StreamWriter:
    """
    .. warning::
        This API is experimental and subject to change.
    """

    open_writer: Operation[
        OpenWriterRequest,
        WriterResult,
    ] = Operation(name="OpenWriter")

    write_records: Operation[
        WriteRecordsRequest,
        WriteRecordsResponse,
    ] = Operation(name="WriteRecords")

    close_writer: Operation[
        CloseWriterRequest,
        CloseWriterResponse,
    ] = Operation(name="CloseWriter")

    free_writer: Operation[
        FreeWriterRequest,
        FreeWriterResponse,
    ] = Operation(name="FreeWriter")

    close_writers: Operation[
        CloseWritersRequest,
        CloseWritersResponse,
    ] = Operation(name="CloseWriters")
