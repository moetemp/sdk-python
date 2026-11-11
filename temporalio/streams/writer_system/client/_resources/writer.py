# Written by hand in the shape of nexgen's client output, since the writer has no WIT. Keep it
# close to that output, so a later switch to generated code changes no caller.

from __future__ import annotations

import collections.abc
import datetime
import typing

from ...models import (
    AppendRecord,
    CloseWriterResponse,
    WriteRecordsRequest,
    WriteRecordsResponse,
)

if typing.TYPE_CHECKING:
    from .._extensions import WriterCalls


class Writer:
    """An open writer of one Producer. The runtime's writer object keeps its epoch and sequence.

    Dropping the last reference releases the runtime's writer.
    """

    def __init__(self, calls: WriterCalls, writer: typing.Any, /) -> None:
        self._calls = calls
        self._writer = writer

    async def write_records(
        self,
        records: list[AppendRecord] | None = None,
        *,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> WriteRecordsResponse:
        """Write Records at once. The answer has the first and the last offset."""
        del rpc_metadata
        return await self._calls.write_records(
            self._writer,
            WriteRecordsRequest(records=records or []),
            WriteRecordsResponse,
            rpc_timeout=rpc_timeout,
        )

    async def close(
        self,
        *,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> CloseWriterResponse:
        """Close the writer. It waits for a write in flight, and a later write fails as closed."""
        del rpc_metadata
        return await self._calls.close_writer(
            self._writer, CloseWriterResponse, rpc_timeout=rpc_timeout
        )


__all__ = ["Writer"]
