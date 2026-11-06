# Written by hand in the shape of nexgen's client output, since the writer has no WIT. Keep it
# close to that output, so a later switch to generated code changes no caller.

from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import typing
import uuid as _uuid

import temporalio.client

from ...models import (
    AppendRecord,
    CloseWriterRequest,
    CloseWriterResponse,
    FreeWriterRequest,
    FreeWriterResponse,
    WriteRecordsRequest,
    WriteRecordsResponse,
)
from ...services import StreamWriter


@dataclasses.dataclass(init=False)
class Writer:
    """An open writer of one Producer. The crate keeps its epoch and sequence."""

    writer_id: int

    def __init__(
        self,
        nexus_client: temporalio.client.NexusClient[typing.Any],
        /,
        *,
        writer_id: int,
    ) -> None:
        self._nexus_client: temporalio.client.NexusClient[typing.Any] = nexus_client
        self.writer_id = writer_id

    async def write_records(
        self,
        records: list[AppendRecord] | None = None,
        *,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> WriteRecordsResponse:
        """Write Records at once. The answer has the first and the last offset."""
        request = WriteRecordsRequest(
            writer_id=self.writer_id,
            records=records or [],
        )
        handle = await self._nexus_client.start_operation(
            operation=StreamWriter.write_records,
            arg=request,
            id=f"{_uuid.uuid4()}",
            rpc_metadata=rpc_metadata or {},
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()

    async def close(
        self,
        *,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> CloseWriterResponse:
        """Release the handle. The last one waits for a write, then drops the writer."""
        request = CloseWriterRequest(
            writer_id=self.writer_id,
        )
        handle = await self._nexus_client.start_operation(
            operation=StreamWriter.close_writer,
            arg=request,
            id=f"{_uuid.uuid4()}",
            rpc_metadata=rpc_metadata or {},
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()

    async def free(
        self,
        *,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> FreeWriterResponse:
        """Release the handle with no wait, for a handle that Python frees."""
        request = FreeWriterRequest(
            writer_id=self.writer_id,
        )
        handle = await self._nexus_client.start_operation(
            operation=StreamWriter.free_writer,
            arg=request,
            id=f"{_uuid.uuid4()}",
            rpc_metadata=rpc_metadata or {},
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()


__all__ = ["Writer"]
