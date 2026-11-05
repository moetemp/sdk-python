# Written by hand in the shape of nexgen's client output, since the writer has no WIT. Keep it
# close to that output, so a later switch to generated code changes no caller.

from __future__ import annotations

import abc
import collections.abc
import datetime
import typing
import uuid as _uuid

import temporalio.client

from .. import models
from ..models import (
    CloseWritersRequest,
    CloseWritersResponse,
    OpenWriterRequest,
    StreamRef,
)
from ..services import StreamWriter
from ._resources.writer import Writer

_NexusServiceT = typing.TypeVar("_NexusServiceT")


# The runtime serves the writer in process, so its calls use an empty endpoint.
class StreamWriterClientExtension(abc.ABC):
    """Starts the operations of ``StreamWriter`` from code outside a Workflow.

    A client class inherits this class and provides the SDK's ``create_nexus_client``
    method.

    .. warning::
        This API is experimental and subject to change.
    """

    @abc.abstractmethod
    def create_nexus_client(
        self,
        service: type[_NexusServiceT] | str,
        endpoint: str,
    ) -> temporalio.client.NexusClient[_NexusServiceT]:
        """Return the Nexus client that starts the operations of ``service``."""
        ...

    def get_writer(
        self,
        *,
        writer_id: int,
    ) -> Writer:
        """A handle on an existing ``Writer``, with no call."""
        return Writer(
            self.create_nexus_client(
                service=StreamWriter,
                endpoint="",
            ),
            writer_id=writer_id,
        )

    async def _stream_writer_open_writer(
        self,
        request: OpenWriterRequest,
        *,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> Writer:
        nexus_client = self.create_nexus_client(
            service=StreamWriter,
            endpoint="",
        )
        handle = await nexus_client.start_operation(
            operation=StreamWriter.open_writer,
            arg=request,
            id=f"{_uuid.uuid4()}",
            rpc_metadata=rpc_metadata or {},
            rpc_timeout=rpc_timeout,
        )
        result = await handle.result()
        if result.writer_id is None:
            raise RuntimeError("OpenWriter answered without `writer_id`")
        resource = Writer(
            nexus_client,
            writer_id=result.writer_id,
        )
        return resource

    async def open_writer(
        self,
        *,
        ref: StreamRef,
        writer: models.StreamWriter | None = None,
        max_batch_size: int | None = None,
        max_retry_duration: datetime.timedelta | None = None,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> Writer:
        """
        Args:
            writer: Each batch sends it as AppendRecords does. For a Local Activity it has
                the run's task facts, so the writes withdraw the run's orphaned Local
                Activity Records. Its withdraw_run must be empty, since only a retry run's
                first append withdraws a run, and a writer sends many.
            max_batch_size: A write sends at most this many Records in one batch. Zero,
                or a value above 1000, is 1000.
            max_retry_duration: How long a write retries a Store failure. Empty is 10 s.
        """
        request = OpenWriterRequest(
            ref=ref,
            writer=writer,
            max_batch_size=max_batch_size,
            max_retry_duration=max_retry_duration,
        )
        return await self._stream_writer_open_writer(
            request, rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
        )

    async def _stream_writer_close_writers(
        self,
        request: CloseWritersRequest,
        *,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> CloseWritersResponse:
        nexus_client = self.create_nexus_client(
            service=StreamWriter,
            endpoint="",
        )
        handle = await nexus_client.start_operation(
            operation=StreamWriter.close_writers,
            arg=request,
            id=f"{_uuid.uuid4()}",
            rpc_metadata=rpc_metadata or {},
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()

    async def close_writers(
        self,
        *,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> CloseWritersResponse:
        """Drop every open writer, for example at shutdown."""
        request = CloseWritersRequest()
        return await self._stream_writer_close_writers(
            request, rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
        )
