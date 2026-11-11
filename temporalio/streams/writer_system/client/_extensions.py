# Written by hand in the shape of nexgen's client output, since the writer has no WIT. Keep it
# close to that output, so a later switch to generated code changes no caller.

from __future__ import annotations

import abc
import collections.abc
import datetime
import typing

from .. import models
from ..models import (
    CloseWriterResponse,
    OpenWriterRequest,
    StreamRef,
    WriteRecordsRequest,
    WriteRecordsResponse,
)
from ._resources.writer import Writer


class WriterCalls(typing.Protocol):
    """The runtime's calls of a writer, which encode the models with the user's converters."""

    async def open_writer(
        self,
        request: OpenWriterRequest,
        *,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> typing.Any:
        """Open a writer, and return the runtime's writer object."""
        ...

    async def write_records(
        self,
        writer: typing.Any,
        request: WriteRecordsRequest,
        result_type: type[WriteRecordsResponse],
        *,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> WriteRecordsResponse:
        """Write Records with a writer object."""
        ...

    async def close_writer(
        self,
        writer: typing.Any,
        result_type: type[CloseWriterResponse],
        *,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> CloseWriterResponse:
        """Close a writer object."""
        ...


class StreamWriterClientExtension(abc.ABC):
    """Opens the writers of the runtime from code outside a Workflow.

    A client class inherits this class and provides the runtime's ``writer_calls``.

    .. warning::
        This API is experimental and subject to change.
    """

    @abc.abstractmethod
    def writer_calls(self) -> WriterCalls:
        """Return the runtime's calls of a writer."""
        ...

    async def open_writer(
        self,
        *,
        ref: StreamRef,
        producer: models.Producer,
        max_batch_size: int | None = None,
        max_retry_duration: datetime.timedelta | None = None,
        rpc_metadata: collections.abc.Mapping[str, str | bytes] | None = None,
        rpc_timeout: datetime.timedelta | None = None,
    ) -> Writer:
        """
        Args:
            producer: The Producer of each batch, which sends it as AppendRecords does.
            max_batch_size: A write sends at most this many Records in one batch. Zero,
                or a value above 1000, is 1000.
            max_retry_duration: How long a write retries a Store failure. Empty is 10 s.
        """
        # The runtime makes no RPC, so it has no use for the metadata.
        del rpc_metadata
        calls = self.writer_calls()
        request = OpenWriterRequest(
            ref=ref,
            producer=producer,
            max_batch_size=max_batch_size,
            max_retry_duration=max_retry_duration,
        )
        return Writer(calls, await calls.open_writer(request, rpc_timeout=rpc_timeout))
