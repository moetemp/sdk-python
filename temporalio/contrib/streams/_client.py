"""Streams in client code."""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
from temporalio.contrib.streams import _convert, _errors, _ids
from temporalio.contrib.streams._backend import StreamBackend, backend_of_client
from temporalio.contrib.streams._outside import OutsideStream, batch_options
from temporalio.contrib.streams._writer import BatchOptions, StreamWriter

# A client's Producer id has a key per process, so two processes with one identity don't fence
# each other.
_PROCESS_KEY = str(uuid.uuid4())


def _client_producer(client: temporalio.client.Client) -> stream_pb.Producer:
    return stream_pb.Producer(
        kind=stream_pb.ProducerKind.PRODUCER_KIND_CLIENT,
        id=_ids.client_producer_id(client.identity, _PROCESS_KEY),
        epoch=stream_pb.ProducerEpoch(attempt=1),
    )


class ClientStream(OutsideStream):
    """A Stream that a client reads, writes and closes.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        client: temporalio.client.Client,
        backend: StreamBackend,
        stream_id: str,
        batch: BatchOptions,
        *,
        wait_for_create: timedelta | None = None,
        handle: temporalio.client.WorkflowHandle[Any, Any] | None = None,
    ) -> None:
        """Create a Stream handle. Use :py:func:`for_handle` or :py:func:`create_stream`."""
        self._client = client
        self._handle = handle
        converter = client.data_converter
        super().__init__(
            backend=backend,
            converter=converter,
            namespace=client.namespace,
            stream_id=stream_id,
            writer=lambda: StreamWriter(
                backend=backend,
                converter=converter,
                namespace=client.namespace,
                stream_id=stream_id,
                producer=_client_producer(client),
                batch=batch,
                create_first=False,
            ),
            wait_for_create=wait_for_create,
            run_ended=self._handle_closed if handle is not None else None,
        )

    async def _handle_closed(self) -> bool:
        assert self._handle is not None
        description = await self._handle.describe()
        return (
            description.status is not None
            and description.status != temporalio.client.WorkflowExecutionStatus.RUNNING
        )

    async def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Flush this client's Records and close the Stream. Only its creator closes it."""
        if self._writer is not None:
            await self._writer.close()
        request = stream_pb.CloseStreamRequest(
            namespace=self._namespace,
            stream=stream_pb.StreamRef(id=self._id),
            producer=_client_producer(self._client),
        )
        if result is not None:
            request.result.CopyFrom(
                (await _convert.encode(self._converter, result)).payload
            )
        if failure is not None:
            await self._converter.encode_failure(failure, request.failure)
        await self._backend.call_async(
            "CloseStream", request, stream_pb.CloseStreamResponse
        )


def for_handle(
    handle: temporalio.client.WorkflowHandle[Any, Any],
    *,
    name: str | None = None,
    wait_for_create: timedelta | None = None,
    end_with_run: bool = False,
    batch_interval: timedelta | None = None,
    max_batch_size: int | None = None,
    max_retry_duration: timedelta | None = None,
) -> ClientStream:
    """The default Stream of a Workflow, or the Stream it created with ``name``.

    It builds the id from the Workflow id and the first run id, so it makes no call.

    Args:
        handle: A handle with a first run id, such as one that ``start_workflow`` returned.
        name: The name that the Workflow gave ``create_workflow_stream``.
        wait_for_create: How long a read waits for a Stream that doesn't exist yet. Defaults
            to no wait.
        end_with_run: End a read when the run is no longer running, also with no close.
        batch_interval: How often this client's writes flush. Defaults to 100 ms.
        max_batch_size: At most this many Records in one batch.
        max_retry_duration: How long a flush retries a Store failure.
    """
    first_run_id = handle.first_execution_run_id or handle.run_id
    if not first_run_id:
        raise ValueError("The handle has no first run id, so the Stream id is unknown")
    stream_id = (
        _ids.scoped_stream_id(handle.id, first_run_id, name)
        if name is not None
        else _ids.default_stream_id(handle.id, first_run_id)
    )
    return ClientStream(
        handle._client,
        backend_of_client(handle._client),
        stream_id,
        batch_options(batch_interval, max_batch_size, max_retry_duration),
        wait_for_create=wait_for_create,
        handle=handle if end_with_run else None,
    )


async def create_stream(
    client: temporalio.client.Client,
    stream_id: str,
    *,
    retention: timedelta | None = None,
    batch_interval: timedelta | None = None,
    max_batch_size: int | None = None,
    max_retry_duration: timedelta | None = None,
) -> ClientStream:
    """Create a standalone Stream. The client owns it, so only the client closes it.

    It is idempotent for one id and one Retention.

    Raises:
        StreamError: The Stream exists with another Retention, with the reason
            ``stream_already_exists``.
    """
    backend = backend_of_client(client)
    request = stream_pb.CreateStreamRequest(
        namespace=client.namespace, stream_id=stream_id
    )
    if retention is not None:
        request.retention.FromTimedelta(retention)
    response = await backend.call_async(
        "CreateStream", request, stream_pb.CreateStreamResponse
    )
    if response.status == stream_pb.StreamStatus.STREAM_STATUS_CLOSED:
        raise _errors.StreamClosedError(
            f"The Stream {stream_id} was created and closed before",
            reason="stream_closed",
        )
    return ClientStream(
        client,
        backend,
        stream_id,
        batch_options(batch_interval, max_batch_size, max_retry_duration),
    )
