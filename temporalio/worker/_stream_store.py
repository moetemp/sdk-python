"""What a Worker does for the Stream Store of its client."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable, Generator
from typing import TYPE_CHECKING, Any

from nexusrpc import OutputT

import temporalio.activity
import temporalio.converter
import temporalio.exceptions
import temporalio.workflow
from temporalio.client._stream_store import (
    StreamConfig,
    StreamStore,
    StreamStoreCallOptions,
    _call_stream_store,
    _request_ref,
    _stream_response,
)
from temporalio.exceptions import StreamConfigError

from ._interceptor import StartLocalActivityInput, StartNexusOperationInput

if TYPE_CHECKING:
    from ._workflow_instance import _WorkflowInstanceImpl

_STREAM_STORE_ACTIVITY = "__temporal_stream_store_call"

_STREAM_STORE_REFUSALS = {
    "AppendRecords": (
        "Workflow code can't append to a Stream on a Store in this release. "
        "Call a Local Activity that appends through its client."
    ),
    "CloseStream": (
        "Workflow code can't close a Stream on a Store in this release. "
        "Call an Activity that closes it through its client."
    ),
    "ReadRecords": (
        "Workflow code can't read a Stream on a Store in this release. "
        "Call an Activity that reads it through its client."
    ),
}


def _stream_store_activity(  # pyright: ignore[reportUnusedFunction]
    store: StreamStore, data_converter: temporalio.converter.DataConverter
) -> Callable[[str, bytes], Awaitable[bytes]]:
    @temporalio.activity.defn(name=_STREAM_STORE_ACTIVITY)
    async def call(operation: str, request: bytes) -> bytes:
        return await _call_stream_store(store, data_converter, operation, request)

    return call


def _worker_stream_store(config: StreamConfig | None) -> StreamStore | None:
    """Give the one Store that a Worker serves, or ``None`` when its client has none.

    Raises:
        temporalio.exceptions.StreamConfigError: The client has more than one Store.
    """
    if config is None:
        return None
    # Workflow code never calls the selector, so a Workflow's new Stream needs a single
    # Store that the Worker knows without one.
    if len(config.stores) > 1:
        raise StreamConfigError(
            f"A Worker serves one Stream Store, and its client has {len(config.stores)}"
        )
    return config.stores[0]


@dataclasses.dataclass(frozen=True)
class _WorkflowStreamStore:
    """The Worker's Stream Store as Workflow code reaches it."""

    name: str
    """Name of the Store."""

    call_options: StreamStoreCallOptions
    """Options of the Local Activity that calls the Store."""

    @classmethod
    def from_config(cls, config: StreamConfig | None) -> _WorkflowStreamStore | None:
        """Build it from the Stream config of a client, or give ``None`` when it has none.

        Raises:
            temporalio.exceptions.StreamConfigError: The client has more than one Store.
        """
        store = _worker_stream_store(config)
        if store is None:
            return None
        return cls(name=store.name, call_options=store.call_options)

    def start_operation(
        self,
        instance: _WorkflowInstanceImpl,
        input: StartNexusOperationInput[Any, OutputT],
    ) -> temporalio.workflow.NexusOperationHandle[OutputT]:
        """Answer a Stream call of Workflow code with a Local Activity that calls the Store."""
        options = self.call_options
        # The generator can't leave these methods out of the Workflow `Stream` yet. In
        # this release, Workflow code writes and reads a Stream on a Store through its
        # own Activities.
        refusal = _STREAM_STORE_REFUSALS.get(input.operation_name)
        if refusal is not None:
            raise temporalio.exceptions.ApplicationError(refusal, non_retryable=True)
        # Workflow code builds the request, so the Local Activity does only the I/O.
        payload_converter, _ = instance._system_nexus_converters(input)
        [request] = payload_converter.to_payloads([input.input])
        # The Local Activity calls the Worker's Store, so a ref of another Store can't be
        # answered here. The error fails the Workflow Task, so that a fixed Worker can retry it.
        ref_store = _request_ref(input.operation_name, request.data).store
        if ref_store and ref_store != self.name:
            raise StreamConfigError(
                f"The ref names the Stream Store '{ref_store}', and this Worker serves "
                f"'{self.name}'"
            )
        activity = instance._outbound_schedule_activity(
            StartLocalActivityInput(
                activity=_STREAM_STORE_ACTIVITY,
                args=(input.operation_name, request.data),
                activity_id=None,
                schedule_to_close_timeout=options.schedule_to_close_timeout,
                schedule_to_start_timeout=None,
                start_to_close_timeout=options.start_to_close_timeout,
                retry_policy=options.retry_policy,
                local_retry_threshold=None,
                cancellation_type=temporalio.workflow.ActivityCancellationType.TRY_CANCEL,
                headers={},
                summary=input.summary,
                event_groups=input.event_groups,
                arg_types=[str, bytes],
                ret_type=bytes,
            )
        )

        async def result() -> OutputT:
            try:
                response = await activity
            except temporalio.exceptions.ApplicationError as err:
                # Core hands back a failed Local Activity's error without a wrapper.
                stream_error = temporalio.exceptions._stream_error_from_failure(err)
                if stream_error is not None:
                    raise stream_error from None
                raise
            except temporalio.exceptions.ActivityError as err:
                # Workflow code gets the Store's error, not the Local Activity's wrapper.
                if err.cause is not None:
                    stream_error = temporalio.exceptions._stream_error_from_failure(
                        err.cause
                    )
                    if stream_error is not None:
                        raise stream_error from None
                if isinstance(
                    err.cause,
                    (
                        temporalio.exceptions.ApplicationError,
                        temporalio.exceptions.TimeoutError,
                    ),
                ):
                    raise err.cause
                raise
            return _stream_response(
                payload_converter, input.operation_name, input.output_type, response
            )

        return _StreamStoreOperationHandle(
            instance.create_task(result(), name=f"stream store: {input.operation_name}")
        )


class _StreamStoreOperationHandle(temporalio.workflow.NexusOperationHandle[OutputT]):
    def __init__(self, task: asyncio.Task[OutputT]) -> None:
        self._task = task

    def cancel(self) -> bool:
        return self._task.cancel()

    def __await__(self) -> Generator[Any, Any, OutputT]:
        return self._task.__await__()

    @property
    def operation_token(self) -> str | None:
        return None
