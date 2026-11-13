"""What a Worker does for the Stream Stores of its client."""

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
    _STREAM_OPERATIONS,
    StreamConfig,
    _call_stream_store,
    _request_ref,
)
from temporalio.converter import StorageDriverWorkflowInfo
from temporalio.nexus.system.stream_service import StreamRef

from ._interceptor import StartLocalActivityInput, StartNexusOperationInput

if TYPE_CHECKING:
    from ._workflow_instance import _WorkflowInstanceImpl

_STREAM_STORE_ACTIVITY = "__temporal_stream_store_call"


def _stream_store_activity(  # pyright: ignore[reportUnusedFunction]
    config: StreamConfig, data_converter: temporalio.converter.DataConverter
) -> Callable[[str, bytes, str, str], Awaitable[bytes]]:
    @temporalio.activity.defn(name=_STREAM_STORE_ACTIVITY)
    async def call(
        operation: str, request: bytes, stream_id: str, store_name: str
    ) -> bytes:
        # The Workflow passes the ref beside the encoded request, so that the request
        # isn't decoded again to route it. An empty `store_name` means no Store.
        ref = StreamRef[Any](id=stream_id, store=store_name or None)
        store = config._store_for(ref, _workflow_target())
        message = _STREAM_OPERATIONS[operation].request_type.FromString(request)
        response = await _call_stream_store(store, data_converter, operation, message)
        return response.SerializeToString()

    return call


def _workflow_target() -> StorageDriverWorkflowInfo:
    # The selector sees the Workflow that made the Stream call, not the SDK's Local
    # Activity. External storage builds the same value for an Activity of a Workflow.
    info = temporalio.activity.info()
    return StorageDriverWorkflowInfo(
        id=info.workflow_id or None,
        type=info.workflow_type or None,
        run_id=info.workflow_run_id or None,
        namespace=info.workflow_namespace or info.namespace,
    )


@dataclasses.dataclass(frozen=True)
class _WorkflowStreamStore:
    """The Worker's Stream Stores as Workflow code reaches them."""

    config: StreamConfig
    """The Stream config of the Worker's client."""

    @classmethod
    def from_config(cls, config: StreamConfig | None) -> _WorkflowStreamStore | None:
        """Build it from the Stream config of a client, or give ``None`` when it has none."""
        return cls(config=config) if config is not None else None

    def start_operation(
        self,
        instance: _WorkflowInstanceImpl,
        input: StartNexusOperationInput[Any, OutputT],
    ) -> temporalio.workflow.NexusOperationHandle[OutputT]:
        """Answer a Stream call of Workflow code with a Local Activity that calls a Store."""
        # The Workflow `Stream` has no `append`, `close` or `read`, but the generated
        # Workflow service still has their operations, so Workflow code can start one
        # through a Nexus client. In this release, Workflow code writes and reads a Stream
        # on a Store through its own Activities.
        stream_operation = _STREAM_OPERATIONS[input.operation_name]
        if stream_operation.workflow_refusal is not None:
            raise temporalio.exceptions.ApplicationError(
                stream_operation.workflow_refusal, non_retryable=True
            )
        # Workflow code builds the request, so the Local Activity does only the I/O.
        payload_converter, _ = instance._system_nexus_converters(input)
        request = payload_converter._to_transfer_value(input.input)
        # A ref of a Store that the config doesn't have fails here, with no I/O. Its
        # `StreamConfigError` fails the Workflow Task.
        ref = _request_ref(input.input)
        if ref.store:
            self.config._get_store_by_name(ref.store)
        options = self.config.workflow_call_options
        activity = instance._outbound_schedule_activity(
            StartLocalActivityInput(
                activity=_STREAM_STORE_ACTIVITY,
                args=(
                    input.operation_name,
                    request.SerializeToString(),
                    ref.id,
                    ref.store or "",
                ),
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
                arg_types=[str, bytes, str, str],
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
            return payload_converter._from_transfer_value(
                stream_operation.response_type.FromString(response),
                input.output_type,
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
