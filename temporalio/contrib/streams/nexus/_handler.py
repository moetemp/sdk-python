"""The stream service's handler, which Core serves."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Awaitable, Callable
from datetime import timezone
from typing import Any, Literal

import nexusrpc
import nexusrpc.handler
from google.protobuf.json_format import MessageToDict

import temporalio.api.failure.v1
import temporalio.nexus
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import NexusHandlerErrorRetryBehavior
from temporalio.bridge.proto.streams import (
    CodecResult,
    CodecWork,
    NexusCall,
    NexusFailure,
    NexusLimits,
    NexusResumeRequest,
)
from temporalio.contrib.streams._body import decode_body, encode_bodies
from temporalio.contrib.streams._plugin import call, store_for_client
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams.nexus._generated import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    TemporalStreams,
)
from temporalio.contrib.streams.nexus._generated.forwarder import (
    TemporalStreamsForwarder,
)
from temporalio.converter import DataConverter, WorkflowSerializationContext

__all__ = ["StreamAccess", "TemporalStreamsHandler"]

# The Nexus failure metadata type under which the details are a
# temporal.api.failure.v1.Failure, which the failure converter reads back as is.
_TEMPORAL_FAILURE_TYPE = "temporal.api.failure.v1.Failure"


@dataclasses.dataclass(frozen=True)
class StreamAccess:
    """One call the stream service is about to serve, for an authorizer.

    .. warning::
        This API is experimental.
    """

    operation: Literal["append", "read"]
    """What the caller asks to do."""

    stream: StreamRef
    """The stream the call names."""

    context: nexusrpc.handler.StartOperationContext
    """The Nexus call, with the caller's headers."""

    producer_id: str | None = None
    """The producer an append writes as. ``None`` for a read."""

    attempt: int | None = None
    """The producer attempt an append writes. ``None`` for a read."""


@nexusrpc.handler.service_handler(service=TemporalStreams)
class TemporalStreamsHandler(TemporalStreamsForwarder):
    """Serves the stream service over the store of its Worker's client.

    Register it on a Worker with ``nexus_service_handlers=[handler]`` and
    point a Nexus endpoint at the Worker's task queue. The Worker's client
    must carry a stream store, and a stream lives in the client's namespace.

    Core serves each call and runs every rule of the contract. The handler
    runs only what Core can't: ``authorize``, and the namespace's payload
    codec from the Worker's client. The store keeps each body encoded with
    that codec, and a read decodes the bodies, so Workflow code that reads
    gets plain bodies with no codec of its own. Core holds those plain bodies
    in memory during the call only. It never stores or logs them.

    The handler holds no state between calls. A reader carries Core's read
    state in ``ReadInput.state``, so any Worker on the endpoint's task queue
    can serve its next call, and retries are deduplicated by the store.

    A read answers with at most ``max_records`` records and, past its first
    record, at most ``max_answer_bytes`` of records as they cross, so a read's
    result stays below the server's blob size limit. A record whose bytes
    would take more than ``max_record_bytes`` in a read result can never be
    read through the service, so an append with one is refused. A record
    already in the store that is larger, a record Core can't parse, and a
    record whose body the codec can't decode each cross as an error at their
    cursor, which a reader goes on past. Sizes count what crosses: the record
    bytes as base64, about 4/3 of them.

    The handler writes and reads with its Worker's client and that client's
    store, whatever caller sent the call. So every caller that may reach the
    endpoint gets the Worker's store rights, for every stream in the
    namespace and as any producer. Pass ``authorize`` to check each call: it
    gets a :class:`StreamAccess` and answers whether to serve it, and a
    refused call fails as unauthorized.

    Parked reads hold Nexus task slots for up to their ``wait_ms``. With many
    readers, serve appends and reads on separate task queues or endpoints, so
    an append doesn't wait for a slot behind parked reads, and give the
    Worker more Nexus pollers than the default (``nexus_task_poller_behavior``).
    """

    def __init__(
        self,
        *,
        max_answer_bytes: int = 1 << 20,
        max_record_bytes: int = 3 << 19,
        authorize: Callable[[StreamAccess], Awaitable[bool]] | None = None,
    ) -> None:
        """Serve the store of the Worker's client.

        Raises:
            ValueError: ``max_answer_bytes`` or ``max_record_bytes`` is not
                positive.
        """
        if max_answer_bytes <= 0:
            raise ValueError("max_answer_bytes must be positive")
        if max_record_bytes <= 0:
            raise ValueError("max_record_bytes must be positive")
        self._limits = NexusLimits(
            max_answer_bytes=max_answer_bytes, max_record_bytes=max_record_bytes
        )
        self._authorize = authorize

    @nexusrpc.handler.sync_operation
    async def append(
        self, ctx: nexusrpc.handler.StartOperationContext, input: AppendInput
    ) -> AppendOutput:
        """Write one batch, or finish the producer."""
        await self._check(
            StreamAccess(
                "append",
                input.stream,
                ctx,
                producer_id=input.producer_id,
                attempt=input.attempt,
            )
        )
        return await super().append(ctx, input)

    @nexusrpc.handler.sync_operation
    async def read(
        self, ctx: nexusrpc.handler.StartOperationContext, input: ReadInput
    ) -> ReadOutput:
        """Answer with the records after the caller's cursor, waiting for some."""
        await self._check(StreamAccess("read", input.stream, ctx))
        return await super().read(ctx, input)

    async def forward(
        self,
        operation: str,
        ctx: nexusrpc.handler.StartOperationContext,
        transfer: Any,
    ) -> Any:
        """Serve one operation in Core, running the codec when Core asks."""
        client = temporalio.nexus.client()
        service = await store_for_client(client)._nexus_service_for(client)
        request = NexusCall(
            input=json.dumps(transfer).encode(),
            limits=self._limits,
            namespace=client.namespace,
        )
        if ctx.request_deadline is not None:
            deadline = ctx.request_deadline
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
            request.deadline.FromDatetime(deadline)
        if operation == "append":
            step = await call(service.nexus_append(request))
        else:
            step = await call(service.nexus_read(request))
        while step.HasField("codec"):
            results = await _run_codec(client.data_converter, step.codec)
            step = await call(
                service.nexus_resume(
                    NexusResumeRequest(
                        continuation=step.codec.continuation, results=results
                    )
                )
            )
        if step.HasField("failure"):
            raise _handler_error(step.failure)
        return json.loads(step.output)

    async def _check(self, access: StreamAccess) -> None:
        if self._authorize is None:
            return
        try:
            allowed = await self._authorize(access)
        except Exception as error:
            # The authorizer is user code, so its failure crosses spelled out,
            # with its stack frames left on this side.
            raise _handler_error(_internal(error)) from None
        if allowed:
            return
        raise nexusrpc.HandlerError(
            f"the caller may not {access.operation} stream {access.stream.topic!r} "
            f"of Workflow {access.stream.workflow_id!r}",
            type=nexusrpc.HandlerErrorType.UNAUTHORIZED,
            retryable_override=False,
        )


async def _run_codec(converter: DataConverter, work: CodecWork) -> list[CodecResult]:
    # The stream is the Workflow's, so its bodies use the Workflow's context.
    converter = converter.with_context(
        WorkflowSerializationContext(
            namespace=work.namespace, workflow_id=work.workflow_id
        )
    )
    payloads = list(work.payloads)
    if work.encode:
        try:
            encoded = await encode_bodies(converter, payloads)
        except Exception as error:
            # An append's batch is one write, so one failed body fails it.
            failed = CodecResult(error=f"{type(error).__name__}: {error}")
            return [failed] * len(payloads)
        return [CodecResult(payload=payload) for payload in encoded]
    results: list[CodecResult] = []
    for payload in payloads:
        # Each record decodes on its own, so a body the codec refuses crosses
        # as an error at its cursor and the rest of the read goes on.
        try:
            decoded: Payload = await decode_body(converter, payload)
        except Exception as error:
            results.append(CodecResult(error=f"{type(error).__name__}: {error}"))
        else:
            results.append(CodecResult(payload=decoded))
    return results


def _internal(error: Exception) -> NexusFailure:
    # The same shape Core gives a stream condition, with the error's class as
    # the cause's type and no stack.
    name = type(error).__name__
    return NexusFailure(
        handler_error_type=nexusrpc.HandlerErrorType.INTERNAL.name,
        retryable=True,
        message=f"{name}: {error}",
        failure=temporalio.api.failure.v1.Failure(
            cause=temporalio.api.failure.v1.Failure(
                message=str(error),
                application_failure_info=temporalio.api.failure.v1.ApplicationFailureInfo(
                    type=name
                ),
            ),
            nexus_handler_failure_info=temporalio.api.failure.v1.NexusHandlerFailureInfo(
                type=nexusrpc.HandlerErrorType.INTERNAL.name,
                retry_behavior=NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
            ),
        ),
    )


def _handler_error(failure: NexusFailure) -> nexusrpc.HandlerError:
    kind = nexusrpc.HandlerErrorType[failure.handler_error_type]
    return nexusrpc.HandlerError(
        failure.message,
        type=kind,
        retryable_override=failure.retryable,
        original_failure=nexusrpc.Failure(
            message=failure.message,
            metadata={"type": _TEMPORAL_FAILURE_TYPE},
            details=MessageToDict(failure.failure),
        ),
    )
