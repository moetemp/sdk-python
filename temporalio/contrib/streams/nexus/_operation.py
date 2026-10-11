"""A Nexus operation whose start hands its caller a stream.

The operation stays open while the stream runs. Its start attaches the
caller's callback to the stream's notifier on the server, so the notifier
tells the caller when the stream moves (Nexus operation progress) and
completes the operation when the stream closes. The caller learns which
stream it is from the operation token. Core resolves the stream's run chain,
mints the token and attaches the callback.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

import nexusrpc
import nexusrpc.handler

import temporalio.nexus
from temporalio.bridge.proto.streams import (
    AttachCallerRequest,
    DetachCallerRequest,
    StreamAddress,
    StreamOwnerKind,
)
from temporalio.contrib.streams._errors import (
    StreamError,
    StreamNotFoundError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._plugin import call, store_for_client
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams.nexus._generated.models import StreamOperationToken

__all__ = ["StreamOperationHandler", "stream_ref_from_token"]

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


def stream_ref_from_token(token: str) -> StreamRef:
    """The stream a stream-returning operation's token names.

    This is how a caller learns the stream: the start answers with the
    operation token, and the token carries the stream reference.

    Raises:
        ValueError: ``token`` did not come from :class:`StreamOperationHandler`.
    """
    return StreamOperationToken.from_token(token).ref


def _handler_error(error: StreamError | ValueError) -> nexusrpc.HandlerError:
    # The same table the stream service's operations answer with, so a caller
    # sees one meaning for each kind.
    if isinstance(error, ValueError):
        kind, retryable = nexusrpc.HandlerErrorType.BAD_REQUEST, False
    elif isinstance(error, StreamNotFoundError):
        kind, retryable = nexusrpc.HandlerErrorType.NOT_FOUND, False
    elif isinstance(error, StreamUnsupportedError):
        kind, retryable = nexusrpc.HandlerErrorType.NOT_IMPLEMENTED, False
    elif isinstance(error, StreamStorageError):
        kind, retryable = nexusrpc.HandlerErrorType.UNAVAILABLE, True
    else:
        kind, retryable = nexusrpc.HandlerErrorType.INTERNAL, False
    return nexusrpc.HandlerError(str(error), type=kind, retryable_override=retryable)


class StreamOperationHandler(
    nexusrpc.handler.OperationHandler[InputT, OutputT], Generic[InputT, OutputT]
):
    """A Nexus operation whose start hands its caller a stream.

    ``open_stream`` names the stream for a start: it creates or finds the
    stream and returns its reference. The start then attaches the caller's
    callback to that stream's notifier and answers asynchronously, with the
    reference in the operation token. From then on the server tells the
    caller each time the stream moves, when the store has
    :meth:`temporalio.contrib.streams.StreamStorePlugin.notify_on_append` on,
    and completes the operation with the close result when the stream closes.
    A cancel detaches the caller.

    The operation's output type is the close result's type. The records
    themselves are read through the stream service, not carried by the
    operation.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        open_stream: Callable[
            [nexusrpc.handler.StartOperationContext, InputT], Awaitable[StreamRef]
        ],
    ) -> None:
        """Hand out the stream ``open_stream`` names for each start."""
        self._open_stream = open_stream

    async def start(
        self, ctx: nexusrpc.handler.StartOperationContext, input: InputT
    ) -> nexusrpc.handler.StartOperationResultAsync:
        """Attach the caller to the stream and answer with its reference.

        Raises:
            nexusrpc.HandlerError: The caller sent no callback, so it could
                never hear about the stream (``BAD_REQUEST``), or the server
                didn't take the attach.
        """
        if not ctx.callback_url:
            # Refused before open_stream runs, so a start that can't work
            # creates nothing.
            raise _handler_error(
                ValueError(
                    "a stream-returning operation needs a caller callback; start "
                    "it asynchronously"
                )
            )
        ref = await self._open_stream(ctx, input)
        client = temporalio.nexus.client()
        try:
            ref._require_supported()
            service = await store_for_client(client)._nexus_service_for(client)
            response = await call(
                service.attach_caller(
                    AttachCallerRequest(
                        stream=StreamAddress(
                            namespace=client.namespace,
                            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                            workflow_id=ref.workflow_id,
                            run_id=ref.run_id or "",
                            topic=ref.topic,
                        ),
                        request_id=ctx.request_id,
                        callback_url=ctx.callback_url,
                        callback_headers=dict(ctx.callback_headers),
                    )
                )
            )
        except (StreamError, ValueError) as error:
            raise _handler_error(error) from None
        return nexusrpc.handler.StartOperationResultAsync(
            token=response.operation_token
        )

    async def cancel(
        self, ctx: nexusrpc.handler.CancelOperationContext, token: str
    ) -> None:
        """Detach the caller: the stream goes on, the operation ends.

        Raises:
            nexusrpc.HandlerError: ``token`` is not one this operation issued
                (``BAD_REQUEST``), or the server didn't take the detach.
        """
        client = temporalio.nexus.client()
        try:
            service = await store_for_client(client)._nexus_service_for(client)
            await call(
                service.detach_caller(
                    DetachCallerRequest(
                        namespace=client.namespace, operation_token=token
                    )
                )
            )
        except (StreamError, ValueError) as error:
            raise _handler_error(error) from None
