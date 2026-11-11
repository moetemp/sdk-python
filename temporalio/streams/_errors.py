"""Errors of the Streams API."""

from __future__ import annotations

import temporalio.api.stream.v1
import temporalio.bridge.streams
import temporalio.exceptions
from temporalio.api.enums.v1 import StreamErrorReason

__all__ = [
    "StreamConfigError",
    "StreamError",
]

# The one reason a retry can fix. Every other reason raises a non-retryable error.
_RETRYABLE_REASONS: frozenset[int] = frozenset(
    {StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE}
)


class StreamConfigError(RuntimeError):
    """The Stream config is wrong, such as when no backend has a given name.

    The cases are a missing or duplicate backend, more than one backend with no selector, a
    selector that returns a backend that isn't registered, a backend in ``plugins=``, a Worker
    whose client has two backends, a ref whose backend the client doesn't have, and a bridge
    built without the Stream runtime.

    It isn't a :py:class:`StreamError`, so ``except StreamError`` doesn't catch it. In Workflow
    code it fails the Workflow Task. An Activity that raises it retries.

    .. warning::
        This API is experimental.
    """


class StreamError(temporalio.exceptions.ApplicationError):
    """A failed Stream call. ``reason`` tells the cases apart.

    One class covers every failed call, so match on ``reason`` rather than the type. The reason
    is a ``StreamErrorReason``, such as ``STREAM_ERROR_REASON_NOT_FOUND`` (nobody created the
    Stream), ``STREAM_ERROR_REASON_CLOSED`` (the Stream is closed),
    ``STREAM_ERROR_REASON_FENCED`` (a newer attempt of the Producer wrote), or
    ``STREAM_ERROR_REASON_ALREADY_EXISTS`` (a ``FAIL`` create of an id that exists). A Store
    outage (``STREAM_ERROR_REASON_STORAGE_UNAVAILABLE``) is the one retryable reason.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self, message: str, *, reason: StreamErrorReason.ValueType | None = None
    ) -> None:
        """Create the error. ``reason`` defaults to ``STREAM_ERROR_REASON_UNSPECIFIED``."""
        reason = (
            StreamErrorReason.STREAM_ERROR_REASON_UNSPECIFIED
            if reason is None
            else reason
        )
        # The details carry the reason, so the error keeps it when it crosses an Activity or a
        # Nexus operation. The reason, not the class, decides whether a retry can fix it.
        super().__init__(
            message,
            temporalio.api.stream.v1.StreamErrorDetails(reason=reason),
            type=type(self).__name__,
            non_retryable=reason not in _RETRYABLE_REASONS,
        )
        self.reason: StreamErrorReason.ValueType = reason


def closed(message: str) -> StreamError:
    """A ``StreamError`` for a write to, or close of, a Stream that is already closed."""
    return StreamError(message, reason=StreamErrorReason.STREAM_ERROR_REASON_CLOSED)


def not_owner(message: str) -> StreamError:
    """A ``StreamError`` for code that may not close a Stream it does not own."""
    return StreamError(message, reason=StreamErrorReason.STREAM_ERROR_REASON_NOT_OWNER)


def unsupported(message: str) -> StreamError:
    """A ``StreamError`` for a call that the code or the Store does not support."""
    return StreamError(
        message, reason=StreamErrorReason.STREAM_ERROR_REASON_UNSUPPORTED
    )


def has_reason(err: BaseException, reason: StreamErrorReason.ValueType) -> bool:
    """Whether ``err`` is a :py:class:`StreamError` with ``reason``."""
    return isinstance(err, StreamError) and err.reason == reason


def from_reason(reason: int, message: str) -> StreamError:
    """The Stream error of a ``StreamErrorReason`` number."""
    if reason not in StreamErrorReason.values():
        # A reason that this SDK doesn't know gives the unspecified reason.
        reason = StreamErrorReason.STREAM_ERROR_REASON_UNSPECIFIED
    return StreamError(message, reason=StreamErrorReason.ValueType(reason))


def from_failure(err: BaseException) -> StreamError | None:
    """The Stream error that a failed call carries, or None if it carries none.

    A failed call of the crate's runtime has the gRPC code, the message and the encoded
    ``StreamErrorDetails`` as its args. Another failed call, such as a Local Activity's or a
    Nexus operation's, has an application error in its causes, whose details hold the
    ``StreamErrorDetails``.
    """
    if isinstance(err, temporalio.bridge.streams.StreamCallError):
        _, message, details = err.args
        proto = temporalio.api.stream.v1.StreamErrorDetails.FromString(details)
        return from_reason(proto.reason, message)
    current: BaseException | None = err
    while current is not None:
        if isinstance(current, StreamError):
            return current
        if isinstance(current, temporalio.exceptions.ApplicationError):
            for detail in current.details:
                if isinstance(detail, temporalio.api.stream.v1.StreamErrorDetails):
                    return from_reason(detail.reason, current.message)
        current = current.__cause__
    return None
