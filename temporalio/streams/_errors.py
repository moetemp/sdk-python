"""Errors of the Streams API."""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

import temporalio.api.stream.v1
import temporalio.bridge.streams
import temporalio.exceptions
from temporalio.api.enums.v1 import StreamErrorReason

__all__ = [
    "StreamAlreadyExistsError",
    "StreamClosedError",
    "StreamConfigError",
    "StreamError",
    "StreamExpiredError",
    "StreamFencedError",
    "StreamNotFoundError",
    "StreamNotOwnerError",
    "StreamOffsetTooHighError",
    "StreamSeqDivergentError",
    "StreamStorageUnavailableError",
    "StreamTooLargeError",
    "StreamUnsupportedError",
]


class StreamConfigError(RuntimeError):
    """The Stream backend config is wrong, such as when no backend has a given name.

    It isn't a :py:class:`StreamError`, so ``except StreamError`` doesn't catch it. In Workflow
    code it fails the Workflow Task. An Activity that raises it retries.

    .. warning::
        This API is experimental.
    """


def no_default_backend(
    owner: str, names: Sequence[str], hint: str
) -> StreamConfigError:
    """The error of a call with no backend name when no backend has ``default=True``."""
    return StreamConfigError(
        f"No Stream backend of the {owner} ({', '.join(names)}) has default=True, so the "
        "default is native, which this SDK does not have. Mark a backend with "
        f"default=True, or give its name in {hint}."
    )


class StreamError(temporalio.exceptions.ApplicationError):
    """A failed Stream call, with its ``StreamErrorReason``.

    A reason with no class of its own, such as ``STREAM_ERROR_REASON_STORAGE_CORRUPT``, raises
    this class.

    .. warning::
        This API is experimental.
    """

    _reason: ClassVar[StreamErrorReason.ValueType] = (
        StreamErrorReason.STREAM_ERROR_REASON_UNSPECIFIED
    )
    _retryable: ClassVar[bool] = False

    def __init__(
        self, message: str, *, reason: StreamErrorReason.ValueType | None = None
    ) -> None:
        """Create the error. ``reason`` defaults to the reason of the class."""
        reason = self._reason if reason is None else reason
        # The details carry the reason, so the error keeps it when it crosses an Activity or a
        # Nexus operation.
        super().__init__(
            message,
            temporalio.api.stream.v1.StreamErrorDetails(reason=reason),
            type=type(self).__name__,
            non_retryable=not self._retryable,
        )
        self.reason: StreamErrorReason.ValueType = reason


class StreamNotFoundError(StreamError):
    """Nobody created the Stream, even after ``create_wait``.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND


class StreamExpiredError(StreamError):
    """The Stream's Retention or safety TTL passed.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_EXPIRED


class StreamClosedError(StreamError):
    """The Stream is closed.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_CLOSED


class StreamFencedError(StreamError):
    """A newer attempt of the Producer wrote, or a retry withdrew this attempt.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_FENCED


class StreamSeqDivergentError(StreamError):
    """A batch has the sequence of the last batch of its epoch, and other content.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_SEQ_DIVERGENT


class StreamNotOwnerError(StreamError):
    """A create gives another owner than the Stream has, or the owner refuses this close.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_NOT_OWNER


class StreamOffsetTooHighError(StreamError):
    """The offset is past the newest offset + 1.

    A read from it works again once the Stream has Records up to it.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_OFFSET_TOO_HIGH


class StreamTooLargeError(StreamError):
    """A Record or a batch is over the Store's limit.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_TOO_LARGE


class StreamStorageUnavailableError(StreamError):
    """The Store failed or did not answer. It is the only retryable Stream error.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_STORAGE_UNAVAILABLE
    _retryable = True


class StreamUnsupportedError(StreamError):
    """The Store or the code that makes the call lacks a capability that the call needs.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_UNSUPPORTED


class StreamAlreadyExistsError(StreamError):
    """A create of an id that exists, with no earlier create of the same request.

    .. warning::
        This API is experimental.
    """

    _reason = StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS


_ERRORS: dict[int, type[StreamError]] = {
    error._reason: error
    for error in (
        StreamNotFoundError,
        StreamExpiredError,
        StreamClosedError,
        StreamFencedError,
        StreamSeqDivergentError,
        StreamNotOwnerError,
        StreamOffsetTooHighError,
        StreamTooLargeError,
        StreamStorageUnavailableError,
        StreamUnsupportedError,
        StreamAlreadyExistsError,
    )
}


def from_reason(reason: int, message: str) -> StreamError:
    """The Stream error of a ``StreamErrorReason`` number."""
    if reason not in StreamErrorReason.values():
        # A reason that this SDK doesn't know gives the base class.
        reason = StreamErrorReason.STREAM_ERROR_REASON_UNSPECIFIED
    return _ERRORS.get(reason, StreamError)(
        message, reason=StreamErrorReason.ValueType(reason)
    )


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
