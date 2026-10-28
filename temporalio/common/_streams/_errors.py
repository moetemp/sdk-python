"""Errors of the Streams API in temporalio.common."""

from __future__ import annotations

from collections.abc import Sequence

import temporalio.exceptions
import temporalio.service
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.api.stream.v1 import StreamErrorDetails


class StreamError(temporalio.exceptions.ApplicationError):
    """Base class of every Stream error.

    Each Stream error is a non-retryable application error, except
    :py:class:`StreamStorageError`. So an uncaught Stream error in Workflow code fails the
    Workflow, and doesn't fail the Workflow Task again and again. A reason with no class of its
    own, such as ``STREAM_ERROR_REASON_SEQUENCE``, gives this base class.

    .. warning::
        This API is experimental.
    """

    _retryable = False

    def __init__(
        self,
        message: str,
        *,
        reason: StreamErrorReason.ValueType = StreamErrorReason.STREAM_ERROR_REASON_UNSPECIFIED,
        code: temporalio.service.RPCStatusCode | None = None,
    ) -> None:
        """Create a Stream error."""
        # The reason goes in the details, so it comes back from a Local Activity.
        super().__init__(
            message,
            *([int(reason)] if reason else []),
            type=type(self).__name__,
            non_retryable=not self._retryable,
        )
        self._reason = reason
        self._code = code

    @property
    def reason(self) -> StreamErrorReason.ValueType:
        """The ``StreamErrorReason`` of the cause. ``STREAM_ERROR_REASON_UNSPECIFIED`` if none."""
        return self._reason

    @property
    def code(self) -> temporalio.service.RPCStatusCode | None:
        """Status code of the failed call, as in gRPC.

        None if no call failed, or at a Workflow await, where the error comes back from a Local
        Activity.
        """
        return self._code


class StreamNotFoundError(StreamError):
    """The Stream does not exist, or it was not created in time.

    A publish needs a Stream that a create made first.
    """


class StreamClosedError(StreamError):
    """The Stream is closed, so it takes no more Records, and a create of it fails."""


class StreamExpiredError(StreamError):
    """Retention removed the Stream, or the Records at the offset that a read asked for."""


class StreamFencedError(StreamError):
    """A newer attempt of the same Producer wrote to the Stream, so this attempt can't."""


class StreamStorageError(StreamError):
    """The Store failed. A retry of the Activity may succeed."""

    _retryable = True


class StreamOffsetError(StreamError):
    """The offset is below zero or past the newest one plus one."""


class StreamTooLargeError(StreamError):
    """A Record or a batch is over the Store's limit."""


class StreamUnsupportedError(StreamError):
    """The Store lacks a capability that the call needs.

    The message ends with the name of the capability.
    """


class StreamNotOwnerError(StreamError):
    """The caller isn't allowed to close the Stream or write as that Producer.

    Only the Workflow's own run chain can close a Workflow's Stream. Only a client can close a
    client's Stream.
    """


class StreamSeqDivergentError(StreamError):
    """A resent batch has the sequence number of the Producer's last batch, but other content.

    A writer that restarts with the same Producer id and epoch sends its first sequence number
    again. Give it a higher epoch instead.
    """


class StreamConfigError(RuntimeError):
    """The Stream backend config is wrong, such as when no backend has a given name.

    It isn't a :py:class:`StreamError`, so ``except StreamError`` doesn't catch it. In Workflow
    code it fails the Workflow Task. An Activity that raises it retries.
    """


def no_default_backend(
    owner: str, names: Sequence[str], hint: str
) -> StreamConfigError:
    """The error of a call with no backend name when no backend has ``default=True``."""
    return StreamConfigError(
        f"No Stream backend of the {owner} ({', '.join(names)}) has default=True, so the "
        "default is native, which this SDK does not have. Mark a backend with "
        f"default=True, or name one, such as with {hint}."
    )


_BY_REASON: dict[StreamErrorReason.ValueType, type[StreamError]] = {
    StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND: StreamNotFoundError,
    StreamErrorReason.STREAM_ERROR_REASON_EXPIRED: StreamExpiredError,
    StreamErrorReason.STREAM_ERROR_REASON_CLOSED: StreamClosedError,
    StreamErrorReason.STREAM_ERROR_REASON_FENCED: StreamFencedError,
    StreamErrorReason.STREAM_ERROR_REASON_SEQ_DIVERGENT: StreamSeqDivergentError,
    StreamErrorReason.STREAM_ERROR_REASON_NOT_OWNER: StreamNotOwnerError,
    StreamErrorReason.STREAM_ERROR_REASON_OFFSET: StreamOffsetError,
    StreamErrorReason.STREAM_ERROR_REASON_TOO_LARGE: StreamTooLargeError,
    StreamErrorReason.STREAM_ERROR_REASON_STORAGE: StreamStorageError,
    StreamErrorReason.STREAM_ERROR_REASON_UNSUPPORTED: StreamUnsupportedError,
}


def from_call_error(err: Exception) -> StreamError:
    """Turn a failed call of the crate's runtime into the Stream error for its reason.

    ``err.args`` are the gRPC code, the message and the encoded ``StreamErrorDetails``.
    """
    code, message, details = err.args
    reason = StreamErrorDetails.FromString(details).reason if details else 0
    return from_reason(reason, message, temporalio.service.RPCStatusCode(code))


def from_reason(
    reason: int,
    message: str,
    code: temporalio.service.RPCStatusCode | None = None,
) -> StreamError:
    """The Stream error for a ``StreamErrorReason``."""
    value = StreamErrorReason.ValueType(reason)
    return _BY_REASON.get(value, StreamError)(message, reason=value, code=code)


_BY_TYPE: dict[str, type[StreamError]] = {
    "StreamError": StreamError,
    **{cls.__name__: cls for cls in _BY_REASON.values()},
}


def from_activity_error(err: Exception) -> Exception:
    """The Stream error that a write Local Activity raised, else ``err``."""
    cause = err.cause if isinstance(err, temporalio.exceptions.ActivityError) else err
    if (
        isinstance(cause, temporalio.exceptions.ApplicationError)
        and cause.type in _BY_TYPE
    ):
        reason = StreamErrorReason.ValueType(
            int(cause.details[0]) if cause.details else 0
        )
        return _BY_TYPE[cause.type](cause.message, reason=reason)
    return err
