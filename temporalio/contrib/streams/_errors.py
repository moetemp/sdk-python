"""Errors of temporalio.contrib.streams."""

from __future__ import annotations

import temporalio.exceptions
import temporalio.service
from temporalio.contrib.streams.proto.v1 import StreamErrorDetails


class StreamError(temporalio.exceptions.ApplicationError):
    """Base of every Stream error.

    It is an application error that is not retryable, except
    :py:class:`StreamStorageError`. So in Workflow code, where only
    :py:class:`StreamClosedError` and the plugin errors are raised, an uncaught one fails the
    Workflow and does not fail the Workflow Task again and again.

    .. warning::
        This API is experimental.
    """

    _retryable = False

    def __init__(
        self,
        message: str,
        *,
        reason: str = "",
        code: temporalio.service.RPCStatusCode | None = None,
    ) -> None:
        """Create a Stream error."""
        # The reason goes in the details, so it comes back from a Local Activity.
        super().__init__(
            message,
            *([reason] if reason else []),
            type=type(self).__name__,
            non_retryable=not self._retryable,
        )
        self._reason = reason
        self._code = code

    @property
    def reason(self) -> str:
        """Token of the cause, such as ``stream_closed``. Empty if the error has none."""
        return self._reason

    @property
    def code(self) -> temporalio.service.RPCStatusCode | None:
        """Status code of the failed call, as in gRPC. None if no call failed."""
        return self._code


class StreamNotFoundError(StreamError):
    """The Stream does not exist, or it was not created in time."""


class StreamClosedError(StreamError):
    """The Stream is closed, so it takes no more Records."""


class StreamExpiredError(StreamError):
    """Retention removed the Stream."""


class StreamFencedError(StreamError):
    """A newer attempt of the same Producer wrote to the Stream, so this attempt can't."""


class StreamStorageError(StreamError):
    """The Store failed. A retry of the Activity may succeed."""

    _retryable = True


class StreamOffsetError(StreamError):
    """The offset is past the newest one plus one, or Retention removed it."""


class StreamUnsupportedError(StreamError):
    """The Store lacks a capability that the call needs. The message ends with its name."""


class StreamNotOwnerError(StreamError):
    """The caller may not close the Stream or write as that Producer, since the Stream's owner
    doesn't allow it.

    A Workflow's Stream closes only from its own run chain, and a client's Stream only from a
    client.
    """


class StreamSeqDivergentError(StreamError):
    """A resend has the sequence of the Producer's last batch and other content.

    A writer that starts again with the same Producer id and epoch sends its first sequence
    again. Give it a higher epoch instead.
    """


class StreamConfigError(RuntimeError):
    """No Stream plugin, or no backend of a given name. It is config, not a Store error.

    ``except StreamError`` does not catch it. It fails a Workflow Task, and an Activity retries.
    """


class ReadOnlyContextError(StreamError):
    """Workflow code that must not change state, such as a query handler, wrote to a Stream."""


_BY_REASON: dict[str, type[StreamError]] = {
    "stream_not_found": StreamNotFoundError,
    "stream_closed": StreamClosedError,
    "stream_expired": StreamExpiredError,
    "stream_fenced": StreamFencedError,
    "stream_storage": StreamStorageError,
    "stream_offset": StreamOffsetError,
    "stream_unsupported": StreamUnsupportedError,
    "stream_not_owner": StreamNotOwnerError,
    "seq_divergent": StreamSeqDivergentError,
}


# The details move to temporal.api.streamservice.v1. Both names carry the same message.
_DETAILS_TYPES = (
    "temporal.api.streamservice.v1.StreamErrorDetails",
    StreamErrorDetails.DESCRIPTOR.full_name,
)


def from_call_error(err: Exception) -> StreamError:
    """Turn a failed call of the crate's runtime into the Stream error for its reason.

    Its args are the gRPC code, the message and the encoded ``StreamErrorDetails``.
    """
    code, message, details = err.args
    reason = StreamErrorDetails.FromString(details).reason if details else ""
    return _BY_REASON.get(reason, StreamError)(
        message, reason=reason, code=temporalio.service.RPCStatusCode(code)
    )


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
        reason = str(cause.details[0]) if cause.details else ""
        return _BY_TYPE[cause.type](cause.message, reason=reason)
    return err


def from_rpc_error(err: temporalio.service.RPCError) -> Exception:
    """The Stream error for a server failure with Stream error details, else ``err``."""
    for detail in err.grpc_status.details:
        if detail.type_url.rsplit("/", 1)[-1] in _DETAILS_TYPES:
            reason = StreamErrorDetails.FromString(detail.value).reason
            return _BY_REASON.get(reason, StreamError)(
                err.message, reason=reason, code=err.status
            )
    return err
