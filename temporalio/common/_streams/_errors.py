"""Errors of the Streams API in temporalio.common."""

from __future__ import annotations

import temporalio.exceptions
import temporalio.service
from temporalio.common._streams.proto.v1 import StreamErrorDetails


class StreamError(temporalio.exceptions.ApplicationError):
    """Base class of every Stream error.

    Each Stream error is a non-retryable application error, except
    :py:class:`StreamStorageError`. So an uncaught Stream error in Workflow code fails the
    Workflow, and doesn't fail the Workflow Task again and again. In Workflow code, only
    :py:class:`StreamClosedError` and the backend errors are raised.

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
    """Retention removed the Stream, or the Records at the offset that a read asked for."""


class StreamFencedError(StreamError):
    """A newer attempt of the same Producer wrote to the Stream, so this attempt can't."""


class StreamStorageError(StreamError):
    """The Store failed. A retry of the Activity may succeed."""

    _retryable = True


class StreamOffsetError(StreamError):
    """The offset is below zero or past the newest one plus one."""


class StreamTooLargeError(StreamError):
    """A Record or a batch is over the Store's limit.

    A native Stream also keeps the Records that an open Workflow reader can read again on
    replay. An append fails if the Stream's size cap would make the Store drop them. The append
    succeeds after the reader advances, such as when it continues as new. The server suggests
    continue-as-new to a reader that holds half of the cap.
    """


class StreamUnsupportedError(StreamError):
    """The Store lacks a capability that the call needs.

    The message ends with the name of the capability. Workflow code gets
    :py:class:`StreamConfigError` for it, since it's a setup error.
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

    It isn't a :py:class:`StreamError`, so ``except StreamError`` doesn't catch it. Workflow
    code also gets it for a backend that lacks a capability, such as a native server with
    ``frontend.enableDirectNexusCalls`` off. In Workflow code it fails the Workflow Task. An
    Activity that raises it retries.
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
    "stream_too_large": StreamTooLargeError,
    "stream_unsupported": StreamUnsupportedError,
    "stream_not_owner": StreamNotOwnerError,
    "seq_divergent": StreamSeqDivergentError,
    # The native server's cap on Workflow readers. Readers finish, so a retry can succeed.
    "stream_exhausted": StreamStorageError,
}


def from_call_error(err: Exception) -> StreamError:
    """Turn a failed call of the crate's runtime into the Stream error for its reason.

    ``err.args`` are the gRPC code, the message and the encoded ``StreamErrorDetails``.
    """
    code, message, details = err.args
    reason = StreamErrorDetails.FromString(details).reason if details else ""
    return _BY_REASON.get(reason, StreamError)(
        message, reason=reason, code=temporalio.service.RPCStatusCode(code)
    )


_ERROR_CLASSES: list[type[StreamError]] = [StreamError, *_BY_REASON.values()]
_BY_NAME: dict[str, type[StreamError]] = {cls.__name__: cls for cls in _ERROR_CLASSES}


def from_application_error(
    err: temporalio.exceptions.ApplicationError,
) -> StreamError | None:
    """The Stream error in the application error of a failed call, if any.

    A Stream error's type is its class name, and its first detail is its reason. A server can
    give the reason as the type, or at the start of the message, such as ``stream_closed: ...``.
    """
    token = err.message.split(":", 1)[0].strip()
    reason = err.details[0] if err.details and isinstance(err.details[0], str) else ""
    cls = _BY_NAME.get(err.type or "")
    if cls is None:
        reason = next(
            (name for name in (err.type or "", token) if name in _BY_REASON), ""
        )
        cls = _BY_REASON.get(reason)
    if cls is None:
        return None
    return cls(err.message, reason=reason)


def from_reason(reason: str, message: str) -> StreamError:
    """The Stream error for a reason token."""
    return _BY_REASON.get(reason, StreamError)(message, reason=reason)


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


def in_workflow(err: BaseException) -> BaseException:
    """The error that Workflow code raises for the Stream error of a failed call.

    A missing capability is not in History, so it fails the Workflow Task, and a Worker or a
    server with that capability retries it.
    """
    if isinstance(err, StreamUnsupportedError):
        return StreamConfigError(err.message)
    return err
