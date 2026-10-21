"""Errors of temporalio.contrib.streams."""

from __future__ import annotations

import temporalio.exceptions
import temporalio.worker
from temporalio.contrib.streams.proto.v1 import StreamErrorDetails


class StreamError(temporalio.exceptions.TemporalError):
    """Base of every Stream error.

    .. warning::
        This API is experimental.
    """

    def __init__(self, message: str, *, reason: str = "") -> None:
        """Create a Stream error."""
        super().__init__(message)
        self._reason = reason

    @property
    def reason(self) -> str:
        """Token of the cause, such as ``stream_closed``. Empty if the error has none."""
        return self._reason


class StreamNotFoundError(StreamError):
    """The Stream does not exist, or it was not created in time."""


class StreamClosedError(StreamError):
    """The Stream is closed, so it takes no more Records."""


class StreamExpiredError(StreamError):
    """Retention removed the Stream."""


class StreamFencedError(StreamError):
    """A newer attempt of the same Producer wrote to the Stream, so this attempt can't."""


class StreamStorageError(StreamError):
    """The Store failed."""


class StreamOffsetError(StreamError):
    """The offset is past the newest one plus one, or Retention removed it."""


class StreamBufferFullError(StreamError):
    """The writer's buffer is full. Flush, or publish less often."""


class StreamTransportMissingError(StreamError):
    """No plugin carries Stream calls, such as ``RedisStreamPlugin``."""


class ReadOnlyContextError(StreamError):
    """Workflow code that must not change state, such as a query handler, wrote to a Stream."""


_BY_REASON: dict[str, type[StreamError]] = {
    "stream_not_found": StreamNotFoundError,
    "stream_closed": StreamClosedError,
    "stream_expired": StreamExpiredError,
    "stream_fenced": StreamFencedError,
    "stream_storage": StreamStorageError,
    "stream_offset": StreamOffsetError,
    "stream_buffer_full": StreamBufferFullError,
}


def from_extension_error(err: temporalio.worker.ExtensionError) -> StreamError:
    """Turn a failed extension call into the Stream error for its reason."""
    reason = ""
    if err.details:
        reason = StreamErrorDetails.FromString(err.details).reason
    return _BY_REASON.get(reason, StreamError)(err.message, reason=reason)
