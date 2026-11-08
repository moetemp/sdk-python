"""The close status of a Stream."""

from __future__ import annotations

from enum import IntEnum

import temporalio.api.enums.v1


class StreamCloseStatus(IntEnum):
    """Why a Stream closed, on a :py:class:`temporalio.streams.Closed` item.

    The memory and Redis backends give ``CLOSED``. The owner statuses and a close failure come
    from the native runtime. See :py:class:`temporalio.api.enums.v1.StreamCloseStatus`.

    .. warning::
        This API is experimental.
    """

    UNSPECIFIED = int(
        temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_UNSPECIFIED
    )
    CLOSED = int(temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_CLOSED)
    OWNER_COMPLETED = int(
        temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_OWNER_COMPLETED
    )
    OWNER_FAILED = int(
        temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_OWNER_FAILED
    )
    OWNER_CANCELED = int(
        temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_OWNER_CANCELED
    )
    OWNER_TERMINATED = int(
        temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_OWNER_TERMINATED
    )
    OWNER_TIMED_OUT = int(
        temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_OWNER_TIMED_OUT
    )
    IDLE = int(temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_IDLE)
