"""The close status of a Stream."""

from __future__ import annotations

from enum import IntEnum

import temporalio.api.enums.v1


class StreamCloseStatus(IntEnum):
    """Why a Stream closed, on a :py:class:`temporalio.streams.Closed` item.

    The memory and Redis backends give ``CLOSED``. Native Streams add their own statuses later.
    See :py:class:`temporalio.api.enums.v1.StreamCloseStatus`.

    .. warning::
        This API is experimental.
    """

    UNSPECIFIED = int(
        temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_UNSPECIFIED
    )
    CLOSED = int(temporalio.api.enums.v1.StreamCloseStatus.STREAM_CLOSE_STATUS_CLOSED)
