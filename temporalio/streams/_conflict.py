"""The conflict policy of a Stream create."""

from __future__ import annotations

from enum import IntEnum

import temporalio.api.enums.v1


class StreamIDConflictPolicy(IntEnum):
    """How a create of a Stream whose id already exists is handled.

    ``FAIL`` raises a :py:class:`temporalio.streams.StreamError` with ``reason``
    ``STREAM_ERROR_REASON_ALREADY_EXISTS``. ``USE_EXISTING`` returns the existing Stream and
    ignores the new options. See
    :py:class:`temporalio.api.enums.v1.StreamIdConflictPolicy`.

    .. warning::
        This API is experimental.
    """

    FAIL = int(
        temporalio.api.enums.v1.StreamIdConflictPolicy.STREAM_ID_CONFLICT_POLICY_FAIL
    )
    USE_EXISTING = int(
        temporalio.api.enums.v1.StreamIdConflictPolicy.STREAM_ID_CONFLICT_POLICY_USE_EXISTING
    )
