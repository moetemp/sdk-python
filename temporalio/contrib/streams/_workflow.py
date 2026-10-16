"""Streams inside Workflow code.

Reading a stream inside a Workflow is not part of this release.
"""

from __future__ import annotations

from typing import Any

from temporalio.contrib.streams._errors import StreamUnsupportedError

__all__ = ["workflow_reader"]


def workflow_reader(*_args: Any, **_kwargs: Any) -> Any:
    """Reading a stream inside a Workflow is not supported in this release.

    Read from an Activity or a client with
    :func:`temporalio.contrib.streams.activity_handle` or
    :func:`temporalio.contrib.streams.get_stream_handle`, and send the
    Workflow what it needs, for example as a Signal.

    Raises:
        StreamUnsupportedError: Always.
    """
    raise StreamUnsupportedError(
        "reading a stream inside a Workflow is not supported in this release; "
        "read it from an Activity or a client and send the Workflow what it "
        "needs, for example as a Signal"
    )
