"""Streams that a Standalone Activity owns.

A Standalone Activity creates a Stream with :py:func:`create_stream`, and a client finds it
with :py:meth:`temporalio.contrib.streams.StreamClient.activity_stream`. The server closes the
Stream when the Activity ends, with its status and result, so only the native backend keeps
one.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TypeVar

import temporalio.activity
import temporalio.api.streamservice.v1 as stream_pb
from temporalio.contrib.streams import _activity, _backend, _errors, _ids
from temporalio.contrib.streams._activity_stream import ActivityStream
from temporalio.contrib.streams._defn import definition_of
from temporalio.contrib.streams._handle import Creating
from temporalio.contrib.streams.system import models

E = TypeVar("E")

__all__ = ["create_stream"]


def create_stream(
    definition: type[E],
    *,
    retention: timedelta | None = None,
    auto_close: timedelta | None = None,
    backend: str | None = None,
) -> Creating[ActivityStream[E]]:
    """Create a Stream that this Standalone Activity owns.

    Its id is ``<activity id>:<activity run id>:<name>``, so a client finds it from the
    Activity handle with no call. The Stream goes on across the Activity's retries, and the
    server closes it when the Activity ends. Await the call for the handle, or use it in
    ``async with``, which closes the Stream at the end of the block.

    Args:
        definition: The Stream definition class. Its name is the Stream name.
        retention: How long the Records stay after the close. Defaults to the backend's.
        auto_close: Close the Stream with no result when no Record was appended for this
            long. Defaults to never.
        backend: The name of the backend. Defaults to the Worker's default backend.

    Raises:
        StreamConfigError: The Activity runs for a Workflow, which owns its Streams, or the
            backend keeps no Stream of a Standalone Activity, such as Redis. No call is made.
    """
    info = temporalio.activity.info()
    if info.in_workflow:
        raise _errors.StreamConfigError(
            "Only a Standalone Activity owns Streams. An Activity of a Workflow writes to "
            "the Streams that it receives."
        )
    attempt = _activity.current.get()
    streams = attempt.get(backend) if attempt is not None else None
    if streams is None:
        raise _errors.StreamConfigError(
            f"No Stream plugin of the backend {backend or 'by default'} runs this Activity"
        )
    if not streams.activity_streams:
        raise _errors.StreamConfigError(
            f"The backend {streams.backend_name} keeps no Stream of a Standalone Activity, "
            "since nothing would close it when the Activity ends. Use the native backend."
        )
    run_id = info.activity_run_id or ""
    stream_id = _ids.scoped_stream_id(
        info.activity_id, run_id, definition_of(definition).name
    )

    async def create() -> ActivityStream[E]:
        owner = models._ProducerTransferTypeConverter().from_transfer_type(
            streams.producer, models.Producer
        )
        request = models.CreateStreamRequest(
            stream_id=stream_id,
            retention=retention,
            auto_close=auto_close,
            owner=owner,
            ref=models.StreamRef(
                id=stream_id, activity_id=info.activity_id, activity_run_id=run_id
            ),
        )
        # The server checks the token against the Activity's current attempt, so a caller
        # can't put a Stream into an Activity it isn't.
        with _backend.with_task_token(streams.task_token):
            response = await streams.backend.service(
                streams.namespace, streams.converter
            ).create_stream(request)
        if response.status == stream_pb.StreamStatus.STREAM_STATUS_CLOSED:
            raise _errors.StreamClosedError(
                f"The Stream {stream_id} was created and closed before",
                reason="stream_closed",
            )
        return ActivityStream(
            id=stream_id,
            activity_id=info.activity_id,
            activity_run_id=run_id,
            backend=streams.backend_name or None,
            definition=definition,
        )

    return Creating(create)
