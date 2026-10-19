"""Following a stream and the streams its links open, in one order.

The stream runtime's ``Follow`` keeps the order. It reads the root and each open linked
stream, holds a child's records until its ``OPEN`` and a parent's records
after a ``JOIN`` until the child reached the join point, and reports each
link's state. What stays here is what needs the user's types: the codec of
each source, converting values, and the position a follower resumes from.
"""

from __future__ import annotations

import base64
import enum
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from temporalio.bridge.proto.streams import FollowRequest, FollowResponse
from temporalio.bridge.proto.streams import LinkState as WireLinkState
from temporalio.contrib.streams._body import decode_body
from temporalio.contrib.streams._errors import error_from_failure
from temporalio.contrib.streams._plugin import call
from temporalio.contrib.streams._record import Cursor, StreamLink, StreamRecord
from temporalio.contrib.streams._ref import StreamRef, from_address
from temporalio.contrib.streams._wire import from_wire_link, to_record
from temporalio.converter import (
    DataConverter,
    PayloadConverter,
    WorkflowSerializationContext,
)

if TYPE_CHECKING:
    from temporalio.contrib.streams._handles import StreamHandle

__all__ = [
    "FollowPosition",
    "FollowedItem",
    "LinkState",
    "LinkStateKind",
]

T = TypeVar("T")

_FOLLOW_WAIT = timedelta(seconds=30)
_MAX_RECORDS = 100


@dataclass(frozen=True)
class FollowPosition:
    """Where a follower stands, in the root stream and in each open link.

    Pass it as ``after=`` to resume. It is plain data, so it can cross
    Continue-as-New. ``cursor`` is the root's cursor, and ``state`` the stream runtime's
    follow state in base64, which holds the cursor of each open linked
    stream.
    """

    cursor: str
    state: str = ""


@enum.unique
class LinkStateKind(enum.IntEnum):
    """What happened to a link, as a follower reports it."""

    OPENED = 1
    """The follower started reading the linked stream.

    With ``reason`` ``resumed``, a later attempt of the producer wrote the same ``OPEN``
    again, and the follower reads the withdrawn segment on from where it paused.
    """
    DONE = 2
    """The linked stream ended, or reached the ``JOIN`` point.

    With ``reason`` ``withdrawn``, the attempt that wrote the ``OPEN`` was superseded. The
    segment pauses, and the same ``OPEN`` from a later attempt resumes it.
    """
    UNAVAILABLE = 3
    """The linked stream can't be read. The follow goes on without it."""
    SKIPPED = 4
    """The follower did not open the link. ``reason`` says why."""


@dataclass(frozen=True)
class LinkState:
    """A follower's report on one link."""

    kind: LinkStateKind
    link: StreamLink
    error: BaseException | None = None
    """On ``UNAVAILABLE``, why the linked stream can't be read."""
    reason: str = ""
    """On ``SKIPPED``: ``depth``, ``limit``, ``cycle``, ``open`` or ``endpoint``.

    On ``DONE``, ``withdrawn`` or empty. On ``OPENED``, ``resumed`` or empty.
    """


@dataclass(frozen=True)
class FollowedItem(Generic[T]):
    """One record or link report of a follow, with the stream it came from.

    Exactly one of ``record`` and ``link_state`` is set.
    """

    source: StreamRef
    """The stream the item belongs to."""
    depth: int
    """How many links away from the root the source is. The root is 0."""
    record: StreamRecord[T] | None
    link_state: LinkState | None
    position: FollowPosition | None
    """Where to resume after this item.

    Set only on the last item of each batch, since the stream runtime gives the follow
    state at each batch end. Other items carry ``None``. To resume from an
    earlier item, resume from the previous batch end, which gives that
    item and the rest of its batch again."""


@dataclass(frozen=True)
class FollowSourceInfo:
    """What a follow knows of one source, by its number."""

    ref: StreamRef
    depth: int


def update_sources(
    sources: dict[int, FollowSourceInfo], response: FollowResponse
) -> None:
    """Add the sources ``response`` names to ``sources``."""
    for source in response.sources:
        sources[source.number] = FollowSourceInfo(
            from_address(source.stream), source.depth
        )


async def decode_follow(
    converter: DataConverter,
    namespace: str,
    root: StreamRef,
    response: FollowResponse,
) -> None:
    """Run the codec over every body in ``response``, in place.

    Each source decodes in the context of the Workflow that owns it, as that
    Workflow encoded it.
    """
    contexts: dict[int, DataConverter] = {
        0: converter.with_context(
            WorkflowSerializationContext(
                namespace=namespace, workflow_id=root.workflow_id
            )
        )
    }
    for source in response.sources:
        stream = source.stream
        contexts[source.number] = converter.with_context(
            WorkflowSerializationContext(
                namespace=stream.namespace or namespace, workflow_id=stream.workflow_id
            )
        )
    for followed in response.records:
        if not followed.HasField("record"):
            continue
        stored = followed.record.stored
        if followed.record.HasField("stored") and stored.HasField("body"):
            context = contexts.get(followed.source, contexts[0])
            stored.body.CopyFrom(await decode_body(context, stored.body))


def follow_items(
    payload_converter: PayloadConverter,
    sources: dict[int, FollowSourceInfo],
    response: FollowResponse,
    result_type: type | None,
    cursor: str,
) -> tuple[list[FollowedItem[Any]], str]:
    """The items of a decoded ``response``, and the root cursor after them.

    The last item carries the position after the batch.
    """
    items: list[FollowedItem[Any]] = []
    for followed in response.records:
        source = sources[followed.source]
        record = link_state = None
        if followed.HasField("record"):
            record = to_record(
                payload_converter,
                followed.record,
                result_type if followed.source == 0 else None,
            )
            if followed.source == 0:
                cursor = record.cursor.token
        else:
            link_state = _link_state(followed.link_state)
        items.append(FollowedItem(source.ref, source.depth, record, link_state, None))
    if items:
        position = FollowPosition(cursor, base64.b64encode(response.state).decode())
        last = items[-1]
        items[-1] = FollowedItem(
            last.source, last.depth, last.record, last.link_state, position
        )
    return items, cursor


def _link_state(wire: WireLinkState) -> LinkState:
    return LinkState(
        kind=LinkStateKind(wire.kind),
        link=from_wire_link(wire.link),
        error=error_from_failure(wire.error) if wire.HasField("error") else None,
        reason=wire.reason,
    )


async def follow(
    handle: StreamHandle,
    topic: str,
    after: Cursor | FollowPosition,
    max_depth: int,
    max_open: int,
    result_type: type | None,
) -> AsyncGenerator[FollowedItem[Any], None]:
    """See :meth:`temporalio.contrib.streams.StreamHandle.follow`."""
    service = await handle._plugin._service_for(handle._client)
    if isinstance(after, FollowPosition):
        cursor, state = after.cursor, base64.b64decode(after.state)
    else:
        cursor, state = after.token, b""
    request = FollowRequest(
        stream=handle._address(topic, run_id=await handle._read_run_id()),
        after=cursor,
        state=state,
        max_records=_MAX_RECORDS,
        max_depth=max_depth,
        max_open=max_open,
    )
    request.wait.FromTimedelta(_FOLLOW_WAIT)
    root = handle.ref.with_topic(topic)
    sources = {0: FollowSourceInfo(root, 0)}
    converter = handle._client.data_converter
    namespace = handle._client.namespace
    while True:
        response: FollowResponse = await call(service.follow(request))
        update_sources(sources, response)
        await decode_follow(converter, namespace, root, response)
        items, cursor = follow_items(
            converter.payload_converter,
            sources,
            response,
            result_type,
            cursor,
        )
        for item in items:
            yield item
        if response.done:
            return
        request.state = response.state
