"""Following a stream and the streams its links open, in one order.

Core's ``Follow`` keeps the order. It reads the root and each open linked
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
from temporalio.contrib.streams._ref import StreamRef, from_address, owner_context
from temporalio.contrib.streams._wire import from_read, from_wire_link
from temporalio.converter import DataConverter

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
    Continue-as-New. ``cursor`` is the root's cursor, and ``state`` Core's
    follow state in base64, which holds the cursor of each open linked
    stream.
    """

    cursor: str
    state: str = ""


@enum.unique
class LinkStateKind(enum.IntEnum):
    """What happened to a link, as a follower reports it."""

    OPENED = 1
    """The follower started reading the linked stream."""
    DONE = 2
    """The linked stream ended, or reached the ``JOIN`` point."""
    UNAVAILABLE = 3
    """The linked stream can't be read. The follow goes on without it."""
    SKIPPED = 4
    """The follower did not open the link. ``reason`` says why."""


@dataclass(frozen=True)
class LinkState:
    """A follower's report on one link."""

    kind: LinkStateKind
    link: StreamLink
    result: Any = None
    """On ``DONE``, the linked stream's close result, if any."""
    error: BaseException | None = None
    """On ``DONE``, the failure the linked stream closed with. On
    ``UNAVAILABLE``, why it can't be read."""
    reason: str = ""
    """On ``SKIPPED``: ``depth``, ``limit``, ``cycle``, ``open`` or ``endpoint``."""


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

    Set only on the last item of each batch, since Core gives the follow
    state at each batch end. Other items carry ``None``. To resume from an
    earlier item, resume from the previous batch end, which gives that
    item and the rest of its batch again."""


@dataclass
class _Source:
    ref: StreamRef
    depth: int
    converter: DataConverter


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
    sources = {0: _Source(handle.ref.with_topic(topic), 0, handle._converter)}
    while True:
        response: FollowResponse = await call(service.follow(request))
        for source in response.sources:
            ref = from_address(source.stream)
            sources[source.number] = _Source(
                ref,
                source.depth,
                handle._client.data_converter.with_context(
                    owner_context(
                        source.stream.namespace or handle._client.namespace,
                        ref.workflow_id,
                        ref.activity_id,
                    )
                ),
            )
        last = len(response.records) - 1
        for index, followed in enumerate(response.records):
            source = sources[followed.source]
            record = link_state = None
            if followed.HasField("record"):
                record = await from_read(
                    source.converter,
                    followed.record,
                    result_type if followed.source == 0 else None,
                )
                if followed.source == 0:
                    cursor = record.cursor.token
            else:
                link_state = await _link_state(handle, followed.link_state)
            position = None
            if index == last:
                position = FollowPosition(
                    cursor, base64.b64encode(response.state).decode()
                )
            yield FollowedItem(source.ref, source.depth, record, link_state, position)
        if response.done:
            if response.HasField("close_failure"):
                raise await handle._converter.decode_failure(response.close_failure)
            return
        request.after = cursor
        request.state = response.state


async def _link_state(handle: StreamHandle, wire: WireLinkState) -> LinkState:
    link = from_wire_link(wire.link)
    converter = handle._converter
    if link.ref is not None:
        # The outcome is the linked stream's, so it decodes in that owner's context.
        converter = handle._client.data_converter.with_context(
            owner_context(
                link.namespace or handle._client.namespace,
                link.ref.workflow_id,
                link.ref.activity_id,
            )
        )
    result: Any = None
    error: BaseException | None = None
    if wire.HasField("close_result"):
        body = await decode_body(converter, wire.close_result)
        result = converter.payload_converter.from_payloads([body])[0]
    if wire.HasField("close_failure"):
        error = await converter.decode_failure(wire.close_failure)
    elif wire.HasField("error"):
        error = error_from_failure(wire.error)
    return LinkState(
        kind=LinkStateKind(wire.kind),
        link=link,
        result=result,
        error=error,
        reason=wire.reason,
    )
