"""How a record crosses to the stream runtime and back: the proto is the record.

``temporal.sdk.streams.v1.StreamRecord`` is the stored format on every
store, and the stream runtime builds and parses it. ``body`` is the user's payload,
produced and consumed through the payload converter, so a pre-encoded
:class:`temporalio.common.RawValue` passes through untouched.
"""

from __future__ import annotations

from typing import Any

import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams import ReadRecord
from temporalio.bridge.proto.streams.v1 import StreamLink as WireLink
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.contrib.streams._body import decode_body
from temporalio.contrib.streams._errors import StreamRecordError
from temporalio.contrib.streams._record import (
    Cursor,
    LinkEvent,
    RecordKind,
    StreamLink,
    StreamRecord,
    Supersession,
)
from temporalio.contrib.streams._ref import from_reference, to_reference

__all__ = [
    "RUN_ID_KEY",
    "WireRecord",
    "decode_or_mark",
    "from_read",
    "from_wire_link",
    "to_record",
    "to_wire",
    "to_wire_link",
]

# The most bytes a link's name and its other strings may take, as the stream runtime checks them.
_MAX_LINK_NAME_BYTES = 256
_MAX_LINK_FIELD_BYTES = 1024

RUN_ID_KEY = "temporal.io/run-id"
"""The record metadata key the producing run id is stored under.

The stream runtime stamps it on every record the owning Workflow publishes, because a
stream follows the Workflow's run chain and a reader needs the run to tell a
reset branch or a successor run apart. Its value is a payload with
``encoding`` ``binary/plain`` whose data is the run id.
"""

# The encoding of a body the codec could not decode before Workflow code got
# it. The data is the codec's error, so the reader raises it for this record
# only, live and on replay.
_UNDECODABLE_ENCODING = b"temporal.io/stream-undecodable"


def to_wire(
    converter: temporalio.converter.PayloadConverter,
    *,
    topic: str,
    kind: RecordKind,
    value: Any = None,
    producer_id: str = "",
    attempt: int = 0,
    sequence: int = 0,
    link: StreamLink | None = None,
) -> WireRecord:
    """The record as the converter produced it, before the codec.

    A ``DATA`` record carries a body, and the converter encodes ``value``
    into it. A ``LINK`` record carries ``link``, and a body only when
    ``value`` is not ``None``.

    Raises:
        ValueError: ``kind`` is ``SUPERSEDED`` or ``UNSPECIFIED``, which no
            writer stores, or a ``LINK`` has no valid ``link``.
    """
    if kind not in (RecordKind.DATA, RecordKind.FINISH, RecordKind.LINK):
        raise ValueError(
            f"a writer stores DATA, FINISH or LINK records, not {kind.name}"
        )
    record = WireRecord(
        topic=topic,
        kind=StreamRecordKind.ValueType(int(kind)),
        producer_id=producer_id,
        attempt=attempt,
        sequence=sequence,
    )
    if kind is RecordKind.LINK:
        if link is None:
            raise ValueError("a LINK record needs a link")
        record.link.CopyFrom(to_wire_link(link))
        if value is not None:
            record.body.CopyFrom(converter.to_payloads([value])[0])
    elif kind is RecordKind.DATA:
        record.body.CopyFrom(converter.to_payloads([value])[0])
    return record


def to_wire_link(link: StreamLink) -> WireLink:
    """``link`` as the stream runtime stores it.

    Raises:
        ValueError: The name is empty or over 256 bytes, a cursor, the
            namespace or the endpoint is over 1024 bytes, an ``OPEN`` has no
            ref or has ``until``, or a ``JOIN`` has a ref or ``after``.
    """
    if not link.name or len(link.name.encode()) > _MAX_LINK_NAME_BYTES:
        raise ValueError(
            f"a link name needs 1 to {_MAX_LINK_NAME_BYTES} bytes, got {link.name!r}"
        )
    if (link.event is LinkEvent.OPEN) != (link.ref is not None):
        raise ValueError("an OPEN link needs a ref, and a JOIN link has none")
    if link.event is LinkEvent.OPEN and link.until is not None:
        raise ValueError("an OPEN link has no until")
    if link.event is LinkEvent.JOIN and link.after is not None:
        raise ValueError("a JOIN link has no after")
    wire = WireLink(
        event=WireLink.Event.ValueType(int(link.event)),
        name=link.name,
        after=link.after.token if link.after is not None else "",
        until=link.until.token if link.until is not None else "",
        namespace=link.namespace,
        endpoint=link.endpoint,
    )
    for name, text in (
        ("after", wire.after),
        ("until", wire.until),
        ("namespace", wire.namespace),
        ("endpoint", wire.endpoint),
    ):
        if len(text.encode()) > _MAX_LINK_FIELD_BYTES:
            raise ValueError(
                f"a link's {name} takes at most {_MAX_LINK_FIELD_BYTES} bytes"
            )
    if link.ref is not None:
        wire.stream.CopyFrom(to_reference(link.ref))
    return wire


def from_wire_link(wire: WireLink) -> StreamLink:
    """The link the stream runtime stored in ``wire``."""
    return StreamLink(
        event=LinkEvent(wire.event),
        ref=from_reference(wire.stream) if wire.HasField("stream") else None,
        name=wire.name,
        after=Cursor(wire.after) if wire.after else None,
        until=Cursor(wire.until) if wire.until else None,
        namespace=wire.namespace,
        endpoint=wire.endpoint,
    )


async def from_read(
    converter: temporalio.converter.DataConverter,
    read: ReadRecord,
    result_type: type | None,
) -> StreamRecord[Any]:
    """The record a reader yields for one record of the stream runtime's answer.

    Raises:
        StreamRecordError: The body did not decode or convert, or the record
            holds a kind this SDK does not know.
    """
    if read.HasField("stored") and read.stored.HasField("body"):
        try:
            body = await decode_body(converter, read.stored.body)
        except Exception as error:
            raise StreamRecordError(
                f"stream record at {read.cursor} could not be decoded: {error}",
                Cursor(read.cursor),
            ) from error
        decoded = ReadRecord()
        decoded.CopyFrom(read)
        decoded.stored.body.CopyFrom(body)
        read = decoded
    return to_record(converter.payload_converter, read, result_type)


async def decode_or_mark(
    converter: temporalio.converter.DataConverter, read: ReadRecord
) -> None:
    """Run the codec on the body of ``read`` in place, for a reader in Workflow code.

    A body that does not decode is replaced with one that names the error,
    which :func:`to_record` raises as a :class:`StreamRecordError` for that
    record alone.
    """
    if not (read.HasField("stored") and read.stored.HasField("body")):
        return
    try:
        body = await decode_body(converter, read.stored.body)
    except Exception as error:
        body = Payload(
            metadata={"encoding": _UNDECODABLE_ENCODING}, data=str(error).encode()
        )
    read.stored.body.CopyFrom(body)


def to_record(
    payload_converter: temporalio.converter.PayloadConverter,
    read: ReadRecord,
    result_type: type | None,
) -> StreamRecord[Any]:
    """The record a reader yields for a record whose body the codec already decoded.

    Workflow code reads with it, since a Local Activity ran the codec.

    Raises:
        StreamRecordError: The body did not convert, or the record holds a
            kind this SDK does not know.
    """
    cursor = Cursor(read.cursor)
    if read.HasField("superseded"):
        superseded = read.superseded
        return StreamRecord(
            kind=RecordKind.SUPERSEDED,
            cursor=cursor,
            topic=superseded.topic,
            producer_id=superseded.producer_id,
            attempt=superseded.attempt,
            supersession=Supersession(
                superseded.producer_id, superseded.previous_attempt, superseded.attempt
            ),
        )
    wire = read.stored
    if wire.body.metadata.get("encoding") == _UNDECODABLE_ENCODING:
        raise StreamRecordError(
            f"stream record at {cursor} could not be decoded: "
            f"{wire.body.data.decode(errors='replace')}",
            cursor,
        )
    try:
        kind = RecordKind(wire.kind)
        if kind is RecordKind.SUPERSEDED:
            raise ValueError("a SUPERSEDED record is synthesized, never stored")
        value: Any = None
        if kind in (
            RecordKind.DATA,
            RecordKind.UNSPECIFIED,
            RecordKind.LINK,
        ) and wire.HasField("body"):
            hints = (
                [result_type]
                if result_type is not None and kind is not RecordKind.LINK
                else None
            )
            value = payload_converter.from_payloads([wire.body], hints)[0]
        link = from_wire_link(wire.link) if kind is RecordKind.LINK else None
    except Exception as error:
        raise StreamRecordError(
            f"stream record at {cursor} could not be decoded: {error}", cursor
        ) from error
    return StreamRecord(
        kind=RecordKind.DATA if kind is RecordKind.UNSPECIFIED else kind,
        cursor=cursor,
        topic=wire.topic,
        producer_id=wire.producer_id,
        attempt=wire.attempt,
        sequence=wire.sequence,
        run_id=wire.metadata[RUN_ID_KEY].data.decode()
        if RUN_ID_KEY in wire.metadata
        else "",
        value=value,
        link=link,
        stale=read.stale,
    )
