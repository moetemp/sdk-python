"""Records, their stored form, and what lang owes a record's body."""

from __future__ import annotations

import uuid
from collections.abc import Sequence

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams import ReadRecord, Supersession
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.common import RawValue
from temporalio.contrib.streams import (
    Cursor,
    LinkEvent,
    RecordKind,
    StreamLink,
    StreamRecordError,
    StreamRef,
    StreamUnsupportedError,
)
from temporalio.contrib.streams import Supersession as ReportedSupersession
from temporalio.contrib.streams._body import (
    batch_digest,
    content_hash,
    decode_body,
    encode_bodies,
)
from temporalio.contrib.streams._ref import from_reference, to_reference
from temporalio.contrib.streams._wire import RUN_ID_KEY, WireRecord, from_read, to_wire
from temporalio.converter import DataConverter, PayloadCodec

CONVERTER = DataConverter.default


def stored(wire: WireRecord, cursor: str = "c", *, stale: bool = False) -> ReadRecord:
    return ReadRecord(cursor=cursor, stored=wire, stale=stale)


async def test_a_record_reads_back_as_it_was_written():
    wire = to_wire(
        CONVERTER.payload_converter,
        topic="out",
        kind=RecordKind.DATA,
        value={"n": 1},
        producer_id="p",
        attempt=2,
        sequence=3,
    )
    record = await from_read(CONVERTER, stored(wire, "c1", stale=True), dict)
    assert record.kind is RecordKind.DATA
    assert record.value == {"n": 1}
    assert record.cursor == Cursor("c1")
    assert (record.producer_id, record.attempt, record.sequence) == ("p", 2, 3)
    assert record.stale

    finish = to_wire(CONVERTER.payload_converter, topic="out", kind=RecordKind.FINISH)
    assert not finish.HasField("body")
    assert (await from_read(CONVERTER, stored(finish), None)).kind is RecordKind.FINISH

    unset = WireRecord(
        topic="out", body=CONVERTER.payload_converter.to_payloads([1])[0]
    )
    assert (await from_read(CONVERTER, stored(unset), int)).kind is RecordKind.DATA


async def test_the_run_id_core_stamps_reaches_the_reader():
    wire = to_wire(
        CONVERTER.payload_converter, topic="out", kind=RecordKind.DATA, value=1
    )
    assert (await from_read(CONVERTER, stored(wire), int)).run_id == ""
    wire.metadata[RUN_ID_KEY].CopyFrom(
        Payload(metadata={"encoding": b"binary/plain"}, data=b"run-1")
    )
    assert (await from_read(CONVERTER, stored(wire), int)).run_id == "run-1"


async def test_a_supersession_reads_as_the_record_core_synthesized():
    read = ReadRecord(
        cursor="c2",
        superseded=Supersession(
            topic="t", producer_id="model", previous_attempt=1, attempt=2
        ),
    )
    record = await from_read(CONVERTER, read, int)
    assert record.kind is RecordKind.SUPERSEDED
    assert record.cursor == Cursor("c2")
    assert (record.topic, record.producer_id, record.attempt) == ("t", "model", 2)
    assert record.supersession == ReportedSupersession("model", 1, 2)
    assert record.value is None


def test_no_writer_stores_a_synthesized_kind():
    for kind in (RecordKind.SUPERSEDED, RecordKind.UNSPECIFIED):
        with pytest.raises(ValueError):
            to_wire(CONVERTER.payload_converter, topic="out", kind=kind)


async def test_a_raw_value_passes_through_untouched():
    raw = Payload(metadata={"encoding": b"binary/custom"}, data=b"\x00\x01")
    wire = to_wire(
        CONVERTER.payload_converter,
        topic="out",
        kind=RecordKind.DATA,
        value=RawValue(raw),
    )
    assert wire.body == raw
    record = await from_read(CONVERTER, stored(wire), RawValue)
    assert isinstance(record.value, RawValue)
    assert record.value.payload == raw


async def test_a_record_that_does_not_decode_raises_with_its_cursor():
    bad = WireRecord(
        topic="t",
        kind=RecordKind.DATA.value,  # type: ignore[arg-type]
        body=Payload(metadata={"encoding": b"json/plain"}, data=b"{not json"),
    )
    with pytest.raises(StreamRecordError) as raised:
        await from_read(CONVERTER, stored(bad, "c1"), int)
    # The caller can resume past it on purpose.
    assert raised.value.cursor == Cursor("c1")
    unknown = WireRecord(topic="t", kind=7)  # type: ignore[arg-type]
    with pytest.raises(StreamRecordError):
        await from_read(CONVERTER, stored(unknown, "c2"), int)


def test_the_content_hash_is_the_plaintext_payload_hash():
    converter = CONVERTER.payload_converter
    one = converter.to_payloads([{"n": 1}])[0]
    assert content_hash(one) == content_hash(converter.to_payloads([{"n": 1}])[0])
    assert content_hash(one) != content_hash(converter.to_payloads([{"n": 2}])[0])
    # Core refuses a hash that isn't a SHA-256.
    assert len(content_hash(one)) == 32


def records(*values: str) -> list[WireRecord]:
    return [
        to_wire(CONVERTER.payload_converter, topic="t", kind=RecordKind.DATA, value=v)
        for v in values
    ]


def test_the_digest_is_length_delimited_over_the_batch():
    assert batch_digest(records("a", "b")) == batch_digest(records("a", "b"))
    assert batch_digest(records("a", "b")) != batch_digest(records("b", "a"))
    assert batch_digest(records("ab")) != batch_digest(records("a", "b"))


def test_the_digest_matches_what_every_sdk_takes():
    # The vector Core's tests hold, so a retry through another SDK still dedupes.
    batch = [
        to_wire(
            CONVERTER.payload_converter,
            topic="events",
            kind=RecordKind.DATA,
            value={"n": n},
            producer_id="p",
            attempt=1,
            sequence=n,
        )
        for n in (1, 2)
    ]
    assert batch_digest(batch).hex() == (
        "0499768af1856bc6fd19cf6cf90e28e971099f1680e8347d70bdb4b65273f30f"
    )


class NonceCodec(PayloadCodec):
    """Encrypts with a fresh nonce per call, so two encodings never match."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/nonce"},
                data=uuid.uuid4().bytes + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(p.data[16:]) for p in payloads]


async def test_the_codec_runs_on_the_body_and_never_on_the_hashes():
    converter = DataConverter(payload_codec=NonceCodec())
    plain = records("a")[0].body
    first, second = await encode_bodies(converter, [plain, plain])
    # The codec made the stored bytes differ, which the plaintext hash can't see.
    assert first != second
    assert first.metadata["encoding"] == b"binary/nonce"
    assert await decode_body(converter, first) == plain

    record = await from_read(
        converter,
        stored(
            WireRecord(
                topic="t", kind=StreamRecordKind.STREAM_RECORD_KIND_DATA, body=first
            )
        ),
        str,
    )
    assert record.value == "a"


@pytest.mark.parametrize(
    "link",
    [
        StreamLink(
            LinkEvent.OPEN,
            StreamRef.for_workflow_activity(
                "agent", run_id="run", activity_id="tool-1", topic="tokens"
            ),
            "tool-1",
            after=Cursor("c7"),
        ),
        StreamLink(
            LinkEvent.OPEN,
            StreamRef.for_workflow("child", chain_run_id="r"),
            "sub-3",
            namespace="other",
            endpoint="streams",
        ),
        StreamLink(LinkEvent.OPEN, StreamRef.for_activity("act"), "saa"),
        StreamLink(LinkEvent.JOIN, None, "sub-3", until=Cursor("c9")),
        StreamLink(LinkEvent.JOIN, None, "tool-1"),
    ],
    ids=["wfa-after", "workflow-remote", "activity", "join-until", "join-end"],
)
async def test_a_link_reads_back_as_it_was_written(link: StreamLink):
    wire = to_wire(
        CONVERTER.payload_converter,
        topic="events",
        kind=RecordKind.LINK,
        link=link,
        producer_id="p",
        attempt=1,
        sequence=4,
    )
    assert wire.kind == StreamRecordKind.STREAM_RECORD_KIND_LINK
    assert not wire.HasField("body")
    record = await from_read(CONVERTER, stored(wire, "c4"), None)
    assert record.kind is RecordKind.LINK
    assert record.link == link
    assert record.value is None


async def test_a_link_carries_an_optional_value_through_the_codec():
    link = StreamLink(LinkEvent.OPEN, StreamRef.for_workflow("child"), "sub")
    wire = to_wire(
        CONVERTER.payload_converter,
        topic="events",
        kind=RecordKind.LINK,
        link=link,
        value={"label": "search"},
    )
    record = await from_read(CONVERTER, stored(wire), dict)
    assert record.value == {"label": "search"}
    assert record.link == link


@pytest.mark.parametrize(
    "link, message",
    [
        (StreamLink(LinkEvent.OPEN, None, "x"), "needs a ref"),
        (StreamLink(LinkEvent.JOIN, StreamRef.for_workflow("wf"), "x"), "needs a ref"),
        (StreamLink(LinkEvent.OPEN, StreamRef.for_workflow("wf"), ""), "1 to 256"),
        (
            StreamLink(LinkEvent.OPEN, StreamRef.for_workflow("wf"), "x" * 257),
            "1 to 256",
        ),
    ],
)
def test_a_link_that_names_nothing_is_refused(link: StreamLink, message: str):
    with pytest.raises(ValueError, match=message):
        to_wire(CONVERTER.payload_converter, topic="t", kind=RecordKind.LINK, link=link)
    with pytest.raises(ValueError, match="needs a link"):
        to_wire(CONVERTER.payload_converter, topic="t", kind=RecordKind.LINK)


def test_a_ref_of_a_later_owner_kind_decodes_from_a_link():
    reference = to_reference(StreamRef.for_workflow("wf"))
    reference.owner_kind = 9  # type: ignore[assignment]
    ref = from_reference(reference)
    assert ref.kind == "kind_9"
    with pytest.raises(StreamUnsupportedError):
        ref._require_supported()
