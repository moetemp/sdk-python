"""Direct stream reads: the transports, the plugin and the codec step, with no server."""

from __future__ import annotations

import base64
import dataclasses
import json
from collections.abc import Sequence
from typing import Any

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.client import ClientConfig
from temporalio.contrib.streams import (
    StreamError,
    StreamRef,
    StreamReplayMismatchError,
    StreamReplayRangeLostError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._replay_reads import ReadChain
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    HttpTransport,
    ReadInput,
    ReadOutput,
    RecordWire,
    StreamDirectReads,
)
from temporalio.contrib.streams.nexus._direct import (
    _DirectReadCodec,
    answer_of,
    innermost_codec,
    positions_payload,
)
from temporalio.contrib.streams.nexus._generated import (
    ReadOutputReplay,
    ReadRangeInput,
)
from temporalio.contrib.streams.nexus._generated import client as generated_client
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.worker import ReplayerConfig
from temporalio.workflow import NondeterminismError

_STREAM = StreamRef.for_workflow("owner", topic="out")


class Base64Codec(PayloadCodec):
    """Stands in for an encryption codec."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/b64"},
                data=base64.b64encode(p.SerializeToString()),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(base64.b64decode(p.data)) for p in payloads]


class FakeTransport:
    """Answers each read_range from a list, and keeps what it was asked."""

    def __init__(self, *answers: ReadOutput | Exception) -> None:
        self.answers = list(answers)
        self.asked: list[ReadRangeInput] = []

    async def read(self, request: ReadInput) -> ReadOutput:
        raise AssertionError(f"a replay never reads, but read {request}")

    async def read_range(self, request: ReadRangeInput) -> ReadOutput:
        self.asked.append(request)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def record(token: str, text: str = "") -> RecordWire:
    return RecordWire(token=token, record=text.encode())


def live(
    *records: RecordWire, stored: int | None = None
) -> tuple[ReadRangeInput, ReadOutput]:
    """A live read's range and answer, as the read Activity records them."""
    answer = ReadOutput(
        records=list(records),
        next_token=records[-1].token if records else "c0",
        done=False,
        state=b"\x02",
        replay=ReadOutputReplay(
            chain_first_run_id="run-1",
            after_token="c0",
            stored_records=len(records) if stored is None else stored,
            records_digest=b"digest",
        ),
    )
    range = ReadRangeInput(
        stream=_STREAM,
        chain_first_run_id="run-1",
        after_token="c0",
        to_token=answer.next_token,
        stored_records=answer.replay.stored_records,  # type: ignore[union-attr]
        state=b"\x01",
        records_digest=b"digest",
        floor_holder="ns/run/1",
    )
    return range, answer


def client_config(converter: DataConverter) -> ClientConfig:
    """A client configuration with only ``converter``, as a plugin sees one."""
    config: Any = {"data_converter": converter}
    return config


def codec_of(
    plugin: StreamDirectReads, inner: PayloadCodec | None = None
) -> PayloadCodec:
    converter = dataclasses.replace(DataConverter.default, payload_codec=inner)
    codec = plugin.configure_client(client_config(converter))[
        "data_converter"
    ].payload_codec
    assert codec is not None
    return codec


async def turned_back(codec: PayloadCodec, payload: Payload) -> ReadOutput:
    """``payload`` as the Worker's codec hands it to Workflow code."""
    [decoded] = await codec.decode(await codec.encode([payload]))
    return answer_of(decoded)


def test_the_client_codec_is_wrapped_once():
    plugin = StreamDirectReads({})
    inner = Base64Codec()
    converter = dataclasses.replace(DataConverter.default, payload_codec=inner)
    config = plugin.configure_client(client_config(converter))
    config = plugin.configure_client(config)
    # Beside the store plugin's wrapper too, in either order.
    config = MemoryStreams().configure_client(config)
    config = plugin.configure_client(config)
    codec = config["data_converter"].payload_codec
    wrappers = 0
    while codec is not inner:
        wrappers += isinstance(codec, _DirectReadCodec)
        codec = getattr(codec, "inner")
    assert wrappers == 1
    assert innermost_codec(config["data_converter"].payload_codec) is inner


def test_a_replayer_codec_is_wrapped():
    plugin = StreamDirectReads({})
    config = plugin.configure_replayer(ReplayerConfig())
    codec = config["data_converter"].payload_codec  # type: ignore[reportTypedDictNotRequiredAccess]
    assert isinstance(codec, _DirectReadCodec)
    assert codec.inner is None


def test_an_unknown_endpoint_names_the_opt_out():
    with pytest.raises(StreamUnsupportedError, match="replay_from_store=False"):
        StreamDirectReads({}).transport("elsewhere")


def test_answer_of_a_result_no_codec_turned_back_names_the_plugin_and_the_opt_out():
    range, answer = live(record("c1", "a"))
    with pytest.raises(StreamUnsupportedError) as raised:
        answer_of(positions_payload("e", range, answer))
    assert "StreamDirectReads" in str(raised.value)
    assert "replay_from_store=False" in str(raised.value)


async def test_the_live_task_turns_the_result_back_from_the_cache():
    transport = FakeTransport()
    plugin = StreamDirectReads({"e": transport})
    range, answer = live(record("c1", "a"), record("c2", "b"))
    plugin._batches.put(range, answer)
    batch = await turned_back(
        codec_of(plugin, Base64Codec()), positions_payload("e", range, answer)
    )
    assert batch == answer
    assert transport.asked == []


async def test_a_replay_reads_the_range_again_through_the_endpoints_transport():
    range, answer = live(record("c1", "a"), record("c2", "b"))
    transport = FakeTransport(
        ReadOutput(records=answer.records, next_token="c2", done=False)
    )
    plugin = StreamDirectReads({"e": transport})
    batch = await turned_back(codec_of(plugin), positions_payload("e", range, answer))
    assert transport.asked == [range]
    # The rest comes from the marker, since read_range answers only the records.
    assert batch == answer


async def test_a_read_with_no_stored_record_replays_with_no_call():
    range, answer = live(stored=0)
    transport = FakeTransport()
    plugin = StreamDirectReads({"e": transport})
    batch = await turned_back(codec_of(plugin), positions_payload("e", range, answer))
    assert batch == answer
    assert transport.asked == []


async def test_a_replay_with_no_transport_for_the_endpoint_names_the_opt_out():
    range, answer = live(record("c1", "a"))
    plugin = StreamDirectReads({})
    with pytest.raises(StreamUnsupportedError, match="replay_from_store=False"):
        await turned_back(codec_of(plugin), positions_payload("e", range, answer))


async def test_other_records_on_replay_are_nondeterminism():
    range, answer = live(record("c1", "a"))
    plugin = StreamDirectReads(
        {"e": FakeTransport(StreamReplayMismatchError("digest"))}
    )
    with pytest.raises(NondeterminismError):
        await turned_back(codec_of(plugin), positions_payload("e", range, answer))


@pytest.mark.parametrize(
    "failure",
    [StreamReplayRangeLostError("trimmed"), StreamStorageError("store down")],
)
async def test_a_lost_range_or_an_outage_fails_the_task(failure: Exception):
    range, answer = live(record("c1", "a"))
    plugin = StreamDirectReads({"e": FakeTransport(failure)})
    with pytest.raises(type(failure)):
        await turned_back(codec_of(plugin), positions_payload("e", range, answer))


async def test_an_error_mark_replays_even_when_the_record_now_decodes():
    bad = RecordWire(token="c2", record=b"", error="codec: key not found")
    range, answer = live(record("c1", "a"), bad, record("c3", "c"))
    again = ReadOutput(
        records=[record("c1", "a"), record("c2", "b"), record("c3", "c")],
        next_token="c3",
        done=False,
    )
    plugin = StreamDirectReads({"e": FakeTransport(again)})
    batch = await turned_back(codec_of(plugin), positions_payload("e", range, answer))
    assert batch.records == answer.records


async def test_an_error_on_replay_where_the_read_had_a_record_fails_the_task():
    range, answer = live(record("c1", "a"))
    again = ReadOutput(
        records=[RecordWire(token="c1", record=b"", error="codec down")],
        next_token="c1",
        done=False,
    )
    plugin = StreamDirectReads({"e": FakeTransport(again)})
    with pytest.raises(StreamStorageError):
        await turned_back(codec_of(plugin), positions_payload("e", range, answer))


def chained() -> tuple[list[Payload], list[ReadRangeInput]]:
    """Two reads of one reader, as the read Activity records them.

    Returns the markers and the ranges a replay must ask for.
    """
    first_range, first = live(record("c1", "a"))
    second = ReadOutput(
        records=[record("c2", "b")],
        next_token="c2",
        done=False,
        state=b"\x03",
        replay=ReadOutputReplay(
            chain_first_run_id="run-1",
            after_token="c1",
            stored_records=1,
            records_digest=b"digest-2",
        ),
    )
    second_range = ReadRangeInput(
        stream=_STREAM,
        chain_first_run_id="run-1",
        after_token="c1",
        to_token="c2",
        stored_records=1,
        state=first.state,
        records_digest=b"digest-2",
        floor_holder="ns/run/1",
    )
    markers = [
        positions_payload("e", first_range, first, ReadChain("wf/1", 1)),
        positions_payload(
            "e",
            dataclasses.replace(second_range, state=None),
            second,
            ReadChain("wf/1", 2),
        ),
    ]
    return markers, [first_range, second_range]


async def test_a_later_read_replays_with_the_previous_answers_state():
    markers, ranges = chained()
    # The second marker leaves its input state out, which the first holds.
    assert "state" not in json.loads(markers[1].data)["range"]
    answers = [ReadOutput(records=[record("c1", "a")], next_token="c1", done=False)]
    answers.append(ReadOutput(records=[record("c2", "b")], next_token="c2", done=False))
    transport = FakeTransport(*answers)
    codec = codec_of(StreamDirectReads({"e": transport}))
    for marker in markers:
        await turned_back(codec, marker)
    assert transport.asked == ranges


async def test_a_later_read_without_the_previous_one_in_this_process_fails_the_task():
    markers, _ = chained()
    codec = codec_of(StreamDirectReads({"e": FakeTransport()}))
    with pytest.raises(StreamError, match="previous read"):
        await turned_back(codec, markers[1])


def test_the_marker_holds_no_record():
    range, answer = live(record("c1", "a" * 10_000))
    payload = positions_payload("e", range, answer)
    assert len(payload.data) < 1024
    assert payload.metadata["temporal.io/stream-nexus-endpoint"] == b"e"


@pytest.mark.parametrize(
    "failure_type,raised",
    [
        ("StreamReplayMismatchError", StreamReplayMismatchError),
        ("StreamReplayRangeLostError", StreamReplayRangeLostError),
    ],
)
async def test_the_http_transport_raises_the_class_the_handler_named(
    monkeypatch: pytest.MonkeyPatch, failure_type: str, raised: type[Exception]
):
    detail = json.dumps(
        {
            "message": f"{failure_type}: no",
            "details": {
                "cause": {
                    "message": "no",
                    "applicationFailureInfo": {"type": failure_type},
                }
            },
        }
    )

    def post(url: str, *_args: Any, **_kwargs: Any) -> tuple[bytes, str]:
        raise generated_client.HTTPStatusError(url, 400, detail, False)

    monkeypatch.setattr(generated_client, "_post", post)
    range, _ = live(record("c1", "a"))
    with pytest.raises(raised):
        await HttpTransport("http://stub").read_range(range)


async def test_the_http_transport_keeps_an_unnamed_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    def post(url: str, *_args: Any, **_kwargs: Any) -> tuple[bytes, str]:
        raise generated_client.HTTPStatusError(url, 503, "upstream down")

    monkeypatch.setattr(generated_client, "_post", post)
    with pytest.raises(generated_client.HTTPStatusError) as raised:
        await HttpTransport("http://stub").read(ReadInput(stream=_STREAM))
    assert raised.value.retryable
