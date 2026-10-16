"""Direct stream reads: the transports, the plugin and the codec step, with no server."""

from __future__ import annotations

import base64
import dataclasses
import json
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import nexusrpc
import pytest

from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import NexusHandlerErrorRetryBehavior
from temporalio.api.failure.v1 import (
    ApplicationFailureInfo,
    Failure,
    NexusHandlerFailureInfo,
)
from temporalio.api.workflowservice.v1 import (
    ExecuteSyncNexusOperationRequest,
    ExecuteSyncNexusOperationResponse,
)
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
    ClientTransport,
    HttpTransport,
    ReadInput,
    ReadOutput,
    RecordWire,
    StreamAccess,
    StreamDirectReads,
    StreamServiceCallError,
    TemporalStreamsHandler,
)
from temporalio.contrib.streams.nexus._direct import (
    FOLLOW_POSITIONS_TYPE,
    _DirectReadCodec,
    _put_follow,
    _transfer,
    answer_of,
    direct_follow_activity,
    direct_read_activity,
    follow_answer_of,
    innermost_codec,
    positions_payload,
)
from temporalio.contrib.streams.nexus._generated import (
    FollowedWire,
    FollowedWireRecord,
    FollowInput,
    FollowOutput,
    FollowOutputReplay,
    FollowRangeInput,
    ReadOutputReplay,
    ReadRangeInput,
)
from temporalio.contrib.streams.nexus._generated import client as generated_client
from temporalio.contrib.streams.nexus._handler import _caller_namespace
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment
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
            state_elided=True,
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


class FakeWorkflowService:
    """Answers ``execute_sync_nexus_operation`` from a list, and keeps the requests."""

    def __init__(self, *answers: ExecuteSyncNexusOperationResponse | Exception) -> None:
        self.answers = list(answers)
        self.requests: list[ExecuteSyncNexusOperationRequest] = []

    async def execute_sync_nexus_operation(
        self, req: ExecuteSyncNexusOperationRequest, **_kwargs: Any
    ) -> ExecuteSyncNexusOperationResponse:
        self.requests.append(req)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def fake_client(service: FakeWorkflowService, codec: PayloadCodec | None = None) -> Any:
    converter = dataclasses.replace(DataConverter.default, payload_codec=codec)
    return SimpleNamespace(
        namespace="caller-ns",
        identity="caller@host",
        data_converter=converter,
        workflow_service=service,
    )


def answered(output: ReadOutput) -> ExecuteSyncNexusOperationResponse:
    return ExecuteSyncNexusOperationResponse(
        result=Payload(
            metadata={"encoding": b"json/plain"},
            data=json.dumps(_transfer(output)).encode(),
        )
    )


async def test_the_client_transport_round_trips_through_the_inner_codec():
    _, answer = live(record("c1", "a"))
    codec = Base64Codec()
    [encoded] = await codec.encode([answered(answer).result])
    service = FakeWorkflowService(ExecuteSyncNexusOperationResponse(result=encoded))
    # The client's codec is wrapped, as on a client that carries the plugin.
    plugin = StreamDirectReads({})
    client = fake_client(service, codec_of(plugin, codec))
    got = await ClientTransport(client, "e").read(ReadInput(stream=_STREAM))
    assert got == answer
    [request] = service.requests
    assert (request.namespace, request.endpoint, request.operation) == (
        "caller-ns",
        "e",
        "read",
    )
    assert request.service == "temporal.sdk.streams.v1.TemporalStreams"
    assert request.request_id
    # The input crossed encoded once, with the codec under the wrapper.
    [plain] = await codec.decode([request.input])
    assert plain.metadata["encoding"] == b"json/plain"
    assert json.loads(plain.data)["stream"]["workflow_id"] == "owner"


async def test_the_client_transport_serves_a_replay_from_the_replay_codec():
    range, answer = live(record("c1", "a"))
    codec = Base64Codec()
    [encoded] = await codec.encode(
        [
            answered(
                ReadOutput(records=answer.records, next_token="c1", done=False)
            ).result
        ]
    )
    service = FakeWorkflowService(ExecuteSyncNexusOperationResponse(result=encoded))
    holder: dict[str, Any] = {}
    plugin = StreamDirectReads(
        {"e": cast(Any, LateTransport(lambda: holder["transport"]))}
    )
    replay_codec = codec_of(plugin, codec)
    holder["transport"] = ClientTransport(fake_client(service, replay_codec), "e")
    batch = await turned_back(replay_codec, positions_payload("e", range, answer))
    assert batch.records == answer.records
    assert [r.operation for r in service.requests] == ["read_range"]


class LateTransport:
    """A transport made after the plugin, as the client it needs is."""

    def __init__(self, get: Any) -> None:
        self._get = get

    async def read(self, request: ReadInput) -> ReadOutput:
        return await self._get().read(request)

    async def read_range(self, request: ReadRangeInput) -> ReadOutput:
        return await self._get().read_range(request)


def handler_failure(
    kind: str,
    *,
    retry: NexusHandlerErrorRetryBehavior.ValueType = NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
    cause_type: str | None = None,
) -> ExecuteSyncNexusOperationResponse:
    failure = Failure(
        message="refused",
        nexus_handler_failure_info=NexusHandlerFailureInfo(
            type=kind, retry_behavior=retry
        ),
    )
    if cause_type is not None:
        failure.cause.CopyFrom(
            Failure(
                message="the cause",
                application_failure_info=ApplicationFailureInfo(type=cause_type),
            )
        )
    return ExecuteSyncNexusOperationResponse(failure=failure)


@pytest.mark.parametrize(
    "cause_type,raised",
    [
        ("StreamReplayMismatchError", StreamReplayMismatchError),
        ("StreamReplayRangeLostError", StreamReplayRangeLostError),
        ("StreamStorageError", StreamStorageError),
    ],
)
async def test_the_client_transport_raises_the_class_the_handler_named(
    cause_type: str, raised: type[Exception]
):
    service = FakeWorkflowService(handler_failure("BAD_REQUEST", cause_type=cause_type))
    range, _ = live(record("c1", "a"))
    with pytest.raises(raised, match="the cause"):
        await ClientTransport(fake_client(service), "e").read_range(range)


@pytest.mark.parametrize(
    "kind,retry,retryable",
    [
        # A refused credential is a deployment fault, so the read retries.
        ("UNAUTHORIZED", None, True),
        ("UNAVAILABLE", None, True),
        ("INTERNAL", "non_retryable", False),
        ("BAD_REQUEST", "retryable", True),
    ],
)
async def test_the_client_transport_keeps_an_unnamed_failure_and_its_retry(
    kind: str, retry: str | None, retryable: bool
):
    behavior = {
        None: NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
        "retryable": NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
        "non_retryable": NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
    }[retry]
    service = FakeWorkflowService(handler_failure(kind, retry=behavior))
    with pytest.raises(StreamServiceCallError) as raised:
        await ClientTransport(fake_client(service), "e").read(ReadInput(stream=_STREAM))
    assert raised.value.failure_type == kind
    assert raised.value.retryable is retryable


async def test_the_client_transport_refuses_an_answer_its_codec_cannot_read():
    service = FakeWorkflowService(
        ExecuteSyncNexusOperationResponse(
            result=Payload(metadata={"encoding": b"binary/encrypted"}, data=b"x")
        )
    )
    with pytest.raises(ValueError, match="codec"):
        await ClientTransport(fake_client(service), "e").read(ReadInput(stream=_STREAM))


def test_authorize_sees_the_caller_namespace_the_server_set():
    def ctx(headers: dict[str, str]) -> Any:
        return SimpleNamespace(headers=headers)

    assert _caller_namespace(ctx({"temporal-caller-namespace": "a"})) == "a"
    assert _caller_namespace(ctx({"Temporal-Caller-Namespace": "a"})) == "a"
    assert _caller_namespace(ctx({"other": "a"})) is None


class FakeFollowTransport:
    """Answers each follow_range from a list, and keeps what it was asked."""

    def __init__(self, *answers: FollowOutput | Exception) -> None:
        self.answers = list(answers)
        self.asked: list[FollowRangeInput] = []

    async def read(self, request: ReadInput) -> ReadOutput:
        raise AssertionError(f"a replay never reads, but read {request}")

    async def read_range(self, request: ReadRangeInput) -> ReadOutput:
        raise AssertionError(f"a follow replay never reads a range, but got {request}")

    async def follow(self, request: FollowInput) -> FollowOutput:
        raise AssertionError(f"a replay never follows, but followed {request}")

    async def follow_range(self, request: FollowRangeInput) -> FollowOutput:
        self.asked.append(request)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def followed(*tokens: str) -> FollowOutput:
    return FollowOutput(
        records=[
            FollowedWire(source=0, record=FollowedWireRecord(token=t, record=b""))
            for t in tokens
        ],
        done=False,
        state=b"\x07live",
        sources=[],
        replay=FollowOutputReplay(ranges=[b"r1", b"r2"], follow_digest=b"digest"),
    )


def follow_marker(answer: FollowOutput) -> tuple[FollowRangeInput, Payload]:
    """A live follow's range and the result its Activity records."""
    range = FollowRangeInput(
        stream=_STREAM,
        state=b"\x01",
        ranges=[b"r1", b"r2"],
        follow_digest=b"digest",
        max_depth=4,
        max_open=16,
        floor_holder="ns/run/1",
    )
    document = {
        "range": _transfer(range),
        "answer": _transfer(dataclasses.replace(answer, records=[])),
    }
    payload = Payload(
        metadata={
            "encoding": b"json/plain",
            "messageType": FOLLOW_POSITIONS_TYPE,
            "temporal.io/stream-nexus-endpoint": b"e",
        },
        data=json.dumps(document, sort_keys=True).encode(),
    )
    return range, payload


async def turned_back_follow(codec: PayloadCodec, payload: Payload) -> FollowOutput:
    [decoded] = await codec.decode(await codec.encode([payload]))
    return follow_answer_of(decoded)


async def test_a_follow_turns_back_from_the_cache_on_the_live_task():
    transport = FakeFollowTransport()
    plugin = StreamDirectReads({"e": transport})
    answer = followed("c1", "c2")
    range, payload = follow_marker(answer)
    _put_follow(plugin, range, answer)
    assert await turned_back_follow(codec_of(plugin, Base64Codec()), payload) == answer
    assert transport.asked == []


async def test_a_follow_replays_its_ranges_and_keeps_the_live_state():
    answer = followed("c1", "c2")
    replayed = dataclasses.replace(answer, state=b"\x07replay")
    transport = FakeFollowTransport(replayed)
    plugin = StreamDirectReads({"e": transport})
    range, payload = follow_marker(answer)
    batch = await turned_back_follow(codec_of(plugin), payload)
    assert transport.asked == [range]
    assert batch.records == answer.records
    # A replay's state differs in its clock fields, so the marker's stays.
    assert batch.state == b"\x07live"


async def test_a_follow_that_replays_other_items_is_nondeterminism():
    transport = FakeFollowTransport(StreamReplayMismatchError("digest"))
    plugin = StreamDirectReads({"e": transport})
    _, payload = follow_marker(followed("c1"))
    with pytest.raises(NondeterminismError):
        await turned_back_follow(codec_of(plugin), payload)


def test_a_follow_result_no_codec_turned_back_names_the_opt_out():
    _, payload = follow_marker(followed("c1"))
    with pytest.raises(StreamUnsupportedError, match="replay_from_store=False"):
        follow_answer_of(payload)


async def test_the_client_transport_follows_through_execute_sync():
    answer = followed("c1")
    service = FakeWorkflowService(
        ExecuteSyncNexusOperationResponse(
            result=Payload(
                metadata={"encoding": b"json/plain"},
                data=json.dumps(_transfer(answer)).encode(),
            )
        )
    )
    got = await ClientTransport(fake_client(service), "e").follow(
        FollowInput(stream=_STREAM)
    )
    assert got == answer
    [request] = service.requests
    assert request.operation == "follow"


class LiveTransport:
    """Answers each live read with one record, and keeps the timeout it got."""

    async def read(self, request: ReadInput) -> ReadOutput:
        del request
        return live(record("c1", "a"))[1]

    async def read_range(self, request: ReadRangeInput) -> ReadOutput:
        del request
        raise AssertionError("a live read never reads a range")


@pytest.mark.parametrize("chained", [False, True])
async def test_the_read_activity_chains_by_the_reading_run(chained: bool):
    plugin = StreamDirectReads({"e": LiveTransport()})
    env = ActivityEnvironment(client=Mock(config=lambda: {"plugins": [plugin]}))
    env.info = dataclasses.replace(env.info, workflow_run_id="run-b", is_local=True)
    raw = await env.run(
        direct_read_activity,
        "e",
        "1",
        ReadInput(stream=_STREAM, state=b"\x01"),
        2,
        chained,
    )
    document = json.loads(raw.payload.data)
    # Runs of one Workflow id number their readers alike, so the run names
    # the chain. Only a read the reader says follows its own run's read
    # leaves its input state out.
    assert document["reader"] == "run-b/1"
    assert ("state" in document["range"]) is not chained
    assert document.get("elided", False) is chained


async def test_the_http_transport_waits_no_longer_than_the_activity_has_left(
    monkeypatch: pytest.MonkeyPatch,
):
    timeouts: list[float] = []

    def post(
        url: str,
        _body: bytes,
        _headers: Any,
        timeout: float,
        *_args: Any,
        **_kwargs: Any,
    ) -> Any:
        timeouts.append(timeout)
        raise generated_client.HTTPStatusError(url, 503, "down")

    monkeypatch.setattr(generated_client, "_post", post)
    transport = HttpTransport("http://stub")
    env = ActivityEnvironment()
    env.info = dataclasses.replace(
        env.info,
        started_time=datetime.now(timezone.utc),
        start_to_close_timeout=timedelta(seconds=5),
    )

    async def read() -> None:
        await transport.read(ReadInput(stream=_STREAM))

    for run in (env.run(read), read()):
        with pytest.raises(generated_client.HTTPStatusError):
            await run
    assert 0 < timeouts[0] <= 5 and timeouts[1] == 30


async def test_a_refused_credential_retries_the_read():
    class RefusesOnce:
        def __init__(self) -> None:
            self.calls = 0

        async def read(self, request: ReadInput) -> ReadOutput:
            del request
            self.calls += 1
            if self.calls == 1:
                raise generated_client.HTTPStatusError("http://stub", 401, "expired")
            return live(record("c1", "a"))[1]

        async def read_range(self, request: ReadRangeInput) -> ReadOutput:
            del request
            raise AssertionError("a live read never reads a range")

    transport = RefusesOnce()
    plugin = StreamDirectReads({"e": transport})
    env = ActivityEnvironment(client=Mock(config=lambda: {"plugins": [plugin]}))
    env.info = dataclasses.replace(env.info, is_local=True)
    # The Activity's own failure is retryable, so its next attempt reads.
    with pytest.raises(generated_client.HTTPStatusError):
        await env.run(direct_read_activity, "e", "1", ReadInput(stream=_STREAM), 1)
    raw = await env.run(direct_read_activity, "e", "1", ReadInput(stream=_STREAM), 1)
    assert json.loads(raw.payload.data)["range"]["to_token"] == "c1"


async def test_a_credential_the_server_refuses_retries_the_read():
    class RefusesOnce:
        def __init__(self) -> None:
            self.calls = 0

        async def read(self, request: ReadInput) -> ReadOutput:
            del request
            self.calls += 1
            if self.calls == 1:
                raise StreamServiceCallError(
                    "e/read failed: refused",
                    failure_type="UNAUTHORIZED",
                    retryable=False,
                )
            return live(record("c1", "a"))[1]

        async def read_range(self, request: ReadRangeInput) -> ReadOutput:
            del request
            raise AssertionError("a live read never reads a range")

    plugin = StreamDirectReads({"e": RefusesOnce()})
    env = ActivityEnvironment(client=Mock(config=lambda: {"plugins": [plugin]}))
    env.info = dataclasses.replace(env.info, is_local=True)
    with pytest.raises(StreamServiceCallError):
        await env.run(direct_read_activity, "e", "1", ReadInput(stream=_STREAM), 1)
    raw = await env.run(direct_read_activity, "e", "1", ReadInput(stream=_STREAM), 1)
    assert json.loads(raw.payload.data)["range"]["to_token"] == "c1"


async def test_a_floor_holder_outside_the_callers_namespace_is_refused():
    handler = TemporalStreamsHandler()
    context: Any = SimpleNamespace(headers={"temporal-caller-namespace": "caller-ns"})

    def read(holder: str) -> StreamAccess:
        return StreamAccess("read", _STREAM, context, floor_holder=holder)

    await handler._check(read("caller-ns/run/1"))
    with pytest.raises(nexusrpc.HandlerError) as refused:
        await handler._check(read("other-ns/run/1"))
    assert refused.value.type is nexusrpc.HandlerErrorType.UNAUTHORIZED


async def test_a_refused_credential_retries_the_follow():
    class Refuses:
        async def read(self, request: ReadInput) -> ReadOutput:
            del request
            raise AssertionError("a follow never reads")

        async def read_range(self, request: ReadRangeInput) -> ReadOutput:
            del request
            raise AssertionError("a follow never reads a range")

        async def follow(self, request: FollowInput) -> FollowOutput:
            del request
            raise generated_client.HTTPStatusError("http://stub", 403, "lost a role")

        async def follow_range(self, request: FollowRangeInput) -> FollowOutput:
            del request
            raise AssertionError("a live follow never follows a range")

    plugin = StreamDirectReads({"e": Refuses()})
    env = ActivityEnvironment(client=Mock(config=lambda: {"plugins": [plugin]}))
    env.info = dataclasses.replace(env.info, is_local=True)
    # Raised as is, so the Activity's retry runs, not as a final failure.
    with pytest.raises(generated_client.HTTPStatusError):
        await env.run(direct_follow_activity, "e", "1", FollowInput(stream=_STREAM))


async def test_a_refusal_the_handler_marks_final_fails_the_read():
    class Refuses:
        async def read(self, request: ReadInput) -> ReadOutput:
            del request
            raise generated_client.HTTPStatusError(
                "http://stub", 403, "the caller may not read", False
            )

        async def read_range(self, request: ReadRangeInput) -> ReadOutput:
            del request
            raise AssertionError("a live read never reads a range")

    plugin = StreamDirectReads({"e": Refuses()})
    env = ActivityEnvironment(client=Mock(config=lambda: {"plugins": [plugin]}))
    env.info = dataclasses.replace(env.info, is_local=True)
    # An authorizer's refusal is a decision, not a fault, so it stays final.
    with pytest.raises(ApplicationError) as raised:
        await env.run(direct_read_activity, "e", "1", ReadInput(stream=_STREAM), 1)
    assert raised.value.non_retryable
