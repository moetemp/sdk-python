"""The stream service bindings generated from its Nexus contract.

``scripts/gen_nexus_streams_api.py --check`` keeps the bindings in step with
the contract; these pin what the rest of the SDK relies on from them.
"""

from __future__ import annotations

import base64
import inspect
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypeVar

import nexusrpc
import pytest

import temporalio.contrib.streams
from temporalio.contrib.streams import StreamRef, StreamUnsupportedError
from temporalio.contrib.streams.nexus import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    RecordWire,
    TemporalStreams,
    TemporalStreamsHttpClient,
)
from temporalio.contrib.streams.nexus._generated import StreamOperationToken
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError

T = TypeVar("T")

_converter = DataConverter.default.payload_converter


def _to_json(value: object) -> dict[str, Any]:
    return json.loads(_converter.to_payloads([value])[0].data)


def _from_json(document: Mapping[str, Any], type_hint: type[T]) -> T:
    payload = _converter.to_payloads([document])[0]
    return _converter.from_payloads([payload], [type_hint])[0]


def test_the_service_definition_carries_the_contract_operations():
    definition = nexusrpc.get_service_definition(TemporalStreams)
    assert definition is not None
    assert definition.name == "temporal.sdk.streams.v1.TemporalStreams"
    operations = {
        operation.name: (operation.input_type, operation.output_type)
        for operation in definition.operation_definitions.values()
    }
    assert operations == {
        "append": (AppendInput, AppendOutput),
        "read": (ReadInput, ReadOutput),
    }


def test_the_contract_comes_from_the_vendored_core():
    # Core serves the contract, so the SDK generates from Core's copy and keeps
    # none of its own.
    package = Path(temporalio.contrib.streams.__file__).parent
    assert not (package / "nexus" / "temporal_streams.nexusrpc.yaml").exists()
    contract = (
        package.parent.parent
        / "bridge"
        / "sdk-core"
        / "crates"
        / "streams"
        / "nexus"
        / "temporal_streams.nexusrpc.yaml"
    )
    assert "x-nexus-stream-ref: { py: temporalio.contrib.streams.StreamRef }" in (
        contract.read_text()
    )


def test_the_stream_reference_is_the_sdk_type():
    from temporalio.contrib.streams.nexus import StreamRef as ServiceStreamRef

    assert ServiceStreamRef is StreamRef


def test_an_append_crosses_as_the_contract_spells_it():
    ref = StreamRef.for_workflow("wf-1", topic="tokens")
    append = AppendInput(
        stream=ref,
        producer_id="model",
        attempt=2,
        sequence=5,
        payloads=[b"\x0a\x01x", b""],
    )
    document = _to_json(append)
    assert document == {
        "stream": {
            "kind": "workflow",
            "workflow_id": "wf-1",
            "topic": "tokens",
        },
        "producer_id": "model",
        "attempt": 2,
        "sequence": 5,
        "payloads": [base64.b64encode(b"\x0a\x01x").decode(), ""],
    }
    decoded = _from_json(document, AppendInput)
    assert decoded == append
    assert type(decoded.stream) is StreamRef


def test_a_read_answer_decodes_with_its_cursors_and_records():
    document = {
        "records": [{"token": "memory:abcd1234:0", "record": "CgF0"}],
        "next_token": "memory:abcd1234:0",
        "done": False,
        "added_later": True,
    }
    answer = _from_json(document, ReadOutput)
    assert isinstance(answer, ReadOutput)
    assert answer.records == [
        RecordWire(token="memory:abcd1234:0", record=b"\x0a\x01t")
    ]
    assert answer.next_token == "memory:abcd1234:0"
    assert answer.done is False


def test_a_record_that_cannot_cross_decodes_as_an_error_at_its_cursor():
    document = {
        "records": [
            {"token": "memory:abcd1234:0", "record": "", "error": "too large"},
        ],
        "next_token": "memory:abcd1234:0",
        "done": False,
    }
    answer = _from_json(document, ReadOutput)
    assert isinstance(answer, ReadOutput)
    [record] = answer.records
    assert record.error == "too large"
    assert record.record == b""


def test_an_input_with_an_unknown_member_is_refused():
    document = _to_json(ReadInput(stream=StreamRef.for_workflow("wf-1")))
    document["max_record"] = 5
    # The type a Nexus Worker answers BAD_REQUEST for.
    with pytest.raises(ApplicationError) as refused:
        _from_json(document, ReadInput)
    assert refused.value.type == "PayloadValidationError"
    assert "max_record" in str(refused.value.details)


def test_a_reference_of_another_kind_decodes_for_the_handler_to_refuse():
    document = _to_json(ReadInput(stream=StreamRef.for_workflow("wf-1")))
    stream = document["stream"]
    assert isinstance(stream, dict)
    stream["kind"] = "activity"
    decoded = _from_json(document, ReadInput)
    assert isinstance(decoded, ReadInput)
    assert decoded.stream.kind == "activity"
    # Opening a handle on it is what refuses it.
    with pytest.raises(StreamUnsupportedError, match="'activity'"):
        decoded.stream._require_supported()  # type: ignore[reportPrivateUsage]


def test_a_supersession_decodes_at_its_cursor():
    document = {
        "records": [
            {
                "token": "memory:abcd1234:2",
                "record": "",
                "superseded": {
                    "producer_id": "tokens@run-1",
                    "previous_attempt": 1,
                    "attempt": 2,
                },
            },
        ],
        "next_token": "memory:abcd1234:2",
        "done": False,
    }
    answer = _from_json(document, ReadOutput)
    assert isinstance(answer, ReadOutput)
    [record] = answer.records
    assert record.superseded is not None
    assert (
        record.superseded.producer_id,
        record.superseded.previous_attempt,
        record.superseded.attempt,
    ) == ("tokens@run-1", 1, 2)


def test_a_read_carries_its_state_both_ways():
    # The reader carries Core's read state, so the handler holds none.
    read = ReadInput(stream=StreamRef.for_workflow("chat-1"), state=b"\x01\x02")
    assert _to_json(read)["state"] == base64.b64encode(b"\x01\x02").decode()
    answer = _from_json(
        {
            "records": [],
            "next_token": "memory:abcd1234:2",
            "done": False,
            "state": "AQI=",
        },
        ReadOutput,
    )
    assert answer.state == b"\x01\x02"


def test_a_stale_record_decodes_with_its_mark():
    record = base64.b64encode(b"\x1a\x06tokens").decode()
    answer = _from_json(
        {
            "records": [
                {"token": "memory:abcd1234:3", "record": record, "stale": True}
            ],
            "next_token": "memory:abcd1234:3",
            "done": False,
        },
        ReadOutput,
    )
    [stored] = answer.records
    assert stored.stale is True


def test_the_generated_long_poll_sends_back_each_answers_state():
    source = inspect.getsource(TemporalStreamsHttpClient.read_until_records)
    assert "request = dataclasses.replace(request, state=answer.state)" in source


# Core's crates/streams/nexus/operation_token_vectors.json, which Core's
# encoder checks too.
_TOKEN_VECTORS = [
    (
        StreamRef(kind="workflow", workflow_id="owner-1", run_id=None, topic="tokens"),
        "attach-1",
        "chain-1",
        "eyJhdHRhY2giOiJhdHRhY2gtMSIsImNoYWluIjoiY2hhaW4tMSIsInJlZiI6eyJraW5kIjoid29ya2Zsb3ciLC"
        "JydW5faWQiOm51bGwsInRvcGljIjoidG9rZW5zIiwid29ya2Zsb3dfaWQiOiJvd25lci0xIn0sInYiOjJ9",
    ),
    (
        StreamRef(
            kind="workflow", workflow_id="owner-1", run_id="run-2", topic="tokens"
        ),
        "attach-1",
        "run-1",
        "eyJhdHRhY2giOiJhdHRhY2gtMSIsImNoYWluIjoicnVuLTEiLCJyZWYiOnsia2luZCI6IndvcmtmbG93IiwicnV"
        "uX2lkIjoicnVuLTIiLCJ0b3BpYyI6InRva2VucyIsIndvcmtmbG93X2lkIjoib3duZXItMSJ9LCJ2IjoyfQ",
    ),
    (
        StreamRef(
            kind="workflow",
            workflow_id='order/42 "quoted"',
            run_id=None,
            topic="t\\back\nline",
        ),
        "req-\u00e9",
        "chain-\u00fc",
        "eyJhdHRhY2giOiJyZXEtXHUwMGU5IiwiY2hhaW4iOiJjaGFpbi1cdTAwZmMiLCJyZWYiOnsia2luZCI6IndvcmtmbG"
        "93IiwicnVuX2lkIjpudWxsLCJ0b3BpYyI6InRcXGJhY2tcbmxpbmUiLCJ3b3JrZmxvd19pZCI6Im9yZGVyLzQyIFwic"
        "XVvdGVkXCIifSwidiI6Mn0",
    ),
    (
        StreamRef(
            kind="workflow",
            workflow_id="\u65e5\u672c\u8a9e-wf",
            run_id=None,
            topic="emoji-\U0001f600",
        ),
        "r",
        "c",
        "eyJhdHRhY2giOiJyIiwiY2hhaW4iOiJjIiwicmVmIjp7ImtpbmQiOiJ3b3JrZmxvdyIsInJ1bl9pZCI6bnVsbCwidG9"
        "waWMiOiJlbW9qaS1cdWQ4M2RcdWRlMDAiLCJ3b3JrZmxvd19pZCI6Ilx1NjVlNVx1NjcyY1x1OGE5ZS13ZiJ9LCJ2Ij"
        "oyfQ",
    ),
]


@pytest.mark.parametrize("ref, attach, chain, token", _TOKEN_VECTORS)
def test_the_operation_token_reads_cores_vectors(
    ref: StreamRef, attach: str, chain: str, token: str
):
    decoded = StreamOperationToken.from_token(token)
    assert (decoded.v, decoded.ref, decoded.attach, decoded.chain) == (
        2,
        ref,
        attach,
        chain,
    )


@pytest.mark.parametrize(
    "token",
    [
        "",
        "not-a-token",
        # A token with only its version.
        "eyJ2IjogMn0",
        # A version 1 token.
        "eyJ2IjogMSwgImF0dGFjaCI6ICJhdHRhY2gtMSIsICJjaGFpbiI6ICJjaGFpbi0xIiwgInJlZiI6IHsia2luZCI6"
        "ICJ3b3JrZmxvdyIsICJ3b3JrZmxvd19pZCI6ICJvd25lci0xIiwgInJ1bl9pZCI6IG51bGwsICJ0b3BpYyI6ICJ0b2t"
        "lbnMifX0=",
    ],
)
def test_the_operation_token_refuses_cores_refused_vectors(token: str):
    with pytest.raises(ValueError):
        StreamOperationToken.from_token(token)
