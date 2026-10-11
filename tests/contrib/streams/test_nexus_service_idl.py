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


# A copy of Core's crates/streams/nexus/operation_token_vectors.json, from
# moetemp/sdk-rust moe/bridge/cn5-stream-callers at d34278ab. Core's encoder and
# decoder read the same file.
_TOKEN_VECTORS = json.loads(
    (Path(__file__).parent / "operation_token_vectors.json").read_text()
)


def _token_of(vector: dict[str, Any]) -> StreamOperationToken:
    ref = vector["ref"]
    return StreamOperationToken(
        ref=StreamRef(
            kind=ref["kind"],
            workflow_id=ref["workflow_id"],
            run_id=ref.get("run_id"),
            topic=ref["topic"],
        ),
        attach=vector["attach"],
        chain=vector["chain"],
    )


@pytest.mark.parametrize("vector", _TOKEN_VECTORS["vectors"])
def test_the_operation_token_writes_and_reads_cores_vectors(vector: dict[str, Any]):
    token = _token_of(vector)
    assert token.to_token() == vector["token"]
    assert StreamOperationToken.from_token(vector["token"]) == token


@pytest.mark.parametrize("vector", _TOKEN_VECTORS["decode_only"])
def test_the_operation_token_reads_cores_decode_only_vectors(vector: dict[str, Any]):
    assert StreamOperationToken.from_token(vector["token"]) == _token_of(vector)


@pytest.mark.parametrize("token", _TOKEN_VECTORS["refused"])
def test_the_operation_token_refuses_cores_refused_vectors(token: str):
    with pytest.raises(ValueError):
        StreamOperationToken.from_token(token)
