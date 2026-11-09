import json
import subprocess
import sys
from dataclasses import dataclass
from typing import Any

import pydantic
from google.protobuf import json_format

import temporalio.api.stream.v1
import temporalio.common
from temporalio import streams
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.converter import DataConverter


@dataclass
class Turn:
    kind: str


BOUND: streams.StreamRef[Turn] = streams.StreamRef(
    id="session-7-turns", backend="redis"
)


def test_ref_is_the_proto_json_of_the_ref_message():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(BOUND)
    message = temporalio.api.stream.v1.StreamRef(
        id="session-7-turns",
        backend="redis",
    )
    assert json.loads(payload.data) == json_format.MessageToDict(message)
    assert payload.metadata["encoding"] == b"json/plain"
    assert payload.data == b'{"backend":"redis","id":"session-7-turns"}'
    assert json.loads(converter.to_payload(streams.StreamRef(id="s")).data) == {
        "id": "s"
    }


def test_ref_has_the_fields_of_the_wire_model():
    assert BOUND.id == "session-7-turns" and BOUND.backend == "redis"
    assert streams.StreamRef(id="s") == streams.StreamRef("s")
    assert streams.StreamRef(id="s") != streams.StreamRef(id="s", backend="redis")
    same: streams.StreamRef[Turn] = streams.StreamRef("session-7-turns", "redis")
    assert same == BOUND and len({BOUND, same}) == 1


def test_any_type_hint_decodes_the_ref():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(BOUND)
    for hint in (
        streams.StreamRef[Turn],
        streams.StreamRef[list[str]],
        streams.StreamRef[Any],
        streams.StreamRef,
    ):
        decoded = converter.from_payload(payload, hint)
        assert type(decoded) is streams.StreamRef and decoded == BOUND


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.StreamRef[Turn]


def test_ref_in_a_dataclass_field():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(CallModelInput("hi", BOUND))
    assert json.loads(payload.data)["stream"]["id"] == "session-7-turns"
    assert payload.data == (
        b'{"prompt":"hi","stream":{"backend":"redis","id":"session-7-turns"}}'
    )
    decoded = converter.from_payload(payload, CallModelInput)
    assert decoded.stream == BOUND


class CallModelModel(pydantic.BaseModel):
    prompt: str
    stream: streams.StreamRef[Turn]


def test_ref_in_a_pydantic_field():
    model = CallModelModel(prompt="hi", stream=BOUND)
    dumped = json.loads(model.model_dump_json())
    assert dumped["stream"] == {"id": "session-7-turns", "backend": "redis"}
    loaded = CallModelModel.model_validate_json(model.model_dump_json())
    assert loaded.stream == BOUND
    converter = pydantic_data_converter.payload_converter
    payload = converter.to_payload(model)
    decoded = converter.from_payload(payload, CallModelModel)
    assert decoded.stream == BOUND


def test_read_items_have_a_kind_that_narrows_their_type():
    record = streams.Record(
        data=Turn("model"),
        offset=streams.StreamOffset.parse("3"),
        producer=None,
    )
    closed = streams.Closed(
        offset=streams.StreamOffset.parse("4"),
        status=streams.StreamCloseStatus.CLOSED,
    )
    items: list[streams.Record[Turn] | streams.Closed] = [record, closed]
    shown: list[Turn] = []
    ended: list[streams.StreamCloseStatus] = []
    for item in items:
        match item.kind:  # the kind narrows the item's type
            case streams.ItemKind.CLOSED:
                ended.append(item.status)
            case _:
                shown.append(item.data)
    assert shown == [Turn("model")]
    assert ended == [streams.StreamCloseStatus.CLOSED]
    assert record.kind is streams.ItemKind.RECORD
    assert closed.kind is streams.ItemKind.CLOSED


def test_temporalio_common_has_no_stream_api():
    assert not any(name.startswith("Stream") for name in vars(temporalio.common))


def test_import_of_the_sdk_loads_no_stream_code():
    code = (
        "import sys, temporalio.client, temporalio.common, temporalio.worker, "
        "temporalio.workflow; print('temporalio.streams' in sys.modules)"
    )
    loaded = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert loaded.strip() == "False"
