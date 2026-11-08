import json
import subprocess
import sys
from dataclasses import dataclass
from typing import Any

import pydantic
from google.protobuf import json_format

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.stream.v1
import temporalio.common
from temporalio import streams
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.converter import DataConverter
from temporalio.streams import _items


@dataclass
class Turn:
    kind: str


OWNER = streams.Execution(
    type=streams.ExecutionType.ExecutionTypeWorkflow, business_id="wf", run_id="run"
)
BOUND: streams.StreamRef[Turn] = streams.StreamRef(
    id="session-7-turns", owner=OWNER, backend="redis"
)


def test_ref_is_the_proto_json_of_the_ref_message():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(BOUND)
    message = temporalio.api.stream.v1.StreamRef(
        id="session-7-turns",
        owner=temporalio.api.common.v1.Execution(
            type=temporalio.api.enums.v1.ExecutionType.EXECUTION_TYPE_WORKFLOW,
            business_id="wf",
            run_id="run",
        ),
        backend="redis",
    )
    assert json.loads(payload.data) == json_format.MessageToDict(message)
    assert payload.metadata["encoding"] == b"json/plain"
    assert payload.data == (
        b'{"backend":"redis","id":"session-7-turns",'
        b'"owner":{"businessId":"wf","runId":"run","type":"EXECUTION_TYPE_WORKFLOW"}}'
    )
    assert json.loads(converter.to_payload(streams.StreamRef(id="s")).data) == {
        "id": "s"
    }


def test_ref_has_the_fields_of_the_wire_model():
    assert BOUND.id == "session-7-turns"
    assert BOUND.owner is not None and BOUND.owner.business_id == "wf"
    assert BOUND.owner.run_id == "run" and BOUND.backend == "redis"
    assert streams.StreamRef(id="s") == streams.StreamRef("s")
    assert streams.StreamRef(id="s") != streams.StreamRef(id="s", backend="redis")
    same: streams.StreamRef[Turn] = streams.StreamRef("session-7-turns", OWNER, "redis")
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
    assert json.loads(payload.data)["stream"]["owner"]["businessId"] == "wf"
    assert payload.data == (
        b'{"prompt":"hi","stream":{"backend":"redis","id":"session-7-turns",'
        b'"owner":{"businessId":"wf","runId":"run","type":"EXECUTION_TYPE_WORKFLOW"}}}'
    )
    decoded = converter.from_payload(payload, CallModelInput)
    assert decoded.stream == BOUND


class CallModelModel(pydantic.BaseModel):
    prompt: str
    stream: streams.StreamRef[Turn]


def test_ref_in_a_pydantic_field():
    model = CallModelModel(prompt="hi", stream=BOUND)
    dumped = json.loads(model.model_dump_json())
    assert dumped["stream"] == {
        "id": "session-7-turns",
        "owner": {
            "type": "EXECUTION_TYPE_WORKFLOW",
            "businessId": "wf",
            "runId": "run",
        },
        "backend": "redis",
    }
    loaded = CallModelModel.model_validate_json(model.model_dump_json())
    assert loaded.stream == BOUND
    converter = pydantic_data_converter.payload_converter
    payload = converter.to_payload(model)
    decoded = converter.from_payload(payload, CallModelModel)
    assert decoded.stream == BOUND


def test_a_ref_keeps_an_owner_of_any_type():
    # Data from a later stage or another SDK can carry a Standalone Activity's run.
    owner = streams.Execution(
        type=streams.ExecutionType.ExecutionTypeActivity, business_id="a", run_id="r"
    )
    ref: streams.StreamRef[str] = streams.StreamRef(id="s", owner=owner)
    converter = DataConverter.default.payload_converter
    decoded = converter.from_payload(converter.to_payload(ref), streams.StreamRef[str])
    assert decoded == ref and decoded.owner == owner


def _producer(id: str, attempt: int) -> streams.Producer:
    return streams.Producer(
        id=id,
        epoch=streams.ProducerEpoch(
            kind=streams.ProducerEpochKindActivity(
                streams.ActivityEpoch(attempt=attempt)
            )
        ),
        run_id="run",
    )


def test_read_items_have_a_kind_that_narrows_their_type():
    record = streams.Record(
        data=Turn("model"),
        offset=streams.StreamOffset.parse("3"),
        producer=_producer("a", 1),
    )
    superseded = streams.Superseded(
        offset=streams.StreamOffset.parse("4"),
        facts=streams.StreamSuperseded(
            writer=streams.StreamWriter(producer=_producer("a", 2), failed_tasks=[])
        ),
    )
    items: list[streams.Record[Turn] | streams.Superseded] = [record, superseded]
    shown: list[Turn] = []
    withdrawn: list[bool] = []
    for item in items:
        match item.kind:
            case streams.ItemKind.SUPERSEDED:
                withdrawn.append(item.withdraws(record))
            case _:
                shown.append(item.data)
    assert shown == [Turn("model")] and withdrawn == [True]
    assert record.kind is streams.ItemKind.RECORD


def test_superseded_withdraws_by_the_producer_of_an_earlier_record():
    superseded = streams.Superseded(
        offset=streams.StreamOffset.parse("9"),
        facts=streams.StreamSuperseded(
            writer=streams.StreamWriter(producer=_producer("a", 2), failed_tasks=[])
        ),
    )
    # An app that resumes keeps only the Producer of each Record that it showed.
    assert superseded.withdraws(_producer("a", 1))
    assert not superseded.withdraws(_producer("a", 2))
    assert not superseded.withdraws(_producer("b", 1))


def test_a_client_producer_is_never_withdrawn():
    client = streams.Producer(
        id="client:me",
        epoch=streams.ProducerEpoch(
            kind=streams.ProducerEpochKindClient(streams.ClientEpoch(attempt=1))
        ),
    )
    newer = streams.Producer(
        id="client:me",
        epoch=streams.ProducerEpoch(
            kind=streams.ProducerEpochKindClient(streams.ClientEpoch(attempt=2))
        ),
    )
    facts = streams.StreamSuperseded(
        writer=streams.StreamWriter(producer=newer, failed_tasks=[])
    )
    assert _items.is_client(client)
    assert not _items.withdraws(facts, client)


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
