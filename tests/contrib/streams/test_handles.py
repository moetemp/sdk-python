import json
import subprocess
import sys
import types
from dataclasses import dataclass
from typing import Any, TypeVar

import pydantic
import pytest
from google.protobuf import json_format

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.stream.v1
from temporalio import common
from temporalio.common import RawValue
from temporalio.common._streams._defn import DefinitionInfo, definition_of
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.converter import DataConverter

T = TypeVar("T")


@dataclass
class Turn:
    kind: str


@common.defn(name="turns")
class Turns(common.StreamDefinition[Turn]): ...


@common.defn
class Raw(common.StreamDefinition[RawValue]): ...


class PlainStream(common.StreamHandle[T]):
    pass


OWNED: PlainStream[Turn] = PlainStream(
    id="wf:run:turns",
    workflow_id="wf",
    first_execution_run_id="run",
    backend="redis",
    payload_type=Turn,
)


def test_definition_has_its_name_and_payload_type():
    assert definition_of(Turns) == DefinitionInfo(name="turns", type=Turn)
    assert definition_of(Raw) == DefinitionInfo(name="Raw", type=RawValue)


def test_subclass_of_a_definition_keeps_the_payload_type():
    class Base(common.StreamDefinition[int]): ...

    @common.defn
    class Sub(Base): ...

    assert definition_of(Sub).type is int


def test_definition_needs_the_base_class_and_a_payload_type():
    with pytest.raises(TypeError, match="must subclass StreamDefinition"):

        @common.defn
        class Plain:  # pyright: ignore[reportUnusedClass]
            pass

    with pytest.raises(TypeError, match="no payload type"):

        @common.defn
        class Bare(common.StreamDefinition):  # pyright: ignore[reportUnusedClass]
            pass


def test_class_without_defn_is_not_a_definition():
    class Unmarked(common.StreamDefinition[int]): ...

    with pytest.raises(TypeError, match="not a Stream definition"):
        definition_of(Unmarked)


def test_handle_is_the_proto_json_of_the_ref():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(OWNED)
    ref = temporalio.api.stream.v1.StreamRef(
        id="wf:run:turns",
        owner=temporalio.api.common.v1.Execution(
            type=temporalio.api.enums.v1.ExecutionType.EXECUTION_TYPE_WORKFLOW,
            business_id="wf",
            run_id="run",
        ),
        backend="redis",
    )
    assert json.loads(payload.data) == json_format.MessageToDict(ref)
    assert json.loads(converter.to_payload(PlainStream(id="s")).data) == {"id": "s"}


def test_type_hint_picks_the_class_and_the_payload_type():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(OWNED)
    decoded = converter.from_payload(payload, PlainStream[Turn])
    assert type(decoded) is PlainStream
    assert decoded == OWNED
    assert decoded._payload_type is Turn
    listed = converter.from_payload(payload, PlainStream[list[str]])
    assert listed._payload_type == list[str]
    bare = converter.from_payload(payload, PlainStream)
    assert bare._payload_type is None and bare.backend == "redis"
    with pytest.raises(Exception, match="handle type of its context"):
        converter.from_payload(payload, common.StreamHandle)


@dataclass
class CallModelInput:
    prompt: str
    stream: PlainStream[Turn]


def test_handle_in_a_dataclass_field():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(CallModelInput("hi", OWNED))
    assert json.loads(payload.data)["stream"]["owner"]["businessId"] == "wf"
    decoded = converter.from_payload(payload, CallModelInput)
    assert decoded.stream == OWNED and decoded.stream._payload_type is Turn


class CallModelModel(pydantic.BaseModel):
    prompt: str
    stream: PlainStream[Turn]


def test_handle_in_a_pydantic_field():
    model = CallModelModel(prompt="hi", stream=OWNED)
    dumped = json.loads(model.model_dump_json())
    assert dumped["stream"] == {
        "id": "wf:run:turns",
        "owner": {
            "type": "EXECUTION_TYPE_WORKFLOW",
            "businessId": "wf",
            "runId": "run",
        },
        "backend": "redis",
    }
    loaded = CallModelModel.model_validate_json(model.model_dump_json())
    assert loaded.stream == OWNED and loaded.stream._payload_type is Turn
    converter = pydantic_data_converter.payload_converter
    payload = converter.to_payload(model)
    decoded = converter.from_payload(payload, CallModelModel)
    assert decoded.stream == OWNED


def test_any_type_argument_has_no_payload_type():
    converter = DataConverter.default.payload_converter
    decoded = converter.from_payload(converter.to_payload(OWNED), PlainStream[Any])
    assert type(decoded) is PlainStream and decoded._payload_type is None
    assert decoded.id == OWNED.id


def test_temporalio_common_exports_the_streams_api():
    # temporalio.common imports the Stream names on first use. Each is the private package's.
    import temporalio.common._streams as streams_api

    names = {
        name
        for name, value in vars(streams_api).items()
        if not name.startswith("_") and not isinstance(value, types.ModuleType)
    }
    assert names == common._STREAM_NAMES and names <= set(common.__all__)
    for name in common._STREAM_NAMES:
        assert getattr(common, name) is getattr(streams_api, name)


def test_import_of_temporalio_common_loads_no_stream_code():
    # The Stream modules import temporalio.client and temporalio.workflow, which import
    # temporalio.common.
    code = "import sys, temporalio.common; print('temporalio.common._streams' in sys.modules)"
    loaded = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert loaded.strip() == "False"
