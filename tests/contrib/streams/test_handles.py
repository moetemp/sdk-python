import json
import subprocess
import sys
import types
from dataclasses import dataclass
from typing import Any, TypeVar

import pydantic
import pytest

from temporalio import common
from temporalio.common import RawValue
from temporalio.common._streams._defn import definition_of
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.converter import DataConverter

E = TypeVar("E")


@dataclass
class Turn:
    kind: str


@common.defn(name="events")
class Events:
    turns = common.topic(Turn)
    tokens = common.topic(str)
    raw = common.topic(RawValue, name="raw-values")


@common.defn
class Index:
    turns = common.topic(int)


class PlainStream(common.StreamHandle[E]):
    pass


OWNED = PlainStream(
    id="wf:run:events",
    workflow_id="wf",
    first_execution_run_id="run",
    backend="redis",
    definition=Events,
)


def test_definition_rejects_two_topics_of_one_name():
    with pytest.raises(ValueError, match="two Topics named t"):

        @common.defn
        class Twice:  # pyright: ignore[reportUnusedClass]
            a = common.topic(int, name="t")
            b = common.topic(str, name="t")


def test_subclass_topic_replaces_the_base_topic():
    class Base:
        turns = common.topic(int)

    @common.defn
    class Sub(Base):
        turns = common.topic(str)

    assert definition_of(Sub).topics["turns"].type is str


def test_class_without_defn_is_not_a_definition():
    with pytest.raises(TypeError, match="not a Stream definition"):
        PlainStream(id="s", definition=Turn)


def test_topic_of_the_definition_keeps_its_type():
    assert OWNED._topic_spec(Events.turns, None) == ("turns", Turn)
    assert OWNED._topic_spec("tokens", None) == ("tokens", str)
    assert OWNED._topic_spec("custom", Turn) == ("custom", Turn)
    with pytest.raises(common.StreamError, match="has no Topic turns") as err:
        OWNED._topic_spec(Index.turns, None)
    assert err.value.reason == "unknown_topic"
    with pytest.raises(common.StreamError, match="Pass type="):
        OWNED._topic_spec("custom", None)
    with pytest.raises(TypeError, match="has the type"):
        OWNED._topic_spec("tokens", int)
    with pytest.raises(TypeError, match="already has its type"):
        OWNED._topic_spec(Events.turns, int)


def test_handle_is_the_proto_json_of_the_ref():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(OWNED)
    assert json.loads(payload.data) == {
        "id": "wf:run:events",
        "workflowId": "wf",
        "firstExecutionRunId": "run",
        "backend": "redis",
    }
    assert json.loads(converter.to_payload(PlainStream(id="s")).data) == {"id": "s"}


def test_type_hint_picks_the_class_and_the_definition():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(OWNED)
    decoded = converter.from_payload(payload, PlainStream[Events])
    assert type(decoded) is PlainStream
    assert decoded == OWNED
    assert decoded.definition is Events
    bare = converter.from_payload(payload, PlainStream)
    assert bare.definition is None and bare.backend == "redis"
    with pytest.raises(Exception, match="handle type of its context"):
        converter.from_payload(payload, common.StreamHandle)


@dataclass
class CallModelInput:
    prompt: str
    stream: PlainStream[Events]


def test_handle_in_a_dataclass_field():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payload(CallModelInput("hi", OWNED))
    assert json.loads(payload.data)["stream"]["workflowId"] == "wf"
    decoded = converter.from_payload(payload, CallModelInput)
    assert decoded.stream == OWNED and decoded.stream.definition is Events


class CallModelModel(pydantic.BaseModel):
    prompt: str
    stream: PlainStream[Events]


def test_handle_in_a_pydantic_field():
    model = CallModelModel(prompt="hi", stream=OWNED)
    dumped = json.loads(model.model_dump_json())
    assert dumped["stream"] == {
        "id": "wf:run:events",
        "workflowId": "wf",
        "firstExecutionRunId": "run",
        "backend": "redis",
    }
    loaded = CallModelModel.model_validate_json(model.model_dump_json())
    assert loaded.stream == OWNED and loaded.stream.definition is Events
    converter = pydantic_data_converter.payload_converter
    payload = converter.to_payload(model)
    decoded = converter.from_payload(payload, CallModelModel)
    assert decoded.stream == OWNED


def test_any_type_argument_has_no_definition():
    converter = DataConverter.default.payload_converter
    decoded = converter.from_payload(converter.to_payload(OWNED), PlainStream[Any])
    assert type(decoded) is PlainStream and decoded.definition is None
    assert decoded.id == OWNED.id


def test_end_type_is_public():
    # subscribe takes offset: int | End | None, so the type of END is a public name.
    assert isinstance(common.END, common.End) and "End" in common.__all__


def test_temporalio_common_exports_the_streams_api():
    # The Stream names load on first use, and each is the object of the private package.
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
    # The Stream modules import temporalio.client and temporalio.workflow.
    code = "import sys, temporalio.common; print('temporalio.common._streams' in sys.modules)"
    loaded = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert loaded.strip() == "False"
