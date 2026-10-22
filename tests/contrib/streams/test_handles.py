import json
from dataclasses import dataclass
from typing import Any, TypeVar

import pydantic
import pytest

from temporalio.common import RawValue
from temporalio.contrib import streams
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.streams._defn import definition_of
from temporalio.converter import DataConverter

E = TypeVar("E")


@dataclass
class Turn:
    kind: str


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)
    raw = streams.topic(RawValue, name="raw-values")


@streams.defn
class Index:
    turns = streams.topic(int)


class PlainStream(streams.StreamHandle[E]):
    pass


OWNED = PlainStream(
    id="wf:run:events",
    workflow_id="wf",
    first_execution_run_id="run",
    backend="redis",
    definition=Events,
)


def test_definition_takes_the_name_and_the_topics():
    events = definition_of(Events)
    assert events.name == "events"
    assert list(events.topics) == ["turns", "tokens", "raw-values"]
    assert Events.turns.name == "turns" and Events.turns.type is Turn
    assert Events.raw.name == "raw-values"
    assert definition_of(Index).name == "Index"


def test_definition_rejects_two_topics_of_one_name():
    with pytest.raises(ValueError, match="two Topics named t"):

        @streams.defn
        class Twice:  # pyright: ignore[reportUnusedClass]
            a = streams.topic(int, name="t")
            b = streams.topic(str, name="t")


def test_subclass_topic_replaces_the_base_topic():
    class Base:
        turns = streams.topic(int)

    @streams.defn
    class Sub(Base):
        turns = streams.topic(str)

    assert definition_of(Sub).topics["turns"].type is str


def test_class_without_defn_is_not_a_definition():
    with pytest.raises(TypeError, match="not a Stream definition"):
        PlainStream(id="s", definition=Turn)


def test_topic_of_the_definition_keeps_its_type():
    assert OWNED._topic_spec(Events.turns, None) == ("turns", Turn)
    assert OWNED._topic_spec("tokens", None) == ("tokens", str)
    assert OWNED._topic_spec("custom", Turn) == ("custom", Turn)
    with pytest.raises(streams.StreamError, match="has no Topic turns") as err:
        OWNED._topic_spec(Index.turns, None)
    assert err.value.reason == "unknown_topic"
    with pytest.raises(streams.StreamError, match="Pass type="):
        OWNED._topic_spec("custom", None)
    with pytest.raises(TypeError, match="has the type"):
        OWNED._topic_spec("tokens", int)
    with pytest.raises(TypeError, match="already has its type"):
        OWNED._topic_spec(Events.turns, int)


def test_stream_without_definition_takes_any_topic():
    plain: PlainStream[Any] = PlainStream(id="s")
    assert plain._topic_spec("t", None) == ("t", None)
    assert plain._topic_spec(Events.turns, None) == ("turns", Turn)


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
        converter.from_payload(payload, streams.StreamHandle)


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
