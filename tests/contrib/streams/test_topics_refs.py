"""Typed topics and the stream reference that crosses process boundaries."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams import StreamOwnerKind
from temporalio.contrib.streams import (
    DEFAULT_TOPIC,
    StreamRef,
    StreamTopic,
    StreamUnsupportedError,
    topic,
)
from temporalio.contrib.streams._ref import owner_context, stream_address
from temporalio.contrib.streams._topic import (
    resolve_topic,
)
from temporalio.converter import ActivitySerializationContext, DataConverter

OUT = topic("out", dict)


def test_a_definition_carries_its_name_and_type():
    assert OUT == StreamTopic("out", dict)
    assert resolve_topic(OUT) == ("out", dict)
    assert resolve_topic("free", int) == ("free", int)
    assert resolve_topic(None) == (DEFAULT_TOPIC, None)


def test_topic_mistakes_are_value_errors():
    with pytest.raises(ValueError):
        topic("")
    with pytest.raises(ValueError):
        resolve_topic("")
    with pytest.raises(ValueError, match="already carries its type"):
        resolve_topic(OUT, dict)


def test_a_ref_names_a_workflow_stream():
    ref = StreamRef.for_workflow("wf", topic=OUT)
    assert ref == StreamRef("workflow", "wf", topic="out")
    assert StreamRef.for_workflow("wf").topic == DEFAULT_TOPIC
    assert ref.with_topic(None).topic == DEFAULT_TOPIC
    assert ref.with_topic("other").workflow_id == "wf"


def test_a_ref_names_a_standalone_activity_stream():
    ref = StreamRef.for_activity("act", run_id="run", topic=OUT)
    assert ref == StreamRef(
        "activity", "", topic="out", chain_run_id="run", activity_id="act"
    )
    assert StreamRef.for_activity("act").chain_run_id is None
    assert StreamRef.for_activity("act").topic == DEFAULT_TOPIC
    assert ref.with_topic("other").activity_id == "act"


def test_a_ref_names_a_workflow_activity_stream():
    ref = StreamRef.for_workflow_activity(
        "wf", run_id="run", activity_id="act", topic=OUT
    )
    assert ref == StreamRef(
        "workflow_activity", "wf", topic="out", chain_run_id="run", activity_id="act"
    )
    assert (
        StreamRef.for_workflow_activity("wf", run_id="run", activity_id="act").topic
        == DEFAULT_TOPIC
    )
    assert ref.with_topic("other").activity_id == "act"
    ref._require_supported()


def test_a_workflow_activity_ref_maps_to_its_address_and_context():
    ref = StreamRef.for_workflow_activity("wf", run_id="run", activity_id="act")
    address = stream_address("ns", ref, topic="t")
    assert address.owner_kind == StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY
    assert (address.namespace, address.workflow_id, address.run_id) == (
        "ns",
        "wf",
        "run",
    )
    assert (address.activity_id, address.topic) == ("act", "t")
    context = owner_context("ns", ref.workflow_id, ref.activity_id)
    assert context == ActivitySerializationContext(
        namespace="ns",
        activity_id="act",
        workflow_id="wf",
        workflow_type=None,
        activity_type=None,
        activity_task_queue=None,
        is_local=False,
    )
    # A standalone Activity has no Workflow in its context.
    standalone = StreamRef.for_activity("act", run_id="run")
    assert isinstance(
        context := owner_context("ns", standalone.workflow_id, standalone.activity_id),
        ActivitySerializationContext,
    )
    assert context.workflow_id is None


@pytest.mark.parametrize(
    "ref",
    [
        StreamRef.for_workflow("wf", topic=OUT, chain_run_id="run"),
        StreamRef.for_activity("act", run_id="run", topic=OUT),
        StreamRef.for_workflow_activity(
            "wf", run_id="run", activity_id="act", topic=OUT
        ),
    ],
    ids=["workflow", "activity", "workflow_activity"],
)
def test_a_ref_round_trips_through_the_default_converter(ref: StreamRef):
    converter = DataConverter.default.payload_converter
    payload = converter.to_payloads([ref])[0]
    assert converter.from_payloads([payload], [StreamRef])[0] == ref


@dataclass(frozen=True)
class _RefBeforeActivities:
    kind: str
    workflow_id: str
    topic: str = DEFAULT_TOPIC
    chain_run_id: str | None = None


def test_an_older_release_decodes_an_activity_ref_and_refuses_it_where_it_opens():
    converter = DataConverter.default.payload_converter
    payload = converter.to_payloads([StreamRef.for_activity("act", run_id="run")])[0]
    old = converter.from_payloads([payload], [_RefBeforeActivities])[0]
    assert old == _RefBeforeActivities("activity", "", DEFAULT_TOPIC, "run")
    # An older release took any kind but "workflow" as unsupported.
    assert old.kind != "workflow"


def test_a_ref_with_the_old_run_id_decodes_as_the_newest_chain():
    # The old field also ended a read at that run, which no read does now,
    # so it is dropped rather than read as chain_run_id.
    converter = DataConverter.default.payload_converter
    payload = Payload(
        metadata={"encoding": b"json/plain"},
        data=b'{"kind": "workflow", "workflow_id": "wf", "run_id": "r", "topic": "out"}',
    )
    ref = converter.from_payloads([payload], [StreamRef])[0]
    assert ref == StreamRef.for_workflow("wf", topic=OUT)


def test_a_ref_refuses_what_it_cannot_name():
    with pytest.raises(ValueError):
        StreamRef("workflow", "")
    with pytest.raises(ValueError):
        StreamRef("workflow", "wf", topic="")
    with pytest.raises(ValueError, match="has no activity_id"):
        StreamRef("workflow", "wf", activity_id="act")
    with pytest.raises(ValueError, match="needs an activity_id"):
        StreamRef("activity", "")
    with pytest.raises(ValueError, match="has no workflow_id"):
        StreamRef("activity", "wf", activity_id="act")
    with pytest.raises(ValueError, match="needs an activity_id"):
        StreamRef.for_activity("")
    with pytest.raises(ValueError, match="needs its workflow_id"):
        StreamRef("workflow_activity", "", chain_run_id="run", activity_id="act")
    with pytest.raises(ValueError, match="needs its activity_id"):
        StreamRef("workflow_activity", "wf", chain_run_id="run")
    with pytest.raises(ValueError, match="needs its chain_run_id"):
        StreamRef("workflow_activity", "wf", activity_id="act")
    with pytest.raises(ValueError, match="needs its chain_run_id"):
        StreamRef.for_workflow_activity("wf", run_id="", activity_id="act")


@pytest.mark.parametrize("kind", ["standalone", "nexus"])
def test_a_ref_from_a_later_release_decodes_and_fails_where_it_opens(kind: str):
    # Decoding happens while a Workflow Task applies its input, where a raise
    # fails the task on every retry. Opening the handle is where the caller
    # can catch it.
    converter = DataConverter.default.payload_converter
    payload = Payload(
        metadata={"encoding": b"json/plain"},
        data=f'{{"kind": "{kind}", "workflow_id": "wf", "topic": "out"}}'.encode(),
    )
    ref = converter.from_payloads([payload], [StreamRef])[0]
    assert ref.kind == kind
    with pytest.raises(StreamUnsupportedError, match="a Workflow or an Activity"):
        ref._require_supported()


@pytest.mark.parametrize(
    "name",
    ["x" * 257, "é" * 129, "a\x1fb", "line\nbreak", "\x00"],
)
def test_a_topic_name_that_is_too_long_or_has_a_control_character_is_refused(name: str):
    # Every store puts the name in keys, cursors and Core's manifest budget.
    with pytest.raises(ValueError):
        topic(name)
    with pytest.raises(ValueError):
        resolve_topic(name)


def test_a_topic_name_of_256_bytes_is_accepted():
    assert topic("x" * 256).name == "x" * 256
    assert resolve_topic("é" * 128)[0] == "é" * 128
