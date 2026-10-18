"""Stream wakes as the server attaches them to a Workflow Task.

Each wake is an ``Any`` of ``StreamWake`` in a ``WorkflowTaskAttachments``
job. The close failure in a wake holds what the stream owner's codec
encoded, so the Worker decodes it in the owner's context before the instance
sees it. The fold must give the results of the sdk-streams vectors.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import pytest
from google.protobuf.any_pb2 import Any as AnyProto

from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.stream.v1 import StreamReference, StreamWake
from temporalio.bridge.proto.workflow_activation import (
    WorkflowActivation,
    WorkflowActivationJob,
    WorkflowTaskAttachments,
)
from temporalio.bridge.worker import decode_activation
from temporalio.contrib.streams._ref import owner_context
from temporalio.contrib.streams._wakes import fold_stream_wakes
from temporalio.converter import (
    ActivitySerializationContext,
    DataConverter,
    PayloadCodec,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)
from tests.contrib.streams._support import streams_crate_dir


class XorCodec(PayloadCodec):
    """Encodes by a byte flip, so an undecoded payload does not parse."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/xor"},
                data=bytes(b ^ 0x5A for b in p.SerializeToString()),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload.FromString(bytes(b ^ 0x5A for b in p.data))
            if p.metadata.get("encoding") == b"binary/xor"
            else p
            for p in payloads
        ]


CONVERTER = DataConverter(payload_codec=XorCodec())


class OwnerKeyCodec(PayloadCodec, WithSerializationContext):
    """Encodes under a key from the context, so only the same context decodes."""

    def __init__(self, key: str = "") -> None:
        self.key = key

    def with_context(self, context: SerializationContext | None) -> OwnerKeyCodec:
        if isinstance(context, ActivitySerializationContext):
            return OwnerKeyCodec(f"activity/{context.activity_id}")
        if isinstance(context, WorkflowSerializationContext):
            return OwnerKeyCodec(f"workflow/{context.workflow_id}")
        return OwnerKeyCodec()

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/keyed", "key": self.key.encode()},
                data=p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        for p in payloads:
            assert p.metadata["key"] == self.key.encode(), "decoded in another context"
        return [Payload.FromString(p.data) for p in payloads]


async def encoded(value: str) -> Payload:
    [payload] = await CONVERTER.encode([value])
    return payload


def activation(*attachments: AnyProto) -> WorkflowActivation:
    act = WorkflowActivation(run_id="run")
    job = WorkflowActivationJob(
        workflow_task_attachments=WorkflowTaskAttachments(attachments=attachments)
    )
    act.jobs.append(job)
    return act


def packed(message: StreamWake | StreamReference) -> AnyProto:
    attachment = AnyProto()
    attachment.Pack(message)
    return attachment


def unpacked(attachment: AnyProto) -> StreamWake:
    wake = StreamWake()
    assert attachment.Unpack(wake)
    return wake


async def test_a_wake_close_result_is_left_as_sent():
    # No reader reads a close result from a wake, so the Worker never decodes it.
    wake = StreamWake(
        subscription_id="s", counter=3, closed=True, close_result=await encoded("done")
    )
    act = activation(packed(wake))
    before = act.SerializeToString()
    await decode_activation(
        act,
        CONVERTER,
        decode_headers=False,
        storage_concurrency_limit=1,
        stream_wake_converter=lambda _: CONVERTER,
    )
    assert act.SerializeToString() == before


async def test_the_reader_codec_decodes_a_wake_close_failure():
    details = await encoded("why")
    failure = Failure(
        message="boom",
        application_failure_info=ApplicationFailureInfo(type="Bad"),
    )
    failure.application_failure_info.details.payloads.append(details)
    wake = StreamWake(subscription_id="s", closed=True, close_failure=failure)
    act = activation(packed(wake))
    await decode_activation(
        act,
        CONVERTER,
        decode_headers=False,
        storage_concurrency_limit=4,
        stream_wake_converter=lambda _: CONVERTER,
    )
    decoded = unpacked(act.jobs[0].workflow_task_attachments.attachments[0])
    [payload] = decoded.close_failure.application_failure_info.details.payloads
    assert DataConverter.default.payload_converter.from_payloads([payload]) == ["why"]
    assert decoded.close_failure.message == "boom"


async def test_a_wake_close_failure_decodes_in_the_owner_context():
    converter = DataConverter(payload_codec=OwnerKeyCodec())
    owner = owner_context("default", "owner-wf", None)
    [details] = await converter.with_context(owner).encode(["why"])
    failure = Failure(message="boom", application_failure_info=ApplicationFailureInfo())
    failure.application_failure_info.details.payloads.append(details)
    known = StreamWake(subscription_id="1", closed=True, close_failure=failure)
    other = StreamWake(subscription_id="2", closed=True, close_failure=failure)
    act = activation(packed(known), packed(other))
    reader = converter.with_context(
        WorkflowSerializationContext(namespace="default", workflow_id="reader-wf")
    )
    await decode_activation(
        act,
        reader,
        decode_headers=False,
        storage_concurrency_limit=1,
        stream_wake_converter=lambda sub: (
            converter.with_context(owner) if sub == "1" else None
        ),
    )
    attachments = act.jobs[0].workflow_task_attachments.attachments
    [payload] = unpacked(
        attachments[0]
    ).close_failure.application_failure_info.details.payloads
    assert DataConverter.default.payload_converter.from_payloads([payload]) == ["why"]
    # A wake whose subscription registered no context keeps its failure encoded.
    assert unpacked(attachments[1]) == other


async def test_attachments_of_other_types_pass_unchanged():
    other = packed(StreamReference(workflow_id="w"))
    wake = StreamWake(subscription_id="s", counter=1)
    act = activation(other, packed(wake))
    before = act.SerializeToString()
    await decode_activation(
        act, CONVERTER, decode_headers=False, storage_concurrency_limit=1
    )
    assert act.SerializeToString() == before


def _fold_cases() -> list[dict[str, Any]]:
    with open(streams_crate_dir() / "testdata" / "wake_folds.json") as f:
        return json.load(f)


def _wake(fields: dict[str, Any]) -> StreamWake:
    # The vectors give a close result by its data and a close failure by its
    # message.
    fields = dict(fields)
    result = fields.pop("close_result", None)
    failure = fields.pop("close_failure", None)
    wake = StreamWake(**fields)
    if result is not None:
        wake.close_result.CopyFrom(Payload(data=result.encode()))
    if failure is not None:
        wake.close_failure.CopyFrom(Failure(message=failure))
    return wake


@pytest.mark.parametrize("case", _fold_cases(), ids=lambda case: case["name"])
def test_the_fold_gives_the_shared_vectors(case: dict[str, Any]):
    folded = fold_stream_wakes([_wake(w) for w in case["wakes"]])
    assert folded == [_wake(w) for w in case["folded"]]
