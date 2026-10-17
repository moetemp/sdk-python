"""Stream wakes as the server attaches them to a Workflow Task.

Each wake is an ``Any`` of ``StreamWake`` in a ``WorkflowTaskAttachments``
job. The close outcome in a wake holds what the producer's codec encoded, so
the Worker decodes it with the reader's codec before the instance sees it.
"""

from __future__ import annotations

from collections.abc import Sequence

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
from temporalio.converter import DataConverter, PayloadCodec


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


async def test_the_reader_codec_decodes_a_wake_close_result():
    wake = StreamWake(
        subscription_id="s", counter=3, closed=True, close_result=await encoded("done")
    )
    act = activation(packed(wake))
    await decode_activation(
        act, CONVERTER, decode_headers=False, storage_concurrency_limit=1
    )
    decoded = unpacked(act.jobs[0].workflow_task_attachments.attachments[0])
    assert decoded.close_result.metadata["encoding"] == b"json/plain"
    assert DataConverter.default.payload_converter.from_payloads(
        [decoded.close_result]
    ) == ["done"]
    assert (decoded.subscription_id, decoded.counter, decoded.closed) == ("s", 3, True)


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
        act, CONVERTER, decode_headers=False, storage_concurrency_limit=4
    )
    decoded = unpacked(act.jobs[0].workflow_task_attachments.attachments[0])
    [payload] = decoded.close_failure.application_failure_info.details.payloads
    assert DataConverter.default.payload_converter.from_payloads([payload]) == ["why"]
    assert decoded.close_failure.message == "boom"


async def test_attachments_of_other_types_pass_unchanged():
    other = packed(StreamReference(workflow_id="w"))
    wake = StreamWake(subscription_id="s", counter=1)
    act = activation(other, packed(wake))
    before = act.SerializeToString()
    await decode_activation(
        act, CONVERTER, decode_headers=False, storage_concurrency_limit=1
    )
    assert act.SerializeToString() == before
