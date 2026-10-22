from datetime import timedelta

import pytest

import temporalio.api.common.v1
import temporalio.api.streamservice.v1 as stream_pb
import temporalio.contrib.streams.proto.v1 as extension_pb
import temporalio.converter
import temporalio.nexus.system as nexus_system
from temporalio.contrib.streams.system import client, models


class FakeTransport:
    def __init__(self, *answers: stream_pb.ReadRecordsResponse) -> None:
        self.calls: list[tuple[str, temporalio.api.common.v1.Payload]] = []
        self.answers = list(answers)

    async def call(
        self,
        *,
        service: str,
        operation: str,
        input: temporalio.api.common.v1.Payload,
    ) -> temporalio.api.common.v1.Payload:
        assert service == "temporal.api.streamservice.v1.StreamService"
        self.calls.append((operation, input))
        answer = self.answers.pop(0)
        return temporalio.api.common.v1.Payload(
            metadata={
                "encoding": b"binary/protobuf",
                "messageType": answer.DESCRIPTOR.full_name.encode(),
            },
            data=answer.SerializeToString(),
        )


def request_of(
    payload: temporalio.api.common.v1.Payload, message: stream_pb.ReadRecordsRequest
) -> stream_pb.ReadRecordsRequest:
    message.ParseFromString(payload.data)
    return message


async def test_client_sends_the_request_as_a_proto():
    transport = FakeTransport(stream_pb.ReadRecordsResponse(next_offset=7))
    answer = await client.StreamServiceClient(transport).read_records(
        models.ReadRecordsRequest(
            stream=models.StreamRef(id="wf:run"), from_offset=5, topics=["a"]
        )
    )
    assert answer.next_offset == 7
    [(operation, payload)] = transport.calls
    assert operation == "ReadRecords"
    request = request_of(payload, stream_pb.ReadRecordsRequest())
    assert request.stream.id == "wf:run"
    assert request.from_offset == 5
    assert list(request.topics) == ["a"]


async def test_long_poll_carries_the_offset_until_records_come():
    record = stream_pb.StreamRecord(
        kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA, offset=9, topic="a"
    )
    transport = FakeTransport(
        stream_pb.ReadRecordsResponse(next_offset=8),
        stream_pb.ReadRecordsResponse(records=[record], next_offset=10),
    )
    answer = await client.StreamServiceClient(transport).read_records_until_records(
        models.ReadRecordsRequest(stream=models.StreamRef(id="wf:run"), from_offset=1),
        deadline=timedelta(seconds=5),
    )
    assert answer.records and answer.records[0].offset == 9
    first, second = (
        request_of(payload, stream_pb.ReadRecordsRequest())
        for _, payload in transport.calls
    )
    assert first.from_offset == 1
    assert second.from_offset == 8
    assert timedelta() < second.wait.ToTimedelta() <= timedelta(seconds=60)


def test_models_round_trip_through_the_proto():
    converter = models._AppendRecordsRequestTransferTypeConverter()
    request = models.AppendRecordsRequest(
        stream=models.StreamRef(id="wf:run"),
        records=[models.AppendRecord(topic="t", body={"n": 1})],
    )
    default = temporalio.converter.DataConverter.default
    with nexus_system._user_converter_context(
        nexus_system._SystemNexusUserConverters(
            payload_converter=default.payload_converter,
            failure_converter=default.failure_converter,
        )
    ):
        message = converter.to_transfer_type(request)
        back = converter.from_transfer_type(message, models.AppendRecordsRequest)
    assert isinstance(message, stream_pb.AppendRecordsRequest)
    assert message.records[0].body.metadata["encoding"] == b"json/plain"
    assert back.stream == request.stream
    assert back.records and back.records[0].body == {"n": 1}


def test_extension_protos_reference_the_service_protos():
    call = extension_pb.WorkflowCall(
        append=stream_pb.AppendRecordsRequest(stream=stream_pb.StreamRef(id="wf:run")),
        when_run_closes=False,
    )
    request = extension_pb.BufferWorkflowCallsRequest(run_id="run", calls=[call])
    parsed = extension_pb.BufferWorkflowCallsRequest.FromString(
        request.SerializeToString()
    )
    assert parsed.calls[0].append.stream.id == "wf:run"


async def test_append_stub_runs_only_in_a_workflow():
    from temporalio.contrib.streams.system import append_records

    with pytest.raises(Exception):
        await append_records(stream=models.StreamRef(id="wf:run"))
