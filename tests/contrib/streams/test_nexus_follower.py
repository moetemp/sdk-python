"""Following a stream from a Worker through a standalone operation's callbacks.

The unit tests drive :class:`StreamFollowerService` with a fake transport, as
the server would call it. The live tests need a server that relays progress
to a standalone operation's NexusHandler callbacks (U5), and skip on one that
doesn't. The streams live lane fails on those skips.
"""

from __future__ import annotations

import asyncio
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, Mock

import nexusrpc
import nexusrpc.handler
import pytest

import temporalio.api.workflowservice.v1
import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef
from temporalio.contrib.streams.nexus import (
    FollowerBatch,
    FollowOutcome,
    ReadInput,
    ReadOutput,
    RecordWire,
    StoreTransport,
    StreamFollowerService,
    follow_stream,
)
from temporalio.contrib.streams.nexus._generated import (
    StreamOperationToken,
)
from temporalio.contrib.streams.nexus._generated.models import RecordWireSuperseded
from temporalio.exceptions import ApplicationError
from temporalio.nexus.system.notification_service import (
    NexusOperationProgress,
    OnCompleteRequest,
    OnCompleteRequestResultFailure,
    OnCompleteRequestResultSuccess,
    OnProgressRequest,
)
from temporalio.service import ServiceClient
from temporalio.worker._nexus import _NexusTaskCancellation

REF = StreamRef.for_workflow("owner-1", topic="tokens")
CONVERTER = temporalio.converter.DataConverter.default
# The message a live test skips with when the server never calls the follower.
NO_HANDLER_CALLBACKS = "no NexusHandler callbacks on a standalone operation"


def _token(ref: StreamRef = REF) -> str:
    return StreamOperationToken(v=2, ref=ref, attach="a", chain="c").to_token()


def _wire(value: Any, *, producer: str = "p", attempt: int = 1) -> bytes:
    record = WireRecord(
        topic=REF.topic,
        kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
        producer_id=producer,
        attempt=attempt,
    )
    record.body.CopyFrom(CONVERTER.payload_converter.to_payload(value))
    return record.SerializeToString()


@dataclass
class _FakeTransport:
    """A stream service over a list, paged as Core pages it."""

    records: list[RecordWire]
    closed: bool = False
    state: bytes = b"core-state"
    # Whether the store keeps appending, so a read never comes back empty.
    busy: bool = False
    requests: list[ReadInput] = field(default_factory=list)

    async def read(self, request: ReadInput) -> ReadOutput:
        self.requests.append(request)
        start = 0
        if request.after_token:
            start = [r.token for r in self.records].index(request.after_token) + 1
        page = self.records[start : start + (request.max_records or 100)]
        at_end = start + len(page) >= len(self.records)
        done = self.closed and at_end
        if not page and self.busy:
            # A record lands while the read waits.
            await asyncio.sleep(0.01)
            n = len(self.records) + 1
            self.records.append(RecordWire(token=f"c{n}", record=_wire(f"v{n}")))
            page = self.records[-1:]
        return ReadOutput(
            records=page,
            next_token=page[-1].token if page else request.after_token or "",
            done=done,
            state=self.state,
        )


def _stored(count: int, offset: int = 0) -> list[RecordWire]:
    return [
        RecordWire(token=f"c{n}", record=_wire(f"v{n}"))
        for n in range(1 + offset, count + 1 + offset)
    ]


class _Collector:
    def __init__(self) -> None:
        self.batches: list[FollowerBatch[str]] = []
        self.outcomes: list[FollowOutcome] = []

    async def on_records(self, batch: FollowerBatch[str]) -> None:
        self.batches.append(batch)

    async def on_close(self, outcome: FollowOutcome) -> None:
        self.outcomes.append(outcome)

    @property
    def values(self) -> list[str]:
        return [r.value for b in self.batches for r in b.records if r.value]


def _service(
    transport: _FakeTransport, collector: _Collector, **kwargs: Any
) -> StreamFollowerService:
    client = Mock(data_converter=CONVERTER, namespace="default")
    return StreamFollowerService(
        {"orders": collector}, client=client, transport=transport, **kwargs
    )


def _context(left: timedelta = timedelta(seconds=10)) -> Any:
    return nexusrpc.handler.StartOperationContext(
        service="temporal.api.notificationservice.v1.NotificationService",
        operation="OnProgress",
        headers={},
        task_cancellation=_NexusTaskCancellation(),
        request_id="delivery-1",
        request_deadline=datetime.now(timezone.utc) + left,
    )


def _source_context(**extra: Any) -> dict[str, Any]:
    # What the Worker hands over after it decodes follow_stream's context.
    return {"follower": "orders", **extra}


def _progress(position: str, state: bytes | None = None) -> OnProgressRequest:
    return OnProgressRequest(
        progress=NexusOperationProgress(position=position, counter=1),
        source_context=_source_context(),
        operation_token=_token(),
        receiver_state=state,
    )


async def test_a_follower_gets_each_page_up_to_the_position() -> None:
    transport, collector = _FakeTransport(_stored(5)), _Collector()

    answer = await _service(transport, collector, max_records=2).on_progress(
        _context(), _progress("c5")
    )

    assert [len(b.records) for b in collector.batches] == [2, 2, 1]
    assert collector.values == ["v1", "v2", "v3", "v4", "v5"]
    assert [r.cursor.token for r in collector.batches[0].records] == ["c1", "c2"]
    assert collector.batches[-1].cursor.token == "c5"
    assert not answer.behind
    state = CONVERTER.payload_converter.from_payload(
        Payload(
            metadata={"encoding": b"json/plain"}, data=answer.receiver_state or b""
        ),
        ReadInput,
    )
    assert state.after_token == "c5"
    assert state.state == b"core-state"
    assert state.stream == REF
    # The reads carry the state each answer gave, up to the empty one.
    assert [r.state for r in transport.requests] == [None] + [b"core-state"] * 3


class _ContextRecordingCodec(
    temporalio.converter.PayloadCodec, temporalio.converter.WithSerializationContext
):
    """Passes payloads through and keeps each context it was given."""

    def __init__(self, seen: list[temporalio.converter.SerializationContext]) -> None:
        self.seen = seen

    def with_context(
        self, context: temporalio.converter.SerializationContext
    ) -> _ContextRecordingCodec:
        self.seen.append(context)
        return self

    async def encode(self, payloads: Any) -> list[Payload]:
        return list(payloads)

    async def decode(self, payloads: Any) -> list[Payload]:
        return list(payloads)


async def test_a_follower_decodes_an_activity_stream_with_the_activitys_context() -> (
    None
):
    seen: list[temporalio.converter.SerializationContext] = []
    converter = dataclasses.replace(
        CONVERTER, payload_codec=_ContextRecordingCodec(seen)
    )
    collector = _Collector()
    service = StreamFollowerService(
        {"orders": collector},
        client=Mock(data_converter=converter, namespace="default"),
        transport=_FakeTransport(_stored(2)),
    )
    ref = StreamRef.for_activity("tool-1", run_id="run-1", topic="tokens")

    await service.on_progress(
        _context(),
        OnProgressRequest(
            progress=NexusOperationProgress(position="c2", counter=1),
            source_context=_source_context(),
            operation_token=_token(ref),
        ),
    )

    assert collector.values == ["v1", "v2"]
    assert set(seen) == {
        temporalio.converter.ActivitySerializationContext(
            namespace="default",
            activity_id="tool-1",
            activity_type=None,
            activity_task_queue=None,
            workflow_id=None,
            workflow_type=None,
            is_local=False,
        )
    }


async def test_a_follower_answers_behind_when_its_budget_runs_out() -> None:
    transport, collector = _FakeTransport(_stored(6)), _Collector()
    service = _service(transport, collector, max_records=2)

    first = await service.on_progress(_context(timedelta(0)), _progress("c6"))
    assert first.behind
    assert collector.values == ["v1", "v2"]

    second = await service.on_progress(
        _context(), _progress("c6", first.receiver_state)
    )
    assert not second.behind
    assert collector.values == ["v1", "v2", "v3", "v4", "v5", "v6"]


async def test_a_follower_reads_past_the_position_until_a_read_finds_nothing() -> None:
    """Records can land after the progress the server sent. The follower reads
    on until a read that waited finds nothing, since the next append wakes it."""
    transport, collector = _FakeTransport(_stored(5)), _Collector()

    answer = await _service(transport, collector, max_records=2).on_progress(
        _context(), _progress("c2")
    )

    assert collector.values == ["v1", "v2", "v3", "v4", "v5"]
    assert not answer.behind
    assert all(r.wait_ms == 1000 for r in transport.requests)
    assert len(transport.requests) == 4


async def test_a_follower_on_a_busy_stream_answers_behind() -> None:
    """A stream that never goes quiet keeps the follower reading, so it answers
    behind at half its budget and the server sends the progress again."""
    transport = _FakeTransport(_stored(1), busy=True)
    collector = _Collector()

    answer = await _service(transport, collector).on_progress(
        _context(timedelta(milliseconds=400)), _progress("c1")
    )

    assert answer.behind
    assert collector.values[0] == "v1" and len(collector.values) > 1
    # Each wait is cut to what is left of the budget.
    assert all(0 <= (r.wait_ms or 0) <= 200 for r in transport.requests)


async def test_the_completion_drains_the_rest() -> None:
    transport, collector = _FakeTransport(_stored(2), closed=True), _Collector()

    await _service(transport, collector).on_complete(
        _context(),
        OnCompleteRequest(
            result=OnCompleteRequestResultSuccess("finished"),
            source_context=_source_context(),
            receiver_state=CONVERTER.payload_converter.to_payload(
                ReadInput(stream=REF)
            ).data,
        ),
    )

    assert collector.values == ["v1", "v2"]
    assert [r.wait_ms for r in transport.requests] == [0]


async def test_a_lost_answer_repeats_the_batch_with_its_cursors() -> None:
    transport, collector = _FakeTransport(_stored(4)), _Collector()
    service = _service(transport, collector)
    first = await service.on_progress(_context(), _progress("c2"))
    transport.records.extend(_stored(2, offset=4))

    # The server never got the answer, so it sends the last state it took.
    await service.on_progress(_context(), _progress("c6"))
    await service.on_progress(_context(), _progress("c6"))

    _ = first
    tokens = [[r.cursor.token for r in b.records] for b in collector.batches]
    assert tokens[1] == tokens[2] == ["c1", "c2", "c3", "c4", "c5", "c6"]


async def test_the_completion_reads_the_rest_then_closes() -> None:
    transport, collector = _FakeTransport(_stored(3)), _Collector()
    service = _service(transport, collector)
    taken = await service.on_progress(_context(), _progress("c3"))
    transport.records.extend(_stored(2, offset=3))
    transport.closed = True

    await service.on_complete(
        _context(),
        OnCompleteRequest(
            result=OnCompleteRequestResultSuccess("finished"),
            source_context=_source_context(),
            receiver_state=taken.receiver_state,
        ),
    )

    assert collector.values == ["v1", "v2", "v3", "v4", "v5"]
    [outcome] = collector.outcomes
    assert outcome.succeeded
    assert outcome.result == "finished"
    assert outcome.stream == REF


async def test_a_failed_operation_reaches_on_close_as_its_failure() -> None:
    transport, collector = _FakeTransport(_stored(1), closed=True), _Collector()
    service = _service(transport, collector)
    taken = await service.on_progress(_context(), _progress("c1"))

    await service.on_complete(
        _context(),
        OnCompleteRequest(
            result=OnCompleteRequestResultFailure(
                ApplicationError("owner failed", type="Boom")
            ),
            source_context=_source_context(),
            receiver_state=taken.receiver_state,
        ),
    )

    [outcome] = collector.outcomes
    assert not outcome.succeeded
    assert outcome.error is not None
    assert "owner failed" in str(outcome.error)


async def test_a_canceled_operation_closes_with_the_stream_still_open() -> None:
    transport, collector = _FakeTransport(_stored(3)), _Collector()
    service = _service(transport, collector)
    taken = await service.on_progress(_context(), _progress("c1"))

    await service.on_complete(
        _context(),
        OnCompleteRequest(
            result=OnCompleteRequestResultFailure(
                ApplicationError("canceled", type="Canceled")
            ),
            source_context=_source_context(),
            receiver_state=taken.receiver_state,
        ),
    )

    # The records the store holds still reach the follower, then the close.
    assert collector.values == ["v1", "v2", "v3"]
    [outcome] = collector.outcomes
    assert outcome.error is not None and "canceled" in str(outcome.error)


async def test_the_marks_stay_out_of_the_records() -> None:
    records = [
        RecordWire(token="c1", record=_wire("v1")),
        RecordWire(
            token="c2",
            record=b"",
            superseded=RecordWireSuperseded(
                producer_id="p", previous_attempt=1, attempt=2
            ),
        ),
        RecordWire(token="c3", record=_wire("v3", attempt=2)),
        RecordWire(token="c4", record=_wire("late", attempt=1), stale=True),
        RecordWire(token="c5", record=b"", error="codec refused it"),
    ]
    transport, collector = _FakeTransport(records), _Collector()

    await _service(transport, collector).on_progress(_context(), _progress("c5"))

    [batch] = collector.batches
    assert [r.value for r in batch.records] == ["v1", "v3"]
    [supersession] = batch.supersessions
    assert (supersession.index, supersession.cursor.token) == (1, "c2")
    assert supersession.supersession.attempt == 2
    [stale] = batch.stale_records
    assert (stale.cursor.token, stale.attempt, stale.value) == ("c4", 1, "late")
    [error] = batch.errors
    assert error.cursor.token == "c5"


async def test_a_state_over_the_cap_keeps_the_cursor() -> None:
    transport, collector = _FakeTransport(_stored(2), state=b"x" * 9000), _Collector()

    answer = await _service(transport, collector).on_progress(
        _context(), _progress("c2")
    )

    assert len(answer.receiver_state or b"") <= 8 * 1024
    state = CONVERTER.payload_converter.from_payload(
        Payload(
            metadata={"encoding": b"json/plain"}, data=answer.receiver_state or b""
        ),
        ReadInput,
    )
    assert state.after_token == "c2"
    assert state.state is None


async def test_an_unknown_follower_is_refused() -> None:
    transport, collector = _FakeTransport(_stored(1)), _Collector()
    request = _progress("c1")
    request.source_context = _source_context(follower="nobody")

    with pytest.raises(nexusrpc.HandlerError) as raised:
        await _service(transport, collector).on_progress(_context(), request)

    assert raised.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    assert not raised.value.retryable


def test_a_service_needs_a_transport_or_a_client_with_a_store() -> None:
    with pytest.raises(ValueError, match="transport="):
        StreamFollowerService({"orders": _Collector()})
    client = Mock(spec=Client)
    client.config.return_value = {"plugins": []}
    with pytest.raises(ValueError, match="no stream store"):
        StreamFollowerService({"orders": _Collector()}, client=client)
    with pytest.raises(ValueError, match="no stream store"):
        StoreTransport(client)


async def test_follow_stream_starts_the_operation_with_the_follower_callback() -> None:
    captured: dict[str, Any] = {}

    async def start(req: Any, **_kwargs: Any) -> Any:
        captured["start"] = req
        return temporalio.api.workflowservice.v1.StartNexusOperationExecutionResponse(
            run_id="run"
        )

    service_client = Mock(spec=ServiceClient)
    service_client.workflow_service = Mock()
    service_client.workflow_service.start_nexus_operation_execution = AsyncMock(
        side_effect=start
    )
    service_client.config = Mock(identity="test-identity")
    client = Client(service_client=service_client, namespace="default")

    await follow_stream(
        client,
        endpoint="orders",
        service="OrderService",
        operation="order_stream",
        arg="order-1",
        follower="orders",
        task_queue="app",
        id="follow-1",
    )

    request = captured["start"]
    assert (request.endpoint, request.service, request.operation) == (
        "orders",
        "OrderService",
        "order_stream",
    )
    [callback] = request.completion_callbacks
    handler = callback.nexus_handler
    assert handler.task_queue_name == "app"
    assert (handler.operation, handler.progress_operation) == (
        "OnComplete",
        "OnProgress",
    )
    assert CONVERTER.payload_converter.from_payload(handler.source_context) == {
        "follower": "orders",
        "endpoint": "orders",
        "operation_id": "follow-1",
    }


# Live: a SANO follows a Workflow's stream for a Worker.


@nexusrpc.service(name="FollowedOrders")
class FollowedOrders:
    order_stream: nexusrpc.Operation[str, str]


class _LiveFollower(_Collector):
    def __init__(self, max_records: int = 100) -> None:
        super().__init__()
        self.max_records = max_records
        self.closed = asyncio.Event()
        self.seen: dict[str, str] = {}

    async def on_records(self, batch: FollowerBatch[str]) -> None:
        await super().on_records(batch)
        for record in batch.records:
            # Delivery is at least once, so repeats are dropped by cursor.
            self.seen.setdefault(record.cursor.token, record.value or "")

    async def on_close(self, outcome: FollowOutcome) -> None:
        await super().on_close(outcome)
        self.closed.set()

    def ordered(self) -> list[str]:
        return list(self.seen.values())
