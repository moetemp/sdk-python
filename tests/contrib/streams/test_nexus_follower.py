"""Following a stream from a Worker through a standalone operation's callbacks.

The unit tests drive :class:`StreamFollowerService` with a fake transport, as
the server would call it. The live tests need a server that relays progress
to a standalone operation's NexusHandler callbacks (U5), and skip on one that
doesn't. The streams live lane fails on those skips.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import nexusrpc
import nexusrpc.handler
import pytest

import temporalio.api.workflowservice.v1
import temporalio.converter
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams.v1 import StreamLink as StreamLinkProto
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef, get_stream_handle
from temporalio.contrib.streams._ref import to_reference
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    ClientTransport,
    FollowBatch,
    FollowOutcome,
    ReadInput,
    ReadOutput,
    RecordWire,
    StoreTransport,
    StreamFollowerService,
    StreamOperationHandler,
    TemporalStreamsHandler,
    follow_stream,
)
from temporalio.contrib.streams.nexus._generated import (
    FollowedWire,
    FollowedWireLinkState,
    FollowedWireRecord,
    FollowInput,
    FollowOutput,
    FollowSourceWire,
    StreamOperationToken,
)
from temporalio.contrib.streams.nexus._generated.models import RecordWireSuperseded
from temporalio.exceptions import ApplicationError
from temporalio.nexus.system.notification_service import (
    NexusOperationProgress,
    NotificationService,
    OnCompleteRequest,
    OnCompleteRequestResultFailure,
    OnCompleteRequestResultSuccess,
    OnProgressRequest,
    OnProgressResponse,
)
from temporalio.service import RPCError, RPCStatusCode, ServiceClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker._nexus import _NexusTaskCancellation
from tests.contrib.streams._support import connect_with
from tests.contrib.streams.test_nexus_stream_operation import (
    OwnerUntilDone,
    _skip_without_notifier,
    connect,
)
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker
from tests.helpers.nexus import end_standalone_operation, make_nexus_endpoint_name

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
        self.batches: list[FollowBatch[str]] = []
        self.outcomes: list[FollowOutcome] = []

    async def on_records(self, batch: FollowBatch[str]) -> None:
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

    async def on_records(self, batch: FollowBatch[str]) -> None:
        await super().on_records(batch)
        for record in batch.records:
            # Delivery is at least once, so repeats are dropped by cursor.
            self.seen.setdefault(record.cursor.token, record.value or "")

    async def on_close(self, outcome: FollowOutcome) -> None:
        await super().on_close(outcome)
        self.closed.set()

    def ordered(self) -> list[str]:
        return list(self.seen.values())


@dataclass
class _Lane:
    client: Client
    store: MemoryStreams
    task_queue: str
    endpoint: str
    ref: StreamRef
    followed: list[Any] = field(default_factory=list)
    transport: Any = None
    """How the follower reads. Its client's store when ``None``."""
    service_class: type[StreamFollowerService] = StreamFollowerService
    """The follower service the Worker runs, so a test can count its calls."""

    def handler(self) -> Any:
        ref = self.ref

        async def open_stream(_ctx: Any, _arg: str) -> StreamRef:
            # The test starts the owner with own(), since an append needs it.
            return ref

        @nexusrpc.handler.service_handler(service=FollowedOrders)
        class Orders:
            @nexusrpc.handler.operation_handler
            def order_stream(self) -> nexusrpc.handler.OperationHandler[str, str]:
                return StreamOperationHandler(open_stream)

        return Orders()

    @contextlib.asynccontextmanager
    async def worker(
        self, follower: _LiveFollower, *, hands_over: bool = False
    ) -> AsyncIterator[None]:
        """A Worker that follows with ``follower``.

        Unless the block ``hands_over`` the operations to a later Worker, they
        end before the Worker stops, so a test that skips or fails leaves no
        callback retried against a task queue no one polls.
        """
        async with Worker(
            self.client,
            task_queue=self.task_queue,
            workflows=[OwnerUntilDone],
            nexus_service_handlers=[
                self.handler(),
                self.service(follower),
                # The stream service, for a transport that reads through the endpoint.
                TemporalStreamsHandler(),
            ],
        ):
            try:
                yield
            except BaseException:
                await self.end(follower)
                raise
            if not hands_over:
                await self.end(follower)

    def service(self, follower: _LiveFollower) -> StreamFollowerService:
        return self.service_class(
            {"orders": follower},
            client=self.client,
            transport=self.transport,
            max_records=follower.max_records,
        )

    async def end(self, follower: _LiveFollower) -> None:
        for handle in self.followed:
            await end_standalone_operation(self.client, handle, follower.closed.is_set)

    async def own(self) -> None:
        # The notifier is keyed by the owner's run chain, so the owner runs.
        await self.client.start_workflow(
            OwnerUntilDone.run, id=self.ref.workflow_id, task_queue=self.task_queue
        )

    async def follow(self) -> Any:
        try:
            handle = await follow_stream(
                self.client,
                endpoint=self.endpoint,
                service=FollowedOrders,
                operation=FollowedOrders.order_stream,
                arg="order-1",
                follower="orders",
                task_queue=self.task_queue,
                schedule_to_close_timeout=timedelta(minutes=2),
            )
        except RPCError as err:
            if err.status == RPCStatusCode.INVALID_ARGUMENT and "allback" in str(err):
                pytest.skip(f"{NO_HANDLER_CALLBACKS}: {err}")
            raise
        self.followed.append(handle)
        return handle

    async def close(
        self, result: Any = None, error: BaseException | None = None
    ) -> None:
        await self.store.close_stream(self.client, self.ref, result, error=error)
        await self.client.get_workflow_handle(self.ref.workflow_id).signal(
            OwnerUntilDone.done
        )


async def _lane(client: Client, env: WorkflowEnvironment) -> _Lane:
    if env.supports_time_skipping:
        pytest.skip("Standalone Nexus Operation tests don't work with time-skipping")
    ref = StreamRef.for_workflow(f"owner-{uuid.uuid4()}", topic="tokens")
    await _skip_without_notifier(client, ref)
    task_queue = f"follower-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    store = MemoryStreams().notify_on_append()
    return _Lane(await connect(client, store), store, task_queue, endpoint, ref)


async def _closed(follower: _LiveFollower, timeout: float = 30) -> None:
    try:
        await asyncio.wait_for(follower.closed.wait(), timeout)
    except asyncio.TimeoutError:
        pytest.skip(f"{NO_HANDLER_CALLBACKS}: the follower never closed")


async def _until(predicate: Any, timeout: float = 30) -> None:
    for _ in range(int(timeout * 10)):
        if predicate():
            return
        await asyncio.sleep(0.1)
    pytest.skip(f"{NO_HANDLER_CALLBACKS}: no progress reached the follower")


async def test_live_a_follower_gets_each_batch_then_the_close(
    client: Client, env: WorkflowEnvironment
) -> None:
    lane = await _lane(client, env)
    follower = _LiveFollower()
    async with lane.worker(follower):
        await lane.own()
        await lane.follow()
        producer = get_stream_handle(lane.client, lane.ref).producer(
            producer_id="writer", attempt=1
        )
        await producer.append("a")
        await _until(lambda: "a" in follower.ordered())
        await producer.append("b", "c")
        await _until(lambda: "c" in follower.ordered())
        await lane.close("done")
        await _closed(follower)

    assert follower.ordered() == ["a", "b", "c"]
    [outcome] = follower.outcomes
    assert outcome.result == "done"
    assert outcome.stream == lane.ref


async def test_live_a_follower_reads_through_the_client_transport(
    client: Client, env: WorkflowEnvironment
) -> None:
    lane = await _lane(client, env)
    lane.transport = ClientTransport(lane.client, lane.endpoint)
    follower = _LiveFollower()
    async with lane.worker(follower):
        await lane.own()
        await lane.follow()
        producer = get_stream_handle(lane.client, lane.ref).producer(
            producer_id="writer", attempt=1
        )
        await producer.append("a")
        await _until(lambda: "a" in follower.ordered())
        await producer.append("b", "c")
        await _until(lambda: "c" in follower.ordered())
        await lane.close("done")
        await _closed(follower)

    assert follower.ordered() == ["a", "b", "c"]
    [outcome] = follower.outcomes
    assert outcome.result == "done"


async def test_live_a_follower_resumes_on_another_worker(
    client: Client, env: WorkflowEnvironment
) -> None:
    lane = await _lane(client, env)
    first, second = _LiveFollower(), _LiveFollower()
    producer = get_stream_handle(lane.client, lane.ref).producer(
        producer_id="writer", attempt=1
    )
    async with lane.worker(first, hands_over=True):
        await lane.own()
        await lane.follow()
        await producer.append("a", "b")
        await _until(lambda: "b" in first.ordered())
        # The call answers after its last read waited a second and found
        # nothing. A Worker that stops before that loses the answer, and the
        # server sends the progress again from the earlier position.
        await asyncio.sleep(3)
    # The read position lives on the server's callback, so another Worker,
    # with nothing of the first one's, goes on from it.
    async with lane.worker(second):
        await producer.append("c")
        await _until(lambda: "c" in second.ordered())
        await lane.close("done")
        await _closed(second)

    assert first.ordered() == ["a", "b"]
    assert second.ordered() == ["c"]
    assert second.outcomes[0].result == "done"


async def test_live_a_late_follower_reads_the_backlog_in_pages(
    client: Client, env: WorkflowEnvironment
) -> None:
    lane = await _lane(client, env)
    follower = _LiveFollower(max_records=7)
    producer = get_stream_handle(lane.client, lane.ref).producer(
        producer_id="writer", attempt=1
    )
    async with lane.worker(follower):
        await lane.own()
        await producer.append(*[f"r{n}" for n in range(50)])
        await lane.follow()
        await _until(lambda: len(follower.ordered()) == 50)
        await lane.close("done")
        await _closed(follower)

    assert follower.ordered() == [f"r{n}" for n in range(50)]
    assert max(len(b.records) for b in follower.batches) <= 7
    assert len(follower.batches) >= 8


async def test_live_a_failed_owner_reaches_on_close_as_a_failure(
    client: Client, env: WorkflowEnvironment
) -> None:
    lane = await _lane(client, env)
    follower = _LiveFollower()
    async with lane.worker(follower):
        await lane.own()
        await lane.follow()
        producer = get_stream_handle(lane.client, lane.ref).producer(
            producer_id="writer", attempt=1
        )
        await producer.append("a")
        await _until(lambda: "a" in follower.ordered())
        await lane.close(error=RuntimeError("the owner gave up"))
        await _closed(follower)

    [outcome] = follower.outcomes
    assert not outcome.succeeded
    assert "the owner gave up" in str(outcome.error)
    assert follower.ordered() == ["a"]


async def test_each_progress_read_carries_the_wake_token() -> None:
    """An empty read with the operation's token asks the notifier to wake the
    callback after its position, so the follower hears of the next append."""
    transport, collector = _FakeTransport(_stored(3)), _Collector()

    answer = await _service(transport, collector, max_records=2).on_progress(
        _context(), _progress("c3")
    )

    assert collector.values == ["v1", "v2", "v3"]
    assert {r.wake_token for r in transport.requests} == {_token()}
    # The token rides on the read only, not in the state the server keeps.
    state = CONVERTER.payload_converter.from_payload(
        Payload(
            metadata={"encoding": b"json/plain"}, data=answer.receiver_state or b""
        ),
        ReadInput,
    )
    assert state.wake_token is None


async def test_the_completion_drain_sends_no_wake_token() -> None:
    transport, collector = _FakeTransport(_stored(1), closed=True), _Collector()

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

    assert [r.wake_token for r in transport.requests] == [None]


async def test_follow_stream_asks_for_the_first_wake_from_the_start() -> None:
    started: dict[str, Any] = {}

    class FakeNexusClient:
        async def start_operation(self, *_args: Any, **kwargs: Any) -> Any:
            started.update(kwargs)
            return object()

    client = Mock(create_nexus_client=Mock(return_value=FakeNexusClient()))
    await follow_stream(
        client,
        endpoint="orders",
        service=FollowedOrders,
        operation=FollowedOrders.order_stream,
        arg="order-1",
        follower="orders",
        task_queue="followers",
    )
    # The follower reads only when woken, so its idle callback asks for the
    # first wake at the attach.
    assert started["headers"] == {"temporal-stream-first-wake-after": "0"}


_progress_calls: list[str] = []


@nexusrpc.handler.service_handler(service=NotificationService)
class _CountingFollowerService(StreamFollowerService):
    @nexusrpc.handler.sync_operation
    async def on_progress(
        self, ctx: nexusrpc.handler.StartOperationContext, input: OnProgressRequest
    ) -> OnProgressResponse:
        _progress_calls.append(ctx.request_id)
        return await super().on_progress(ctx, input)


async def test_live_a_follower_on_an_empty_stream_hears_the_first_append(
    client: Client, env: WorkflowEnvironment
) -> None:
    lane = await _lane(client, env)
    follower = _LiveFollower()
    async with lane.worker(follower):
        await lane.own()
        await lane.follow()
        # Nothing is there yet, so the first wake waits for the first append.
        await asyncio.sleep(2)
        assert follower.ordered() == []
        await (
            get_stream_handle(lane.client, lane.ref)
            .producer(producer_id="writer", attempt=1)
            .append("first")
        )
        await _until(lambda: "first" in follower.ordered())
        await lane.close("done")
        await _closed(follower)
    assert follower.ordered() == ["first"]


async def test_live_a_follower_that_reads_a_burst_gets_no_extra_progress(
    client: Client, env: WorkflowEnvironment
) -> None:
    lane = await _lane(client, env)
    lane.service_class = _CountingFollowerService
    _progress_calls.clear()
    follower = _LiveFollower()
    async with lane.worker(follower):
        await lane.own()
        producer = get_stream_handle(lane.client, lane.ref).producer(
            producer_id="writer", attempt=1
        )
        texts = [f"t{n}" for n in range(20)]
        for text in texts:
            await producer.append(text)
        await lane.follow()
        await _until(lambda: "t19" in follower.ordered())
        # The callback waits only after a read found nothing, so the burst
        # costs the one first wake, not a delivery per append.
        await asyncio.sleep(3)
        calls = len(_progress_calls)
        await lane.close("done")
        await _closed(follower)
    assert follower.ordered() == texts
    assert calls == 1, _progress_calls


async def test_a_follower_decodes_a_tool_stream_with_the_activity_and_its_workflow() -> (
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
    ref = StreamRef.for_workflow_activity(
        "agent", run_id="run-1", activity_id="tool-1", topic="tokens"
    )

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
            workflow_id="agent",
            workflow_type=None,
            is_local=False,
        )
    }


class _FakeFollowTransport:
    """A stream service that answers follow with two scripted pages."""

    def __init__(self) -> None:
        self.requests: list[FollowInput] = []
        child = StreamRef.for_workflow_activity(
            "agent", run_id="run-1", activity_id="tool-1", topic="tokens"
        )
        link = StreamLinkProto(
            event=StreamLinkProto.Event.EVENT_OPEN,
            name="tool-1",
            stream=to_reference(child),
        ).SerializeToString()
        self.pages = [
            FollowOutput(
                records=[
                    FollowedWire(
                        source=0,
                        record=FollowedWireRecord(token="r1", record=_wire("d1")),
                    ),
                    FollowedWire(
                        source=1,
                        link_state=FollowedWireLinkState(kind="opened", link=link),
                    ),
                    FollowedWire(
                        source=1,
                        record=FollowedWireRecord(token="t1", record=_wire("a1")),
                    ),
                ],
                done=False,
                state=b"follow-state-1",
                sources=[
                    FollowSourceWire(
                        number=1, parent=0, depth=1, stream=child, name="tool-1"
                    )
                ],
            ),
            FollowOutput(records=[], done=False, state=b"follow-state-2", sources=[]),
        ]

    async def read(self, request: ReadInput) -> ReadOutput:
        raise AssertionError(f"a follower with follow_links follows, not {request}")

    async def follow(self, request: FollowInput) -> FollowOutput:
        self.requests.append(request)
        return self.pages.pop(0)


class _LinkCollector(_Collector):
    follow_links = True


async def test_a_follower_that_asks_for_links_gets_every_source_in_order() -> None:
    transport, collector = _FakeFollowTransport(), _LinkCollector()
    service = StreamFollowerService(
        {"orders": collector},
        client=Mock(data_converter=CONVERTER, namespace="default"),
        transport=cast(Any, transport),
    )

    answer = await service.on_progress(_context(), _progress("c1"))

    [batch] = collector.batches
    assert [r.value for r in batch.records] == ["d1", "a1"]
    assert [
        (
            i.depth,
            i.link_state.kind.name if i.link_state else i.record and i.record.value,
        )
        for i in batch.items
    ] == [(0, "d1"), (1, "OPENED"), (1, "a1")]
    assert batch.items[1].source.activity_id == "tool-1"
    assert batch.items[-1].position is not None
    assert batch.cursor.token == "r1"
    state = CONVERTER.payload_converter.from_payload(
        Payload(
            metadata={"encoding": b"json/plain"}, data=answer.receiver_state or b""
        ),
        FollowInput,
    )
    # The follow state holds every open link, so it carries the position.
    assert state.state == b"follow-state-2"
    assert transport.requests[1].state == b"follow-state-1"


async def test_a_canceled_follow_of_links_closes_with_the_stream_still_open() -> None:
    transport, collector = _FakeFollowTransport(), _LinkCollector()
    service = StreamFollowerService(
        {"orders": collector},
        client=Mock(data_converter=CONVERTER, namespace="default"),
        transport=cast(Any, transport),
    )

    await service.on_complete(
        _context(),
        OnCompleteRequest(
            result=OnCompleteRequestResultFailure(
                ApplicationError("canceled", type="Canceled")
            ),
            source_context=_source_context(),
            receiver_state=CONVERTER.payload_converter.to_payload(
                FollowInput(stream=REF)
            ).data,
        ),
    )

    # Both scripted pages are read, the second one empty, and none is done.
    assert [r.value for b in collector.batches for r in b.records] == ["d1", "a1"]
    [outcome] = collector.outcomes
    assert outcome.error is not None and "canceled" in str(outcome.error)


async def test_the_store_transport_follows_links_through_core(client: Client) -> None:
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, Waits) as worker:
        agent = await running_owner(streams, worker.task_queue)
        child = await running_owner(streams, worker.task_queue)
        child_ref = StreamRef.for_workflow(child, topic="tokens")
        await (
            get_stream_handle(streams, child_ref)
            .producer(producer_id="tool", attempt=1)
            .append("a1", "a2")
        )
        parent = get_stream_handle(streams, agent, topic="tokens").producer(
            producer_id="agent", attempt=1
        )
        await parent.append("d1")
        await parent.link(child_ref, name="sub")
        answer = await StoreTransport(streams).follow(
            FollowInput(stream=StreamRef.for_workflow(agent, topic="tokens"), wait_ms=0)
        )
        for owner in (agent, child):
            await streams.get_workflow_handle(owner).signal(Waits.finish)
    sources = {s.number: s.stream for s in answer.sources}
    seen = [
        (f.source, f.link_state.kind if f.link_state else f.record and f.record.token)
        for f in answer.records
    ]
    assert seen[0][0] == 0 and seen[1][0] == 0
    assert [s for s, _ in seen].count(0) == 2
    child_items = [f for f in answer.records if f.source != 0]
    assert child_items[0].link_state is not None
    assert child_items[0].link_state.kind == "opened"
    assert [f.record is not None for f in child_items[1:]] == [True, True]
    assert sources[child_items[0].source].workflow_id == child
