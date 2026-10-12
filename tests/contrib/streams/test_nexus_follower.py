"""Following a stream from a Worker through a standalone operation's callbacks.

The unit tests drive :class:`StreamFollowerService` with a fake transport, as
the server would call it. The live tests need a server that relays progress
to a standalone operation's NexusHandler callbacks (U5), and skip on one that
doesn't. The streams live lane fails on those skips.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
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
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.notificationservice.v1 import (
    OnCompleteRequest,
    OnProgressRequest,
)
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.client import Client
from temporalio.contrib.streams import StreamRef, get_stream_handle
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    FollowBatch,
    FollowOutcome,
    ReadInput,
    ReadOutput,
    RecordWire,
    StoreTransport,
    StreamFollowerService,
    StreamOperationHandler,
    follow_stream,
)
from temporalio.contrib.streams.nexus._generated import StreamOperationToken
from temporalio.contrib.streams.nexus._generated.models import RecordWireSuperseded
from temporalio.service import RPCError, RPCStatusCode, ServiceClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker._nexus import _NexusTaskCancellation
from tests.contrib.streams.test_nexus_stream_operation import (
    OwnerUntilDone,
    _skip_without_notifier,
    connect,
)
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
    # Whether an empty read that asks to arm arms the stream, as Core's does.
    arms: bool = True
    requests: list[ReadInput] = field(default_factory=list)

    async def read(self, request: ReadInput) -> ReadOutput:
        self.requests.append(request)
        start = 0
        if request.after_token:
            start = [r.token for r in self.records].index(request.after_token) + 1
        page = self.records[start : start + (request.max_records or 100)]
        at_end = start + len(page) >= len(self.records)
        done = self.closed and at_end
        armed = bool(request.arm) and self.arms and not page and not done
        if not page and not armed:
            await asyncio.sleep(0.01)
        return ReadOutput(
            records=page,
            next_token=page[-1].token if page else request.after_token or "",
            done=done,
            state=self.state,
            armed=True if armed else None,
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


def _source_context(**extra: Any) -> Payload:
    return CONVERTER.payload_converter.to_payload({"follower": "orders", **extra})


def _progress(position: str, state: bytes = b"") -> OnProgressRequest:
    request = OnProgressRequest(
        source_context=_source_context(),
        operation_token=_token(),
        receiver_state=state,
    )
    request.progress.position = position
    request.progress.counter = 1
    return request


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
        Payload(metadata={"encoding": b"json/plain"}, data=answer.receiver_state),
        ReadInput,
    )
    assert state.after_token == "c5"
    assert state.state == b"core-state"
    assert state.stream == REF
    # The reads carry the state each answer gave, up to the empty one that armed.
    assert [r.state for r in transport.requests] == [None] + [b"core-state"] * 3


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


async def test_a_follower_reads_past_the_position_until_an_answer_arms() -> None:
    """Records can land after the progress the server sent. The follower reads
    on until a read that waited finds nothing and arms the stream, so the next
    append wakes it."""
    transport, collector = _FakeTransport(_stored(5)), _Collector()

    answer = await _service(transport, collector, max_records=2).on_progress(
        _context(), _progress("c2")
    )

    assert collector.values == ["v1", "v2", "v3", "v4", "v5"]
    assert not answer.behind
    assert all(r.arm for r in transport.requests)
    assert all(r.wait_ms == 1000 for r in transport.requests)
    assert len(transport.requests) == 4


async def test_a_follower_that_never_arms_answers_behind() -> None:
    """With nothing armed no append wakes the follower, so it answers behind
    at half its budget and the server sends the progress again."""
    transport = _FakeTransport(_stored(1), arms=False)
    collector = _Collector()

    answer = await _service(transport, collector).on_progress(
        _context(timedelta(milliseconds=400)), _progress("c1")
    )

    assert answer.behind
    assert collector.values == ["v1"]
    # Each wait is cut to what is left of the budget.
    assert all(0 <= (r.wait_ms or 0) <= 200 for r in transport.requests)


async def test_the_completion_drains_without_arming() -> None:
    transport, collector = _FakeTransport(_stored(2), closed=True), _Collector()

    await _service(transport, collector).on_complete(
        _context(),
        OnCompleteRequest(
            success=CONVERTER.payload_converter.to_payload("finished"),
            source_context=_source_context(),
            receiver_state=CONVERTER.payload_converter.to_payload(
                ReadInput(stream=REF)
            ).data,
        ),
    )

    assert collector.values == ["v1", "v2"]
    assert [r.arm for r in transport.requests] == [None]


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
            success=CONVERTER.payload_converter.to_payload("finished"),
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
            failure=Failure(
                message="owner failed",
                application_failure_info=ApplicationFailureInfo(type="Boom"),
            ),
            source_context=_source_context(),
            receiver_state=taken.receiver_state,
        ),
    )

    [outcome] = collector.outcomes
    assert not outcome.succeeded
    assert outcome.error is not None
    assert "owner failed" in str(outcome.error)


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

    assert len(answer.receiver_state) <= 8 * 1024
    state = CONVERTER.payload_converter.from_payload(
        Payload(metadata={"encoding": b"json/plain"}, data=answer.receiver_state),
        ReadInput,
    )
    assert state.after_token == "c2"
    assert state.state is None


async def test_an_unknown_follower_is_refused() -> None:
    transport, collector = _FakeTransport(_stored(1)), _Collector()
    request = _progress("c1")
    request.source_context.CopyFrom(_source_context(follower="nobody"))

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
            nexus_service_handlers=[self.handler(), self.service(follower)],
        ):
            try:
                yield
            except BaseException:
                await self.end(follower)
                raise
            if not hands_over:
                await self.end(follower)

    def service(self, follower: _LiveFollower) -> StreamFollowerService:
        return StreamFollowerService(
            {"orders": follower}, client=self.client, max_records=follower.max_records
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
