"""Following links inside Workflow code, one Local Activity per step.

A Workflow follower is the Workflow reader's shape over the stream runtime's ``Follow``.
Each step runs one ``Follow`` in a Local Activity. With the running Workflow
as the subscriber, the stream runtime attaches each source that goes idle, and an empty
step asks each idle source's notifier for one wake past its counter and
answers those counters. A step that finds items asks for nothing, so a busy
follower gets no wakes. The follower then waits,
with no Local Activity and no timer, until a wake of any of its sources
moves past its counter, asks for a recheck, closes, or asks to attach again.

With ``replay_from_store``, the marker holds the step's ranges, its input
state and its digest in place of the records, and replay reads them again
with the stream runtime's ``FollowRange`` before Workflow code sees the batch.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Generic, TypeVar, overload

import temporalio.activity
from temporalio import workflow
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.api.failure.v1 import Failure
from temporalio.bridge.proto.streams import (
    FollowRangeRequest,
    FollowRequest,
    FollowResponse,
    FollowSubscriber,
    StreamAddress,
    UnsubscribeRequest,
)
from temporalio.common import RawValue
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._errors import (
    StreamReplayMismatchError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._follow import (
    FollowedItem,
    FollowPosition,
    FollowSourceInfo,
    decode_follow,
    follow_items,
    update_sources,
)
from temporalio.contrib.streams._handles import StreamHandle
from temporalio.contrib.streams._local_activity import require_local_activity
from temporalio.contrib.streams._plugin import call, store_for_client
from temporalio.contrib.streams._record import Cursor
from temporalio.contrib.streams._ref import StreamRef, from_address
from temporalio.contrib.streams._replay_reads import FOLLOW_POSITIONS_TYPE
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wakes import wake_state
from temporalio.contrib.streams._workflow import (
    _FINAL_ERRORS,
    TaskCutoff,
    _next_reader,
)
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError

__all__ = [
    "FOLLOW_ACTIVITY",
    "FollowBatch",
    "WorkflowStreamFollower",
    "follow_activity",
    "workflow_follower",
]

T = TypeVar("T")

FOLLOW_ACTIVITY = "__temporal_stream_follow"
"""The name the built-in follow Activity is registered under."""


_RESPONSE_TYPE = FollowResponse.DESCRIPTOR.full_name.encode()
_FOLLOW_BY_RUN_ID_KEY = "temporal.io/stream-follow-by-run-id"
_FOLLOW_RUN_ID_KEY = "temporal.io/stream-follow-run-id"
_MAX_RECORDS = 100
# How long a follower waits when the stream runtime named no source to wake it, which
# happens only while every source waits for its owner to start.
_NO_WAKE_RETRY = timedelta(seconds=2)


@dataclass(frozen=True)
class FollowBatch(Generic[T]):
    """The items one follow step delivered, in order, and the position after them."""

    items: list[FollowedItem[T]]
    position: FollowPosition


@dataclass
class _FollowInput:
    """What the follow Activity gets. Plain data, so the default converter carries it."""

    workflow_id: str
    chain_run_id: str
    topic: str
    after: str
    state: str
    max_depth: int
    max_open: int
    owner_kind: str = "workflow"
    activity_id: str = ""
    max_records: int = _MAX_RECORDS
    wait_ms: int = 0
    """How long the step waits in the store when nothing is ready."""
    subscription_prefix: str = ""
    """Set to attach the running Workflow to each idle source."""
    generation: int = 0
    read_run_id: str | None = None
    unsubscribe: list[str] = field(default_factory=list)
    """Subscription ids to detach, and nothing to read."""
    unsubscribe_addresses: list[str] = field(default_factory=list)
    """The serialized address of each source to detach, in base64, or empty for the root."""
    positions_only: bool = False
    reader_id: str = ""


@dataclass
class _FollowerState:
    cursor: str
    state: bytes
    number: int
    generation: int = 0
    read_run_id: str | None = None
    attached_run_id: str = ""
    # Each source's subscription and the counter of its last empty step.
    sleeping: dict[str, int] | None = None
    # Each attached source's subscription id and number, for the detach.
    attached: dict[str, int] = field(default_factory=dict)
    addresses: dict[int, StreamAddress] = field(default_factory=dict)
    sources: dict[int, FollowSourceInfo] = field(default_factory=dict)
    # Set once a step came back empty. The stream runtime attaches each idle source of a
    # step that names the subscriber.
    subscribe: bool = False
    done: bool = False
    close_failure: Failure | None = None
    failure_raised: bool = False


class WorkflowStreamFollower(Generic[T]):
    """Follows a stream and its links in Workflow code, one batch per Local Activity.

    Use it as an async iterator of :class:`FollowBatch`, inside
    ``async with``, so that a follower stopped early detaches from every
    source's notifier.
    """

    def __init__(
        self,
        ref: StreamRef,
        topic: str,
        result_type: type | None,
        after: Cursor | FollowPosition,
        idle_wait: timedelta,
        max_depth: int,
        max_open: int,
        start_to_close_timeout: timedelta,
        cutoff: TaskCutoff | None,
        replay_from_store: bool,
    ) -> None:
        """Prefer :func:`temporalio.contrib.streams.workflow_follower`."""
        self._ref = ref
        self._topic = topic
        self._result_type = result_type
        self._idle_wait = idle_wait
        self._max_depth = max_depth
        self._max_open = max_open
        self._start_to_close_timeout = start_to_close_timeout
        self._cutoff = cutoff
        self._replay_from_store = replay_from_store
        if isinstance(after, FollowPosition):
            cursor, state = after.cursor, base64.b64decode(after.state)
        else:
            cursor, state = after.token, b""
        self._state = _FollowerState(cursor, state, _next_reader())
        self._state.sources[0] = FollowSourceInfo(ref.with_topic(topic), 0)

    @property
    def subscription_prefix(self) -> str:
        """What every subscription id of this follower starts with."""
        return f"follow-{self._state.number}"

    @property
    def position(self) -> FollowPosition:
        """Where the follower stands: past the last batch it delivered."""
        return FollowPosition(
            self._state.cursor, base64.b64encode(self._state.state).decode()
        )

    def __aiter__(self) -> WorkflowStreamFollower[T]:
        """The follower is its own iterator."""
        return self

    async def __aenter__(self) -> WorkflowStreamFollower[T]:
        """Start nothing. The first iteration follows."""
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Detach from every source's notifier, unless the follow ended."""
        await self.close()

    async def close(self) -> None:
        """Detach from every source's notifier in a Local Activity. Idempotent."""
        state = self._state
        if state.done or not state.attached:
            return
        input = self._input(subscribe=False)
        for subscription_id, number in state.attached.items():
            address = state.addresses.get(number)
            input.unsubscribe.append(subscription_id)
            input.unsubscribe_addresses.append(
                base64.b64encode(address.SerializeToString()).decode()
                if address is not None
                else ""
            )
        await self._run(input)
        state.attached.clear()
        state.sleeping = None

    async def __anext__(self) -> FollowBatch[T]:
        """Follow until items arrive, sleeping on the sources' wakes while all are idle.

        Raises:
            StopAsyncIteration: The root is done and every segment ended.
            BaseException: The failure the root closed with, once, after the
                last batch.
            temporalio.exceptions.FailureError: A step failed for good, as a
                Workflow reader's read does.
        """
        state = self._state
        while True:
            if state.done:
                raise self._end()
            if (
                state.attached_run_id
                and state.attached_run_id != workflow.info().run_id
            ):
                # A reset run replays the attaches of the run it was reset
                # from, and the notifier wakes only that run.
                state.generation += 1
                state.attached_run_id = ""
                state.sleeping = None
            if state.sleeping is not None:
                if state.sleeping:
                    await workflow.wait_condition(self._woke)
                else:
                    await workflow.sleep(_NO_WAKE_RETRY)
                self._take_wakes()
            self._cut_task()
            response, read_by = await self._step()
            update_sources(state.sources, response)
            payload_converter = workflow.payload_converter()
            runtime: Any = workflow._Runtime.current()
            items, state.cursor = follow_items(
                payload_converter,
                runtime._workflow_context_failure_converter,
                state.sources,
                response,
                self._result_type,
                state.cursor,
            )
            state.state = response.state
            state.attached_run_id = read_by
            for source in response.sources:
                state.addresses[source.number] = source.stream
            for wake in response.wakes:
                state.attached[wake.subscription_id] = wake.source
            if response.done:
                state.done = True
                if response.HasField("close_failure"):
                    state.close_failure = response.close_failure
            elif not items:
                if not state.subscribe:
                    # Attach only when the follow goes idle, then step once
                    # more, since an append before the attach notified no one.
                    state.subscribe = True
                    continue
                state.sleeping = {
                    w.subscription_id: w.wake_after for w in response.wakes
                }
            if items:
                return FollowBatch(items, self.position)

    def _woke(self) -> bool:
        sleeping = self._state.sleeping
        assert sleeping is not None
        for subscription_id, counter in sleeping.items():
            wake = wake_state(subscription_id)
            if wake is not None and (
                wake.counter > counter
                or wake.recheck
                or wake.closed
                or wake.resubscribe
            ):
                return True
        return False

    def _take_wakes(self) -> None:
        state = self._state
        for subscription_id in state.sleeping or {}:
            wake = wake_state(subscription_id)
            if wake is None:
                continue
            wake.recheck = False
            if wake.resubscribe and not wake.closed:
                # The server answers a repeat of the dropped attach as done,
                # so a new generation attaches every source again.
                wake.resubscribe = False
                state.generation += 1
        state.sleeping = None

    def _cut_task(self) -> None:
        if self._cutoff is None:
            return
        runtime: Any = workflow._Runtime.current()
        commands, seconds = runtime._task_progress()
        if (
            commands >= self._cutoff.commands
            or seconds >= self._cutoff.time.total_seconds()
        ):
            runtime._request_task_heartbeat()

    def _end(self) -> BaseException:
        state = self._state
        if state.close_failure is None or state.failure_raised:
            return StopAsyncIteration()
        state.failure_raised = True
        runtime: Any = workflow._Runtime.current()
        converter = runtime._workflow_context_failure_converter
        return converter.from_failure(state.close_failure, workflow.payload_converter())

    def _input(self, *, subscribe: bool) -> _FollowInput:
        state = self._state
        return _FollowInput(
            workflow_id=self._ref.workflow_id,
            owner_kind=self._ref.kind,
            activity_id=self._ref.activity_id or "",
            chain_run_id=self._ref.chain_run_id or "",
            topic=self._topic,
            after=state.cursor,
            state=base64.b64encode(state.state).decode(),
            max_depth=self._max_depth,
            max_open=self._max_open,
            # A step that names the subscriber attaches each idle source, so it
            # need not wait in the store.
            wait_ms=0 if subscribe else self._idle_wait // timedelta(milliseconds=1),
            subscription_prefix=self.subscription_prefix if subscribe else "",
            generation=state.generation,
            read_run_id=state.read_run_id,
            positions_only=self._replay_from_store,
            reader_id=self.subscription_prefix,
        )

    async def _step(self) -> tuple[FollowResponse, str]:
        response, read_by = await self._run(
            self._input(subscribe=self._state.subscribe)
        )
        return response, read_by

    async def _run(self, input: _FollowInput) -> tuple[FollowResponse, str]:
        raw = await workflow.execute_local_activity(
            FOLLOW_ACTIVITY,
            input,
            result_type=RawValue,
            start_to_close_timeout=self._start_to_close_timeout,
            summary=_summary(input),
        )
        payload: Payload = raw.payload
        if payload.metadata.get("messageType") == FOLLOW_POSITIONS_TYPE:
            raise StreamUnsupportedError(
                "a stream follow with replay_from_store=True (the default) recorded "
                "only its ranges, and only the stream store plugin on the Worker or "
                "Replayer turns them back into records; add the plugin, or pass "
                "replay_from_store=False to record each batch in History"
            )
        run_id = payload.metadata.get(_FOLLOW_RUN_ID_KEY)
        if run_id is not None:
            self._state.read_run_id = run_id.decode()
        read_by = payload.metadata.get(_FOLLOW_BY_RUN_ID_KEY)
        return (
            FollowResponse.FromString(payload.data),
            read_by.decode() if read_by is not None else workflow.info().run_id,
        )


def _summary(input: _FollowInput) -> str:
    if input.unsubscribe:
        return f"stream unfollow {input.topic}"
    after = {BEGINNING.token: "the start"}.get(input.after, input.after)
    return f"stream follow {input.topic} after {after}"


@overload
def workflow_follower(
    ref: StreamRef | str,
    *,
    topic: StreamTopic[T],
    after: Cursor | FollowPosition = ...,
    idle_wait: timedelta = ...,
    max_depth: int = ...,
    max_open: int = ...,
    start_to_close_timeout: timedelta | None = ...,
    cutoff: TaskCutoff | None = ...,
    replay_from_store: bool = ...,
) -> WorkflowStreamFollower[T]: ...


@overload
def workflow_follower(
    ref: StreamRef | str,
    *,
    topic: str | None = None,
    result_type: type[T],
    after: Cursor | FollowPosition = ...,
    idle_wait: timedelta = ...,
    max_depth: int = ...,
    max_open: int = ...,
    start_to_close_timeout: timedelta | None = ...,
    cutoff: TaskCutoff | None = ...,
    replay_from_store: bool = ...,
) -> WorkflowStreamFollower[T]: ...


@overload
def workflow_follower(
    ref: StreamRef | str,
    *,
    topic: str | None = None,
    result_type: None = None,
    after: Cursor | FollowPosition = ...,
    idle_wait: timedelta = ...,
    max_depth: int = ...,
    max_open: int = ...,
    start_to_close_timeout: timedelta | None = ...,
    cutoff: TaskCutoff | None = ...,
    replay_from_store: bool = ...,
) -> WorkflowStreamFollower[Any]: ...


def workflow_follower(
    ref: StreamRef | str,
    *,
    topic: str | StreamTopic[Any] | None = None,
    result_type: type | None = None,
    after: Cursor | FollowPosition = BEGINNING,
    idle_wait: timedelta = timedelta(seconds=2),
    max_depth: int = 4,
    max_open: int = 16,
    start_to_close_timeout: timedelta | None = None,
    cutoff: TaskCutoff | None = TaskCutoff(),
    replay_from_store: bool = True,
) -> WorkflowStreamFollower[Any]:
    """A follower of a stream and the streams its links open, for Workflow code.

    It gives what :meth:`temporalio.contrib.streams.StreamHandle.follow`
    gives, in batches. Each step is one Local Activity, which adds one
    marker event. While every open source is idle, the follower waits for a
    wake from any of their notifiers, with no Local Activity and no timer.
    Each open source is one subscription of the running Workflow, so a
    Workflow has room for about as many open sources as it has free wake
    sources (32 per execution by default). A link past that is ``SKIPPED``
    with reason ``limit``.

    A step waits up to ``idle_wait`` in the store. When one still comes
    back empty, the follower steps once more with the running Workflow as
    the subscriber, so the stream runtime attaches each idle source, and then sleeps until
    a wake. To go on in the next run, pass
    :attr:`WorkflowStreamFollower.position` as ``after``.

    Args:
        ref: The root stream, as a ref of any owner, or a Workflow id in
            this namespace.
        topic: The root topic. Without one, the ref's topic.
        result_type: The type the root's records decode to, for a plain
            string topic.
        after: Where to start: a cursor of the root, ``BEGINNING``, or the
            position of an earlier follower.
        idle_wait: How long a step waits in the store before the follower
            sleeps.
        max_depth: How many links deep the follower opens.
        max_open: How many segments it holds open at once.
        start_to_close_timeout: How long one step may run. ``idle_wait``
            plus 10 seconds by default.
        cutoff: When the follower ends the Workflow Task it steps in, as
            for a Workflow reader.
        replay_from_store: Record each step's ranges and digest in History
            in place of the records. On by default. Replay reads them again
            through the stream runtime's ``FollowRange``, so a ``Replayer`` needs the store
            plugin and a connected client. ``False`` records each batch.

    Raises:
        ValueError: A bound is below one, ``idle_wait`` is not positive, or
            ``result_type`` was passed with a topic definition.
        temporalio.workflow.ReadOnlyContextError: Called from a query or an
            update validator.

    .. warning::
        This API is experimental.
    """
    if workflow.unsafe.is_read_only():
        raise workflow.ReadOnlyContextError(
            "cannot follow a stream in a query or an update validator"
        )
    if isinstance(ref, str):
        ref = StreamRef.for_workflow(ref)
    ref._require_supported()
    if max_depth < 1 or max_open < 1:
        raise ValueError("max_depth and max_open must be at least 1")
    if idle_wait <= timedelta(0):
        raise ValueError("idle_wait must be positive")
    if cutoff is not None and (cutoff.commands < 1 or cutoff.time <= timedelta(0)):
        raise ValueError("a task cutoff needs a positive command count and time")
    name, result_type = resolve_topic(
        topic if topic is not None else ref.topic, result_type
    )
    return WorkflowStreamFollower(
        ref,
        name,
        result_type,
        after,
        idle_wait,
        max_depth,
        max_open,
        start_to_close_timeout or idle_wait + timedelta(seconds=10),
        cutoff,
        replay_from_store,
    )


@temporalio.activity.defn(name=FOLLOW_ACTIVITY)
async def follow_activity(input: _FollowInput) -> RawValue:
    """Run one follow step for a Workflow follower.

    A Workflow follower runs it as a Local Activity. The store plugin
    registers it on every Worker built from a client that carries a store.
    """
    try:
        require_local_activity("the stream follow Activity")
        return await _follow_step(input)
    except _FINAL_ERRORS as error:
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error


async def _follow_step(input: _FollowInput) -> RawValue:
    info = temporalio.activity.info()
    client = temporalio.activity.client()
    store = store_for_client(client)
    service = await store._service_for(client)
    ref = StreamRef(
        input.owner_kind,
        input.workflow_id,
        topic=input.topic,
        chain_run_id=input.chain_run_id or None,
        activity_id=input.activity_id or None,
    )
    handle = StreamHandle(store, client, ref)
    read_run_id = input.read_run_id
    if read_run_id is None:
        read_run_id = await handle._read_run_id()
    root = handle._address(input.topic, run_id=read_run_id)
    if input.unsubscribe:
        assert info.workflow_run_id
        for subscription_id, encoded in zip(
            input.unsubscribe, input.unsubscribe_addresses
        ):
            address = (
                StreamAddress.FromString(base64.b64decode(encoded)) if encoded else root
            )
            await call(
                service.unsubscribe(
                    UnsubscribeRequest(
                        stream=address,
                        run_id=info.workflow_run_id,
                        subscription_id=subscription_id,
                        generation=input.generation,
                    )
                )
            )
        return _result(FollowResponse(), read_run_id)
    request = FollowRequest(
        stream=root,
        after=input.after,
        state=base64.b64decode(input.state),
        max_records=input.max_records,
        max_depth=input.max_depth,
        max_open=input.max_open,
    )
    request.wait.FromMilliseconds(input.wait_ms)
    floor_holder = f"{info.workflow_run_id}/{input.reader_id}"
    if input.positions_only:
        request.floor_holder = floor_holder
    if input.subscription_prefix:
        assert info.workflow_id and info.workflow_run_id
        request.subscriber.CopyFrom(
            FollowSubscriber(
                workflow_id=info.workflow_id,
                run_id=info.workflow_run_id,
                subscription_prefix=input.subscription_prefix,
                generation=input.generation,
            )
        )
    response: FollowResponse = await call(service.follow(request))
    await decode_follow(client.data_converter, client.namespace, ref, response)
    if not input.positions_only:
        return _result(response, read_run_id)
    range = FollowRangeRequest(
        stream=request.stream,
        state_in=request.state,
        ranges=response.ranges,
        follow_digest=response.follow_digest,
        floor_holder=floor_holder,
        max_depth=input.max_depth,
        max_open=input.max_open,
    )
    # The Worker turns the result back into this batch before Workflow code
    # sees it, from here while the run stays in this process.
    store._follows.put(follow_key(range), response.SerializeToString())
    return RawValue(follow_positions_payload(range, response, read_run_id))


def _result(response: FollowResponse, read_run_id: str) -> RawValue:
    return RawValue(follow_payload(response, read_run_id, _run_id()))


def _run_id() -> str:
    return temporalio.activity.info().workflow_run_id or ""


def follow_payload(response: FollowResponse, read_run_id: str, read_by: str) -> Payload:
    """The result a recorded step records, and what a positions-only one turns back into."""
    return Payload(
        metadata={
            "encoding": b"binary/protobuf",
            "messageType": _RESPONSE_TYPE,
            _FOLLOW_RUN_ID_KEY: read_run_id.encode(),
            _FOLLOW_BY_RUN_ID_KEY: read_by.encode(),
        },
        data=response.SerializeToString(),
    )


def follow_positions_payload(
    range: FollowRangeRequest, answer: FollowResponse, read_run_id: str
) -> Payload:
    """The result a positions-only step records: its ranges, and its answer without records.

    The answer keeps its live state, since a replay's state differs in its
    clock fields.
    """
    without_records = FollowResponse()
    without_records.CopyFrom(answer)
    del without_records.records[:]
    return Payload(
        metadata={
            "encoding": b"binary/protobuf",
            "messageType": FOLLOW_POSITIONS_TYPE,
            _FOLLOW_RUN_ID_KEY: read_run_id.encode(),
            _FOLLOW_BY_RUN_ID_KEY: _run_id().encode(),
        },
        data=Payloads(
            payloads=[
                Payload(data=range.SerializeToString()),
                Payload(data=without_records.SerializeToString()),
            ]
        ).SerializeToString(),
    )


def follow_key(range: FollowRangeRequest) -> bytes:
    """The key of one step in the plugin's cache: its holder, its input and its digest."""
    return b"\0".join(
        [range.floor_holder.encode(), range.state_in, range.follow_digest]
    )


async def replay_follow(
    plugin: Any, body_converter: DataConverter, payload: Payload
) -> Payload:
    """Turn a positions-only step's result back into its batch.

    Takes the batch this process cached, or reads the ranges again with
    ``FollowRange``, which checks them against the step's digest.

    Raises:
        temporalio.workflow.NondeterminismError: The store holds other
            records in the ranges than the step found.
        StreamUnsupportedError: No client connected the stream store in this
            process.
    """
    parts = Payloads.FromString(payload.data).payloads
    range = FollowRangeRequest.FromString(parts[0].data)
    answer = FollowResponse.FromString(parts[1].data)
    read_run_id = payload.metadata.get(_FOLLOW_RUN_ID_KEY, b"").decode()
    read_by = payload.metadata.get(_FOLLOW_BY_RUN_ID_KEY, b"").decode()
    cached = plugin._follows.get(follow_key(range))
    if cached is not None:
        return follow_payload(FollowResponse.FromString(cached), read_run_id, read_by)
    service = plugin._service
    if service is None:
        raise StreamUnsupportedError(
            "a stream follow with replay_from_store=True (the default) replays "
            "from the store, and no client connected the stream store in this "
            "process; connect a client with the store, or pass "
            "replay_from_store=False to record each batch in History"
        )
    try:
        replayed: FollowResponse = await call(service.follow_range(range))
    except StreamReplayMismatchError as error:
        raise workflow.NondeterminismError(
            f"a positions-only stream follow replayed other records: {error}"
        ) from error
    stream = range.stream
    await decode_follow(
        body_converter, stream.namespace, from_address(stream), replayed
    )
    answer.records.extend(replayed.records)
    return follow_payload(answer, read_run_id, read_by)
