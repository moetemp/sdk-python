"""Follow a stream from a Worker, with no Workflow, through a standalone operation.

A client starts the stream-returning operation as a standalone Nexus operation
with a NexusHandler callback to a Worker's task queue. The server then calls
the Worker each time the stream moves (``OnProgress``) and once when the
operation completes (``OnComplete``). :class:`StreamFollowerService` takes
both calls: it reads the new records through the stream service and hands
them to the application's :class:`StreamFollower` in batches.

The read position travels with the server's callback as its receiver state,
so the Worker keeps nothing between calls and any Worker on the task queue
can take the next one.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import typing
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, Protocol, TypeVar

import nexusrpc
from nexusrpc.handler import StartOperationContext, service_handler, sync_operation

import temporalio.converter
import temporalio.nexus
from temporalio.api.common.v1 import Payload
from temporalio.bridge.proto.streams import NexusLimits
from temporalio.client import Client, NexusHandlerCallback, NexusOperationHandle
from temporalio.contrib.streams._errors import StreamRecordError
from temporalio.contrib.streams._plugin import store_for_client
from temporalio.contrib.streams._record import (
    Cursor,
    RecordKind,
    StreamRecord,
    Supersession,
)
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._wire import RUN_ID_KEY, WireRecord
from temporalio.contrib.streams.nexus._generated import (
    ReadInput,
    ReadOutput,
    RecordWire,
)
from temporalio.contrib.streams.nexus._handler import serve_in_core
from temporalio.contrib.streams.nexus._operation import stream_ref_from_token
from temporalio.contrib.streams.nexus._reader import ReadStaleRecord, ReadSupersession
from temporalio.converter import WorkflowSerializationContext
from temporalio.nexus.system.notification_service import (
    NotificationService,
    OnCompleteRequest,
    OnCompleteRequestResultFailure,
    OnCompleteResponse,
    OnProgressRequest,
    OnProgressResponse,
)

__all__ = [
    "FollowBatch",
    "FollowOutcome",
    "StoreTransport",
    "StreamFollower",
    "StreamFollowerService",
    "StreamReadTransport",
    "follow_stream",
]

T = TypeVar("T")

_PROGRESS_OPERATION = "OnProgress"
_COMPLETE_OPERATION = "OnComplete"
# Used when the server sends no request deadline. It is half the server's
# default request timeout for a callback.
_DEFAULT_BUDGET = timedelta(seconds=5)
# The server's default cap on a callback's receiver state.
_DEFAULT_STATE_CAP = 8 * 1024
# A read after the operation completed waits this long for records, so a
# drain to the stream's end doesn't spin.
_DRAIN_WAIT_MS = 1000
# A progress read waits this long for a record before it arms the stream. The
# Worker holds the server's callback while it waits, so it is shorter than a
# Workflow reader's.
_IDLE_WAIT_MS = 1000
# The default payload converter writes the contract's JSON transfer form.
_CONTRACT = temporalio.converter.PayloadConverter.default


class StreamReadTransport(Protocol):
    """Reads through the stream service, as :class:`StreamFollowerService` does.

    .. warning::
        This API is experimental and may change in future versions.

    :class:`StoreTransport` reads on the store of a client.
    :class:`temporalio.contrib.streams.nexus.TemporalStreamsHttpClient` reads
    through a Nexus endpoint's HTTP ingress, for a stream in another namespace
    or another store.
    """

    async def read(self, request: ReadInput) -> ReadOutput:
        """Answer with the records after ``request``'s cursor."""
        ...


class StoreTransport:
    """Reads through the stream service in Core, on the store of a client.

    .. warning::
        This API is experimental and may change in future versions.

    This is the path an outside reader takes. Core runs every rule of the
    contract, and the client's payload codec decodes each body. Use it for a
    stream in the client's namespace and store.
    """

    def __init__(
        self,
        client: Client,
        *,
        max_answer_bytes: int = 1 << 20,
        max_record_bytes: int = 3 << 19,
    ) -> None:
        """Read on the store registered on ``client``.

        Raises:
            ValueError: No stream store is registered on ``client``.
        """
        store_for_client(client)
        self._client = client
        self._limits = NexusLimits(
            max_answer_bytes=max_answer_bytes, max_record_bytes=max_record_bytes
        )

    async def read(self, request: ReadInput) -> ReadOutput:
        """Answer with the records after ``request``'s cursor."""
        transfer = _transfer(request)
        answer = await serve_in_core(self._client, "read", transfer, self._limits, None)
        return _from_transfer(answer, ReadOutput)


@dataclass(frozen=True)
class FollowBatch(Generic[T]):
    """The records one read handed a follower.

    .. warning::
        This API is experimental and may change in future versions.

    A batch can repeat. The server sends progress again after a lost answer
    or a timeout, and a completion can overtake a read in flight, so the same
    records can come twice, and a batch can come after
    :meth:`StreamFollower.on_close`. Each record carries its cursor, so a
    follower drops the ones it already handled.
    """

    stream: StreamRef
    """The stream the records are on."""

    records: Sequence[StreamRecord[T]]
    """The data records, in stream order. A producer's ``FINISH`` record is
    not handed over."""

    supersessions: Sequence[ReadSupersession]
    """Where a later attempt of a producer took over, with ``index`` counted
    in :attr:`records`."""

    stale_records: Sequence[ReadStaleRecord]
    """Records from an attempt behind one already seen, left out of
    :attr:`records`."""

    errors: Sequence[StreamRecordError]
    """Records that could not be handed over, by cursor. The read goes on
    past them."""

    cursor: Cursor
    """Past every record of the batch."""


@dataclass(frozen=True)
class FollowOutcome:
    """How the followed operation ended.

    .. warning::
        This API is experimental and may change in future versions.
    """

    stream: StreamRef | None
    """The stream, or ``None`` when the follower never learned it."""

    result: Any = None
    """The close result, when the operation succeeded."""

    error: BaseException | None = None
    """The operation's failure, when it failed or was canceled."""

    @property
    def succeeded(self) -> bool:
        """Whether the operation completed with a result."""
        return self.error is None


class StreamFollower(Protocol[T]):
    """What an application does with the records it follows.

    .. warning::
        This API is experimental and may change in future versions.

    The type argument of ``on_records``'s :class:`FollowBatch` annotation is
    what each record body decodes into. Without one, bodies decode with no
    type hint.
    """

    async def on_records(self, batch: FollowBatch[T]) -> None:
        """Handle the next records. See :class:`FollowBatch` on repeats."""
        ...

    async def on_close(self, outcome: FollowOutcome) -> None:
        """Handle the end of the stream, after every record was handed over."""
        ...


@dataclass
class _Followed:
    follower: StreamFollower[Any]
    item_type: type | None


@service_handler(service=NotificationService)
class StreamFollowerService:
    """Takes a standalone stream operation's callbacks and hands its records over.

    .. warning::
        This API is experimental and may change in future versions.

    Register it on a Worker, and start the operation with
    :func:`follow_stream`, naming that Worker's task queue and a follower
    key. One service per Worker routes by key, since a Worker registers a
    service name once::

        worker = Worker(
            client,
            task_queue="app",
            nexus_service_handlers=[StreamFollowerService({"orders": Orders()}, client=client)],
        )

    On each progress call it reads from the receiver state the server sent
    back, page by page, and calls :meth:`StreamFollower.on_records` once per
    page. Each read waits up to a second for a record and asks the stream
    service to arm the stream. The follower stops at the first answer that
    armed it, so the stream's next append notifies the operation again, and
    answers with the new receiver state. A busy stream never arms, so the
    follower also stops once half the request timeout passed. Then it answers
    ``behind``, and the server sends the same progress again at once. On the
    completion it reads the rest of the stream, then calls
    :meth:`StreamFollower.on_close`. If the stream doesn't end within the
    request timeout, the completion fails as retryable, so the server sends
    it again and the follower reads again from the last state the server
    took.

    The receiver state is the generated :class:`ReadInput` with the stream,
    in its JSON transfer form. It holds the cursor and Core's read state, so
    the superseded and stale marks hold across calls and Workers. If it is
    larger than ``max_receiver_state_bytes``, the follower drops Core's read
    state and keeps the cursor. Marks of the dropped producers can then read
    without ``SUPERSEDED``.

    The stream service and the follower must use the same payload codec,
    since the stream service decodes each body with its own.
    """

    def __init__(
        self,
        followers: Mapping[str, StreamFollower[Any]],
        *,
        client: Client | None = None,
        transport: StreamReadTransport | None = None,
        max_records: int = 100,
        max_receiver_state_bytes: int = _DEFAULT_STATE_CAP,
    ) -> None:
        """Hand the records of each followed stream to its follower.

        Args:
            followers: The followers by key. :func:`follow_stream` names one.
            client: The client whose data converter decodes the records, and
                whose store a :class:`StoreTransport` reads when no
                ``transport`` is given. Usually the Worker's client.
            transport: How to read. Defaults to a :class:`StoreTransport` on
                ``client``.
            max_records: The most records one read asks for.
            max_receiver_state_bytes: The server's cap on a callback's
                receiver state, ``callback.nexusHandler.receiverStateMaxSize``.

        Raises:
            ValueError: No ``transport`` is given, and ``client`` is missing
                or carries no stream store. Or a limit is not positive.
        """
        if max_records <= 0:
            raise ValueError("max_records must be positive")
        if max_receiver_state_bytes <= 0:
            raise ValueError("max_receiver_state_bytes must be positive")
        if transport is None:
            if client is None:
                raise ValueError(
                    "pass transport=, or client= a client that carries a stream store"
                )
            transport = StoreTransport(client)
        self._followers = {
            key: _Followed(follower, _item_type(follower))
            for key, follower in followers.items()
        }
        self._client = client
        self._transport = transport
        self._max_records = max_records
        self._state_cap = max_receiver_state_bytes

    @sync_operation
    async def on_progress(
        self, ctx: StartOperationContext, input: OnProgressRequest
    ) -> OnProgressResponse:
        """Read the records until the stream arms, and hand them over."""
        followed = self._followed(input.source_context)
        if not input.operation_token:
            raise _refused(
                "the progress names no stream; the source is not a started "
                "standalone stream operation"
            )
        try:
            stream = stream_ref_from_token(input.operation_token)
        except ValueError as error:
            raise _refused(f"the progress names no stream: {error}") from None
        position = _state_in(input.receiver_state or b"", stream)
        end = _budget_end(ctx)
        behind = False
        while True:
            left_ms = int((end - asyncio.get_running_loop().time()) * 1000)
            wait_ms = min(_IDLE_WAIT_MS, max(left_ms, 0))
            answer = await self._read(position, wait_ms=wait_ms, arm=True)
            await self._hand_over(followed, stream, answer)
            position = _moved(position, answer)
            if answer.done or answer.armed:
                break
            if asyncio.get_running_loop().time() >= end:
                behind = True
                break
        return OnProgressResponse(
            receiver_state=_state_out(position, self._state_cap), behind=behind
        )

    @sync_operation
    async def on_complete(
        self, ctx: StartOperationContext, input: OnCompleteRequest
    ) -> OnCompleteResponse:
        """Read the rest of the stream, then hand over how the operation ended."""
        followed = self._followed(input.source_context)
        position = (
            _state_in(input.receiver_state, None) if input.receiver_state else None
        )
        if position is None:
            stream = await self._stream_of(input.source_context)
            if stream is not None:
                position = ReadInput(stream=stream)
        if position is not None:
            await self._drain(ctx, followed, position)
        outcome = FollowOutcome(stream=position.stream if position else None)
        if isinstance(input.result, OnCompleteRequestResultFailure):
            outcome = dataclasses.replace(outcome, error=input.result.value)
        else:
            outcome = dataclasses.replace(outcome, result=input.result.value)
        await followed.follower.on_close(outcome)
        return OnCompleteResponse()

    async def _drain(
        self, ctx: StartOperationContext, followed: _Followed, position: ReadInput
    ) -> None:
        # The store closes the stream before the operation completes, so every
        # read ends at the stream's end.
        end = _request_end(ctx)
        wait_ms = 0
        while True:
            answer = await self._read(position, wait_ms=wait_ms)
            await self._hand_over(followed, position.stream, answer)
            position = _moved(position, answer)
            if answer.done:
                return
            wait_ms = 0 if answer.records else _DRAIN_WAIT_MS
            if asyncio.get_running_loop().time() >= end:
                raise nexusrpc.HandlerError(
                    "the stream did not end within the request timeout",
                    type=nexusrpc.HandlerErrorType.INTERNAL,
                    retryable_override=True,
                )

    async def _read(
        self, position: ReadInput, *, wait_ms: int, arm: bool = False
    ) -> ReadOutput:
        return await self._transport.read(
            dataclasses.replace(
                position,
                max_records=self._max_records,
                wait_ms=wait_ms,
                arm=True if arm else None,
            )
        )

    async def _hand_over(
        self,
        followed: _Followed,
        stream: StreamRef,
        answer: ReadOutput,
    ) -> None:
        """Hand one answer's records over."""
        converter = self._converter().with_context(
            WorkflowSerializationContext(
                namespace=self._namespace(), workflow_id=stream.workflow_id
            )
        )
        records: list[StreamRecord[Any]] = []
        supersessions: list[ReadSupersession] = []
        stale: list[ReadStaleRecord] = []
        errors: list[StreamRecordError] = []
        for stored in answer.records:
            cursor = Cursor(stored.token)
            if stored.superseded is not None:
                supersessions.append(
                    ReadSupersession(
                        index=len(records),
                        cursor=cursor,
                        supersession=Supersession(
                            producer_id=stored.superseded.producer_id,
                            previous_attempt=stored.superseded.previous_attempt,
                            attempt=stored.superseded.attempt,
                        ),
                    )
                )
                continue
            try:
                record = _record(converter, stored, followed.item_type)
            except Exception as error:
                if stored.stale:
                    stale.append(ReadStaleRecord(len(records), cursor, "", 0, None))
                else:
                    errors.append(
                        StreamRecordError(
                            f"stream record at {cursor} could not be read: {error}",
                            cursor,
                        )
                    )
                continue
            if record is None:
                continue
            if stored.stale:
                stale.append(
                    ReadStaleRecord(
                        index=len(records),
                        cursor=cursor,
                        producer_id=record.producer_id,
                        attempt=record.attempt,
                        value=record.value,
                    )
                )
                continue
            records.append(record)
        if records or supersessions or stale or errors:
            await followed.follower.on_records(
                FollowBatch(
                    stream=stream,
                    records=records,
                    supersessions=supersessions,
                    stale_records=stale,
                    errors=errors,
                    cursor=Cursor(_moved_token(answer) or ""),
                )
            )

    def _followed(self, source_context: Any) -> _Followed:
        key = _context(source_context).get("follower")
        followed = self._followers.get(key) if isinstance(key, str) else None
        if followed is None:
            raise nexusrpc.HandlerError(
                f"no follower is registered under {key!r}",
                type=nexusrpc.HandlerErrorType.NOT_FOUND,
                retryable_override=False,
            )
        return followed

    async def _stream_of(self, source_context: Any) -> StreamRef | None:
        # A completion carries no token. With no progress taken, the operation
        # itself still names the stream.
        operation_id = _context(source_context).get("operation_id")
        if not isinstance(operation_id, str) or not operation_id:
            return None
        client = self._client or temporalio.nexus.client()
        description = await client.get_nexus_operation_handle(operation_id).describe()
        if not description.operation_token:
            return None
        return stream_ref_from_token(description.operation_token)

    def _converter(self) -> temporalio.converter.DataConverter:
        client = self._client or temporalio.nexus.client()
        return client.data_converter

    def _namespace(self) -> str:
        client = self._client or temporalio.nexus.client()
        return client.namespace


async def follow_stream(
    client: Client,
    *,
    endpoint: str,
    service: Any,
    operation: Any,
    arg: Any,
    follower: str,
    task_queue: str,
    id: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
) -> NexusOperationHandle[Any]:
    """Start a stream-returning operation for a Worker to follow.

    .. warning::
        This API is experimental and may change in future versions.

    Starts ``operation`` as a standalone Nexus operation with one NexusHandler
    callback to ``task_queue``, whose :class:`StreamFollowerService` hands the
    records to the follower registered under ``follower``. The server must
    take NexusHandler callbacks on a standalone operation
    (``nexusoperation.enableStandalone`` and
    ``nexusoperation.enabledCallbackKinds``).

    Args:
        client: The client that starts the operation.
        endpoint: The Nexus endpoint of the operation.
        service: The service, as a type or a name.
        operation: The stream-returning operation, as for
            :meth:`temporalio.client.NexusClient.start_operation`.
        arg: The operation's input.
        follower: The key of the follower on the Worker.
        task_queue: The task queue of the Worker that follows.
        id: The operation id. A new one when ``None``.
        schedule_to_close_timeout: How long the operation may run.

    Returns:
        The standalone operation's handle.
    """
    operation_id = id or str(uuid.uuid4())
    nexus_client: Any = client.create_nexus_client(service=service, endpoint=endpoint)
    return await nexus_client.start_operation(
        operation,
        arg,
        id=operation_id,
        schedule_to_close_timeout=schedule_to_close_timeout,
        completion_callbacks=[
            NexusHandlerCallback(
                task_queue=task_queue,
                operation=_COMPLETE_OPERATION,
                progress_operation=_PROGRESS_OPERATION,
                source_context={
                    "follower": follower,
                    "endpoint": endpoint,
                    "operation_id": operation_id,
                },
            )
        ],
    )


def _record(
    converter: temporalio.converter.DataConverter,
    stored: RecordWire,
    item_type: type | None,
) -> StreamRecord[Any] | None:
    """The data record ``stored`` holds, or ``None`` for one that carries no item."""
    if stored.error:
        raise ValueError(stored.error)
    wire = WireRecord.FromString(stored.record)
    kind = RecordKind(wire.kind) if wire.kind else RecordKind.DATA
    if kind is RecordKind.SUPERSEDED:
        raise ValueError("a SUPERSEDED record is marked by readers, never stored")
    if kind is not RecordKind.DATA:
        return None
    value: Any = None
    if wire.HasField("body"):
        hints = [item_type] if item_type is not None else None
        value = converter.payload_converter.from_payloads([wire.body], hints)[0]
    return StreamRecord(
        kind=RecordKind.DATA,
        cursor=Cursor(stored.token),
        topic=wire.topic,
        producer_id=wire.producer_id,
        attempt=wire.attempt,
        sequence=wire.sequence,
        run_id=wire.metadata[RUN_ID_KEY].data.decode()
        if RUN_ID_KEY in wire.metadata
        else "",
        value=value,
    )


def _item_type(follower: StreamFollower[Any]) -> type | None:
    try:
        hints = typing.get_type_hints(type(follower).on_records)
    except Exception:
        return None
    batch = next(
        (hint for name, hint in hints.items() if name != "return"),
        None,
    )
    if typing.get_origin(batch) is not FollowBatch:
        return None
    (item,) = typing.get_args(batch)
    return item if isinstance(item, type) else None


def _context(source_context: Any) -> dict[str, Any]:
    # The Worker decodes the context with its payload converter, so the one
    # follow_stream sent arrives as a dict.
    return source_context if isinstance(source_context, dict) else {}


def _transfer(value: Any) -> Any:
    return json.loads(_CONTRACT.to_payload(value).data)


def _from_transfer(transfer: Any, as_type: type[T]) -> T:
    payload = Payload(
        metadata={"encoding": b"json/plain"}, data=json.dumps(transfer).encode()
    )
    return _CONTRACT.from_payload(payload, as_type)


def _state_in(state: bytes, stream: StreamRef | None) -> ReadInput:
    """The read position a receiver state holds, on ``stream`` when given."""
    if not state:
        assert stream is not None
        return ReadInput(stream=stream)
    try:
        position = _CONTRACT.from_payload(
            Payload(metadata={"encoding": b"json/plain"}, data=state), ReadInput
        )
    except Exception as error:
        raise _refused(f"the receiver state is not a read position: {error}") from None
    if stream is not None:
        position = dataclasses.replace(position, stream=stream)
    return position


def _state_out(position: ReadInput, cap: int) -> bytes:
    state = _CONTRACT.to_payload(position).data
    if len(state) <= cap:
        return state
    # Core's read state lets the marks hold across reads, and the cursor alone
    # still loses no record.
    return _CONTRACT.to_payload(dataclasses.replace(position, state=None)).data


def _moved_token(answer: ReadOutput) -> str | None:
    if answer.next_token:
        return answer.next_token
    return answer.records[-1].token if answer.records else None


def _moved(position: ReadInput, answer: ReadOutput) -> ReadInput:
    return ReadInput(
        stream=position.stream,
        after_token=_moved_token(answer) or position.after_token,
        state=answer.state,
    )


def _budget_end(ctx: StartOperationContext) -> float:
    loop = asyncio.get_running_loop()
    if ctx.request_deadline is None:
        return loop.time() + _DEFAULT_BUDGET.total_seconds()
    left = (_aware(ctx.request_deadline) - datetime.now(timezone.utc)).total_seconds()
    return loop.time() + max(left, 0) / 2


def _request_end(ctx: StartOperationContext) -> float:
    loop = asyncio.get_running_loop()
    if ctx.request_deadline is None:
        return loop.time() + 2 * _DEFAULT_BUDGET.total_seconds()
    left = (_aware(ctx.request_deadline) - datetime.now(timezone.utc)).total_seconds()
    # Leave time for the answer to reach the server.
    return loop.time() + max(left - 1, 0)


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def _refused(message: str) -> nexusrpc.HandlerError:
    return nexusrpc.HandlerError(
        message, type=nexusrpc.HandlerErrorType.BAD_REQUEST, retryable_override=False
    )
