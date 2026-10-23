"""Reads of a Stream in Workflow code.

Each page is a call whose answer is in History, so a replay reads the same items. A Stream
plugin's outbound interceptor reads a page with ``_temporal_stream_read_page``. A plugin whose
server parks reads also starts a wake with ``_temporal_stream_start_wake``.
"""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.nexus.system
import temporalio.workflow
from temporalio.contrib.streams import _convert, _errors
from temporalio.contrib.streams._items import Closed, End, Record, Superseded
from temporalio.contrib.streams.system import models

Item = Record[Any] | Superseded | Closed

# A backend that can't park a read pauses on a durable timer after a page with no Record,
# from the first pause up to the last.
_FIRST_PAUSE = timedelta(seconds=1)
_LAST_PAUSE = timedelta(seconds=60)
# How long a read waits for a close after the Stream's owner ended. The close of a run comes
# from its Worker, so it may come after the run closed, or never.
_OWNER_END_GRACE = timedelta(minutes=1)


@dataclass
class Page:
    """A page of a Workflow read, as History has it."""

    response: stream_pb.ReadRecordsResponse
    # The owner's run chain ended. Only a Store that doesn't close a Stream with its owner
    # sets it.
    owner_ended: bool = False


def reader_of(backend: str) -> Any:
    """The outbound interceptor of the Stream plugin with this backend name."""
    outbound = getattr(temporalio.workflow._Runtime.current(), "_outbound", None)
    while outbound is not None:
        if (
            getattr(outbound, "_temporal_stream_transport", False)
            and getattr(outbound, "_temporal_stream_backend_name", None) == backend
        ):
            return outbound
        outbound = getattr(outbound, "next", None)
    raise RuntimeError(f"No Stream plugin of the Worker has the backend {backend}")


@dataclass
class _ReadState:
    # The run's wakes that wait on the server, so the run's end cancels them.
    wakes: set[Any] = field(default_factory=set)
    # The lowest offset that the run's replayed pages read, per Stream, and whether a live
    # page sent it.
    replay_floors: dict[tuple[str, str], int] = field(default_factory=dict)
    sent_floors: set[tuple[str, str]] = field(default_factory=set)


def _read_state() -> _ReadState:
    runtime = temporalio.workflow._Runtime.current()
    state = getattr(runtime, "_temporal_streams_read_state", None)
    if state is None:
        state = _ReadState()
        setattr(runtime, "_temporal_streams_read_state", state)
    return state


def cancel_wakes() -> None:
    """Cancel the run's wakes at the end of the run, so the server drops their parked reads."""
    for wake in list(_read_state().wakes):
        wake.cancel()


class WorkflowItems(AsyncIterator[Item]):
    """The items of a Stream in order, in Workflow code. ``subscribe`` returns it.

    It ends at a close, or at the newest Record when ``wait_new_records`` is False. It also
    ends when this run should continue as new, and then :py:attr:`continue_as_new_suggested`
    is True. Pass :py:attr:`next_offset` to the next run, which reads on from there.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        reader: Any,
        ref: stream_pb.StreamRef,
        *,
        topics: Sequence[str] | None,
        prefixes: Sequence[str],
        types: Mapping[str, Any],
        from_offset: int | End | None,
        wait_new_records: bool,
        create_wait: timedelta | None,
    ) -> None:
        """Create the iterator. It makes no call until the first item."""
        self._reader = reader
        self._types = types
        self._wait_new_records = wait_new_records
        self._create_wait = create_wait
        self._request = stream_pb.ReadRecordsRequest(
            stream=ref, topics=list(topics or []), topic_prefixes=list(prefixes)
        )
        if from_offset is End.END:
            # END is only the first page's start. Each answer's next offset pins later
            # pages and the waits.
            self._request.start = stream_pb.ReadStart.READ_START_END
        elif from_offset:
            self._request.from_offset = from_offset
        else:
            self._request.start = stream_pb.ReadStart.READ_START_BEGINNING
        self._items: deque[Item] = deque()
        self._page_end = 0
        self._started = False
        self._ended = False
        self._closed = False
        self._create_until: datetime | None = None
        self._pause = _FIRST_PAUSE
        self._server_suggests = False
        self._retention_known = False
        self._range_until: datetime | None = None
        self._owner_ended_at: datetime | None = None
        self._wake: Any = None
        self.next_offset: int | End | None = from_offset or None
        """Where to read on from, as ``from_offset`` of the next read: after the last item
        that the iterator gave, or after the Records that its Topics skip. Before the first
        page it is the read's own ``from_offset``."""
        self.continue_as_new_suggested = False
        """Whether the read ended because this run should continue as new. The server
        suggests it for the run's History or for the Records that the reader keeps on the
        server. A reader of a native Stream that no Workflow owns gets it at half the Stream's
        Retention after its first page, so its replay finds the Records."""
        self.owner_ended = False
        """Whether the read ended because the run chain that owns the Stream ended with no
        close, such as a terminated run on Redis."""

    def __aiter__(self) -> WorkflowItems:
        """Return the iterator."""
        return self

    async def __anext__(self) -> Item:
        """Give the next item, reading pages and waiting as needed."""
        while not self._items:
            if self._ended or self._closed:
                raise StopAsyncIteration
            await self._read()
        if self._closed:
            # A page that came after the close.
            self._items.clear()
            raise StopAsyncIteration
        item = self._items.popleft()
        self.next_offset = item.offset + 1
        if not self._items:
            self.next_offset = max(item.offset + 1, self._page_end)
        return item

    async def aclose(self) -> None:
        """End the read. A wait on the server ends with it."""
        self._closed = True
        self._items.clear()
        if self._wake is not None:
            self._wake.cancel()

    async def _read(self) -> None:
        # Reads one page, or waits once at the end of the Stream.
        if temporalio.workflow.unsafe.is_read_only():
            raise _errors.ReadOnlyContextError("Can't read a Stream in read-only code")
        now = temporalio.workflow.now()
        if not self._started:
            self._started = True
            if self._create_wait:
                self._create_until = now + self._create_wait
        if self._suggests_continue_as_new(now):
            self.continue_as_new_suggested = True
            self._ended = True
            return
        request = self._waiting_request()
        if not temporalio.workflow.unsafe.is_replaying():
            self._send_floor(request)
        try:
            page = await self._reader._temporal_stream_read_page(request)
        except _errors.StreamNotFoundError:
            if not _left(self._create_until):
                raise
            await self._wait_for_create()
            return
        response = page.response
        await self._take(request, response)
        if response.records or response.done:
            self._pause = _FIRST_PAUSE
            return
        if not self._wait_new_records:
            self._ended = True
            return
        if self._owner_gone(page, now):
            self.owner_ended = True
            self._ended = True
            return
        if self._suggests_continue_as_new(now):
            self.continue_as_new_suggested = True
            self._ended = True
            return
        await self._wait_for_records(now)

    def _waiting_request(self) -> stream_pb.ReadRecordsRequest:
        # The next read with the backend's waits. A backend that parks reads on the server
        # takes the waits off its pages and puts them on its wakes.
        request = stream_pb.ReadRecordsRequest()
        request.CopyFrom(self._request)
        if self._wait_new_records:
            request.wait.FromTimedelta(self._reader._temporal_stream_read_wait)
        left = _left(self._create_until)
        if left:
            request.create_wait.FromTimedelta(
                min(left, self._reader._temporal_stream_create_wait)
            )
        return request

    def _send_floor(self, request: stream_pb.ReadRecordsRequest) -> None:
        # A reset run replays the pages of the old run. Its first live page asks the server
        # to keep them too, since its next replay reads them again.
        key = self._key()
        state = _read_state()
        floor = state.replay_floors.get(key)
        if floor is not None and key not in state.sent_floors:
            request.floor_from = floor
            state.sent_floors.add(key)

    async def _take(
        self, request: stream_pb.ReadRecordsRequest, page: stream_pb.ReadRecordsResponse
    ) -> None:
        for record in page.records:
            item = workflow_item(record, self._types)
            if item is None:
                continue
            self._items.append(item)
            if isinstance(item, Closed):
                self._ended = True
                break
        if page.done:
            self._ended = True
        if page.next_offset:
            self._request.from_offset = page.next_offset
            self._request.ClearField("start")
            self._page_end = page.next_offset
            if not self._items:
                offset = self.next_offset if isinstance(self.next_offset, int) else 0
                self.next_offset = max(offset, page.next_offset)
        if page.continue_as_new_suggested:
            self._server_suggests = True
        if not page.records:
            return
        if temporalio.workflow.unsafe.is_replaying():
            key = self._key()
            floors = _read_state().replay_floors
            low = request.from_offset or page.records[0].offset
            floors[key] = min(floors.get(key, low), low)
        if not self._retention_known:
            self._retention_known = True
            await self._learn_retention()

    async def _learn_retention(self) -> None:
        # Retention trims a Stream that no Workflow owns under its readers. A backend whose
        # replay reads the Stream again needs the reader to move on before that.
        stream = self._request.stream
        if stream.workflow_id or not self._reader._temporal_stream_parks_reads:
            return
        description = await self._reader._temporal_stream_describe(
            stream_pb.DescribeStreamRequest(stream=stream)
        )
        retention = description.retention.ToTimedelta()
        if retention > timedelta():
            self._range_until = temporalio.workflow.now() + retention / 2

    def _suggests_continue_as_new(self, now: datetime) -> bool:
        return (
            self._server_suggests
            or temporalio.workflow.info().is_continue_as_new_suggested()
            or (self._range_until is not None and now >= self._range_until)
        )

    def _owner_gone(self, page: Page, now: datetime) -> bool:
        if not page.owner_ended:
            self._owner_ended_at = None
            return False
        if self._owner_ended_at is None:
            self._owner_ended_at = now
        return now - self._owner_ended_at >= _OWNER_END_GRACE

    async def _wait_for_records(self, now: datetime) -> None:
        if not self._reader._temporal_stream_parks_reads:
            # The Store can't wake a Workflow, so a quiet Stream costs one timer and one read
            # per pause.
            await self._sleep()
            return
        request = self._waiting_request()
        if self._range_until is not None:
            # The wake ends in time for the read to end at half the Retention.
            wait = min(
                request.wait.ToTimedelta(),
                max(self._range_until - now, _FIRST_PAUSE),
            )
            request.wait.FromTimedelta(wait)
        await self._wake_once(request)

    async def _wait_for_create(self) -> None:
        if not self._reader._temporal_stream_parks_reads:
            await self._sleep()
            return
        request = self._waiting_request()
        if not self._wait_new_records:
            # After the create, the read takes what is there and ends.
            request.wait.FromTimedelta(_FIRST_PAUSE)
        try:
            await self._wake_once(request)
        except _errors.StreamNotFoundError:
            # The next page tells whether time is left.
            pass

    async def _sleep(self) -> None:
        await temporalio.workflow.sleep(self._pause)
        self._pause = min(self._pause * 2, _LAST_PAUSE)

    async def _wake_once(self, request: stream_pb.ReadRecordsRequest) -> None:
        # The server parks the wake until a Record passes the filter, the Stream closes, or
        # the wait ends. Its answer holds no Records, and the next page reads them.
        wake = await self._reader._temporal_stream_start_wake(request)
        if self._closed:
            # The iterator closed while the server took the wake.
            wake.cancel()
            return
        wakes = _read_state().wakes
        self._wake = wake
        wakes.add(wake)
        try:
            answer = await wake
        except temporalio.exceptions.NexusOperationError as err:
            if self._closed or isinstance(
                err.cause, temporalio.exceptions.TimeoutError
            ):
                # A namespace may cap a Nexus operation below the wake's wait, which ends the
                # wake as an empty one would.
                return
            raise error_of(err) from None
        finally:
            self._wake = None
            wakes.discard(wake)
        if (
            self._request.start == stream_pb.ReadStart.READ_START_END
            and answer.next_offset
        ):
            # A wait for the create answers where END was when the Stream came.
            self._request.from_offset = answer.next_offset
            self._request.ClearField("start")

    def _key(self) -> tuple[str, str]:
        return (self._reader._temporal_stream_backend_name, self._request.stream.id)


def _left(until: datetime | None) -> timedelta | None:
    if until is None:
        return None
    left = until - temporalio.workflow.now()
    return left if left > timedelta() else None


def workflow_item(
    record: stream_pb.StreamRecord, types: Mapping[str, Any]
) -> Item | None:
    """One read item, with the body decoded by the Workflow's converter and its Topic's type.

    The codec already ran outside the sandbox, on the page.
    """
    converter = temporalio.workflow.payload_converter()
    # The generated converter makes the producer and the supersession models, and the body
    # stays a Payload that the Topic's type decodes.
    with temporalio.nexus.system._user_converter_context(
        temporalio.nexus.system._SystemNexusUserConverters(
            payload_converter=_convert._RawPayloadConverter(),
            failure_converter=_failure_converter(),
        )
    ):
        model = models._StreamRecordTransferTypeConverter().from_transfer_type(
            record, models.StreamRecord
        )
    offset = model.offset or 0
    if model.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA:
        topic = model.topic or ""
        return Record(
            topic=topic,
            data=_decode(converter, model.body, types.get(topic)),
            offset=offset,
            producer=model.producer,
        )
    if model.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_SUPERSEDED:
        if model.supersession is None:
            return None
        return Superseded(offset=offset, supersession=model.supersession)
    if model.kind == stream_pb.StreamRecordKind.STREAM_RECORD_KIND_CLOSE:
        return Closed(
            offset=offset,
            result=_decode(converter, model.close_result, None),
            failure=model.close_failure,
        )
    return None


def _decode(
    converter: temporalio.converter.PayloadConverter, body: Any, type_hint: Any
) -> Any:
    if not isinstance(body, temporalio.common.RawValue):
        return body
    return converter.from_payload(body.payload, type_hint)


def _failure_converter() -> temporalio.converter.FailureConverter:
    runtime = temporalio.workflow._Runtime.current()
    return getattr(
        runtime,
        "_workflow_context_failure_converter",
        temporalio.converter.DefaultFailureConverter(),
    )


def error_of(err: BaseException) -> BaseException:
    """The Stream error that a failed page carries as its cause, or the error itself.

    A page's failure is in History, so a replay raises the same error.
    """
    cause: BaseException | None = err
    while cause is not None:
        if isinstance(cause, temporalio.exceptions.ApplicationError):
            stream_error = _errors.from_application_error(cause)
            if stream_error is not None:
                return stream_error
        cause = cause.__cause__
    return err
