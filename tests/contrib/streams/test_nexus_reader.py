"""A Workflow reads the stream a Nexus operation returns.

The reader waits on the operation's progress and reads through the stream
service as Nexus operations of the Workflow, so History records every read and
a replay sees the same batches. These tests replay histories built by hand:
the stream operation, its progress on Workflow Task scheduled events, and each
read with the answer the stream service gave.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import types
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest
from google.protobuf.any_pb2 import Any as AnyProto
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

import temporalio.contrib.streams.nexus._reader as reader_module
import temporalio.nexus
from temporalio import workflow
from temporalio.api.common.v1 import Payload, Payloads, WorkflowType
from temporalio.api.enums.v1 import EventType, StreamOwnerKind
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.history.v1 import (
    HistoryEvent,
    NexusOperationCompletedEventAttributes,
    NexusOperationFailedEventAttributes,
    NexusOperationScheduledEventAttributes,
    NexusOperationStartedEventAttributes,
    TimerCanceledEventAttributes,
    TimerFiredEventAttributes,
    TimerStartedEventAttributes,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
    WorkflowTaskCompletedEventAttributes,
    WorkflowTaskScheduledEventAttributes,
    WorkflowTaskStartedEventAttributes,
)
from temporalio.api.nexus.v1 import NexusOperationProgress as ProgressProto
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    DescribeStreamNotifierRequest,
    DescribeStreamNotifierResponse,
)
from temporalio.client import Client, WorkflowHistory
from temporalio.contrib.streams import (
    StreamRecordError,
    StreamRef,
    StreamStorePlugin,
    Supersession,
    workflow_writer,
)
from temporalio.contrib.streams._record import Cursor, RecordKind
from temporalio.contrib.streams._wire import WireRecord, to_wire
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    ReadInput,
    ReadOutput,
    ReadPosition,
    ReadStaleRecord,
    ReadSupersession,
    RecordWire,
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.contrib.streams.nexus._generated import (
    RecordWireSuperseded,
    StreamOperationToken,
)
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError, NexusOperationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from tests.contrib.streams._support import call_workflow_service
from tests.helpers.nexus import make_nexus_endpoint_name

ENDPOINT = "chat-endpoint"
TASK_QUEUE = "reader-task-queue"
STREAMS_SERVICE = "temporal.sdk.streams.v1.TemporalStreams"
STREAM = StreamRef(kind="workflow", workflow_id="chat-1", topic="tokens")
CHAT_TOKEN = StreamOperationToken(
    ref=STREAM, attach="attach-request", chain="run-id"
).to_token()

converter = DataConverter.default.payload_converter


@dataclass
class Token:
    text: str


@nexusrpc.service
class ChatService:
    chat: nexusrpc.Operation[str, str]


@dataclass
class Seen:
    batches: list[list[Token]] = field(default_factory=list)
    after_end: list[Token] | None = None
    error: str | None = None
    supersessions: list[ReadSupersession] = field(default_factory=list)
    stale_records: list[ReadStaleRecord] = field(default_factory=list)
    undecodable: list[str] = field(default_factory=list)


seen: list[Seen] = []


@workflow.defn(name="ChatReader")
class ChatReader:
    @workflow.run
    async def run(self, after: str = "") -> None:
        client = workflow.create_nexus_client(service=ChatService, endpoint=ENDPOINT)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(
            handle,
            item_type=Token,
            endpoint=ENDPOINT,
            after=Cursor(after) if after else None,
            replay_from_store=False,
        )
        observed = Seen()
        try:
            while True:
                try:
                    batch = await reader.next()
                except StreamRecordError as error:
                    observed.undecodable.append(str(error.cursor))
                    continue
                if batch is None:
                    break
                observed.batches.append(batch)
            observed.after_end = await reader.next()
        except Exception as error:
            # The server wraps a Nexus failure in NexusOperationError; a
            # hand-built failure event reaches the Workflow unwrapped.
            cause = error.cause if isinstance(error, NexusOperationError) else error
            observed.error = type(cause).__name__
        observed.supersessions = list(reader.supersessions)
        observed.stale_records = list(reader.stale_records)
        seen.append(observed)


class History:
    """Builds the reader Workflow's History event by event."""

    def __init__(self) -> None:
        self.events: list[HistoryEvent] = []
        self._time = datetime(2026, 10, 10, tzinfo=timezone.utc)
        self._completed = 0

    def add(self, event_type: EventType.ValueType, **attributes: object) -> int:
        self._time += timedelta(milliseconds=10)
        event_time = Timestamp()
        event_time.FromDatetime(self._time)
        event = HistoryEvent(
            event_id=len(self.events) + 1,
            event_type=event_type,
            event_time=event_time,
            **attributes,  # type: ignore[arg-type]
        )
        self.events.append(event)
        return event.event_id

    def started(self, *args: object) -> None:
        self.add(
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
            workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                workflow_type=WorkflowType(name="ChatReader"),
                task_queue=TaskQueue(name=TASK_QUEUE),
                input=Payloads(payloads=converter.to_payloads(args)) if args else None,
                original_execution_run_id="run-id",
                first_execution_run_id="run-id",
                attempt=1,
            ),
        )

    def task(self, *progress: ProgressProto) -> None:
        scheduled = self.add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
            workflow_task_scheduled_event_attributes=WorkflowTaskScheduledEventAttributes(
                task_queue=TaskQueue(name=TASK_QUEUE),
                start_to_close_timeout=Duration(seconds=10),
                attempt=1,
                attachments=[packed(p) for p in progress],
            ),
        )
        started = self.add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED,
            workflow_task_started_event_attributes=WorkflowTaskStartedEventAttributes(
                scheduled_event_id=scheduled
            ),
        )
        self._completed = self.add(
            EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
            workflow_task_completed_event_attributes=WorkflowTaskCompletedEventAttributes(
                scheduled_event_id=scheduled, started_event_id=started
            ),
        )

    def scheduled(self, service: str, operation: str) -> int:
        return self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
            nexus_operation_scheduled_event_attributes=NexusOperationScheduledEventAttributes(
                endpoint=ENDPOINT,
                service=service,
                operation=operation,
                input=Payload(),
                workflow_task_completed_event_id=self._completed,
                request_id=f"request-{len(self.events)}",
            ),
        )

    def chat_started(self, scheduled_event_id: int) -> None:
        self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_STARTED,
            nexus_operation_started_event_attributes=NexusOperationStartedEventAttributes(
                scheduled_event_id=scheduled_event_id,
                operation_token=CHAT_TOKEN,
                request_id="request-chat",
            ),
        )

    def completed(self, scheduled_event_id: int, result: object) -> None:
        self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
            nexus_operation_completed_event_attributes=NexusOperationCompletedEventAttributes(
                scheduled_event_id=scheduled_event_id,
                result=converter.to_payload(result),
            ),
        )

    def failed(self, scheduled_event_id: int, operation: str) -> None:
        self.add(
            EventType.EVENT_TYPE_NEXUS_OPERATION_FAILED,
            nexus_operation_failed_event_attributes=NexusOperationFailedEventAttributes(
                scheduled_event_id=scheduled_event_id,
                failure=Failure(
                    message=f"{operation} failed",
                    application_failure_info=ApplicationFailureInfo(type="Broken"),
                ),
            ),
        )

    def timer_started(self, seconds: int, timer_id: str = "1") -> int:
        """The timer the reader starts in the task just completed."""
        return self.add(
            EventType.EVENT_TYPE_TIMER_STARTED,
            timer_started_event_attributes=TimerStartedEventAttributes(
                timer_id=timer_id,
                start_to_fire_timeout=Duration(seconds=seconds),
                workflow_task_completed_event_id=self._completed,
            ),
        )

    def timer_fired(
        self, started_event_id: int, seconds: int, timer_id: str = "1"
    ) -> None:
        self._time += timedelta(seconds=seconds)
        self.add(
            EventType.EVENT_TYPE_TIMER_FIRED,
            timer_fired_event_attributes=TimerFiredEventAttributes(
                timer_id=timer_id, started_event_id=started_event_id
            ),
        )

    def timer_canceled(self, started_event_id: int, timer_id: str = "1") -> None:
        self.add(
            EventType.EVENT_TYPE_TIMER_CANCELED,
            timer_canceled_event_attributes=TimerCanceledEventAttributes(
                timer_id=timer_id,
                started_event_id=started_event_id,
                workflow_task_completed_event_id=self._completed,
            ),
        )

    def read(self, output: ReadOutput) -> None:
        """One read the reader issues in the task just completed, and its answer."""
        self.completed(self.scheduled(STREAMS_SERVICE, "read"), output)

    def workflow_completed(self) -> None:
        self.add(
            EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
            workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
                result=Payloads(payloads=[Payload()]),
                workflow_task_completed_event_id=self._completed,
            ),
        )


def record(
    position: int,
    kind: RecordKind = RecordKind.DATA,
    text: str = "",
    *,
    attempt: int = 1,
    sequence: int | None = None,
    stale: bool = False,
) -> RecordWire:
    wire = to_wire(
        converter,
        topic=STREAM.topic,
        kind=kind,
        value=Token(text) if kind is RecordKind.DATA else None,
        producer_id="chat",
        attempt=attempt,
        sequence=position if sequence is None else sequence,
    )
    return RecordWire(
        token=f"cursor-{position}",
        record=wire.SerializeToString(),
        stale=True if stale else None,
    )


def answer(*records: RecordWire, done: bool = False, cursor: str = "") -> ReadOutput:
    return ReadOutput(
        records=list(records),
        next_token=records[-1].token if records else cursor,
        done=done,
    )


def progress(counter: int) -> ProgressProto:
    return ProgressProto(
        operation_key=ProgressProto.OperationKey(
            endpoint=ENDPOINT,
            service="ChatService",
            operation="chat",
            operation_token=CHAT_TOKEN,
        ),
        position=f"cursor-{counter}",
        counter=counter,
    )


def packed(progress: ProgressProto) -> AnyProto:
    attachment = AnyProto()
    attachment.Pack(progress)
    return attachment


def stream_history() -> WorkflowHistory:
    """The stream gets records a and b, then c, then closes with a FINISH record."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    # The first read covers everything up to the progress the start brought.
    h.task(progress(1))
    h.read(answer(record(1, text="a"), record(2, text="b")))
    # The reader hands over a and b. The answer had records, so it reads on.
    h.task()
    h.read(answer(cursor="cursor-2"))
    # An empty answer is the tail, so it waits for newer progress.
    h.task()
    h.task(progress(2))
    h.read(answer(record(3, text="c")))
    h.task()
    h.read(answer(cursor="cursor-3"))
    h.task()
    # The stream closed, which completed the operation. The reader drains what
    # is left and ends.
    h.completed(chat, "summary")
    h.task()
    h.read(answer(record(4, RecordKind.FINISH), done=True))
    h.task()
    h.workflow_completed()
    return WorkflowHistory(workflow_id="chat-reader", events=h.events)


async def replay(
    history: WorkflowHistory, data_converter: DataConverter = DataConverter.default
) -> None:
    await Replayer(
        workflows=[ChatReader],
        workflow_runner=UnsandboxedWorkflowRunner(),
        data_converter=data_converter,
    ).replay_workflow(history)


class MarkingCodec(PayloadCodec):
    """Wraps each payload in an envelope only this codec opens."""

    encoding = b"binary/n15-test-codec"

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(metadata={"encoding": self.encoding}, data=p.SerializeToString())
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload.FromString(p.data)
            if p.metadata.get("encoding") == self.encoding
            else p
            for p in payloads
        ]


@pytest.fixture(autouse=True)
def clear_seen() -> None:
    seen.clear()


async def test_the_reader_hands_over_every_record_in_batches_then_ends() -> None:
    await replay(stream_history())
    assert seen == [
        Seen(
            batches=[[Token("a"), Token("b")], [Token("c")]],
            after_end=None,
        )
    ]


async def test_the_reader_reads_the_same_on_every_replay() -> None:
    await replay(stream_history())
    await replay(stream_history())
    assert len(seen) == 2
    assert seen[0] == seen[1]


async def test_a_full_batch_is_followed_by_a_read_without_waiting() -> None:
    """A read that fills the batch may have left records behind, so the next
    call reads again before it waits for progress."""
    full = [record(n, text=str(n)) for n in range(1, 101)]
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    h.read(answer(*full))
    h.task()
    h.read(answer(record(101, text="101")))
    h.task()
    h.read(answer(cursor="cursor-101"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-101", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [
        [Token(str(n)) for n in range(1, 101)],
        [Token("101")],
    ]


async def test_after_the_operation_completes_only_the_streams_end_ends_it() -> None:
    """After the operation completes, neither a short answer nor an empty one
    ends the reader: only the stream service saying the stream is done."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task()
    # The first read finds nothing, so the reader waits.
    h.read(answer())
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(record(1, text="a")))
    h.task()
    # Empty, but not done: the stream is still open, so the reader reads on.
    h.read(answer(cursor="cursor-1"))
    h.task()
    h.read(answer(record(2, text="b")))
    h.task()
    h.read(answer(cursor="cursor-2", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a")], [Token("b")]]
    assert seen[0].after_end is None


async def test_after_a_completion_a_stream_that_never_ends_raises() -> None:
    """A stream that never says it's done would grow the caller's History for
    ever, so the drain after a completion is bounded."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task()
    h.read(answer())
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer())
    h.task()
    h.read(answer())
    # A minute of empty reads later.
    h._time += timedelta(seconds=61)  # type: ignore[reportPrivateUsage]
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].error == "StreamIncompleteError"


async def test_a_stream_done_without_a_completion_raises_after_the_drain_limit() -> (
    None
):
    """The store says the stream is done, but the operation never completes,
    for example when the owner closed its stream and runs on for days. The
    reader doesn't wait on progress for ever."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(2))
    h.read(answer(record(1, text="a"), record(2, RecordKind.FINISH), done=True))
    h.task()
    h.timer_fired(h.timer_started(60), 60)
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a")]]
    assert seen[0].error == "StreamIncompleteError"


async def test_a_completion_after_the_stream_is_done_ends_the_reader() -> None:
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(2))
    h.read(answer(record(1, text="a"), record(2, RecordKind.FINISH), done=True))
    h.task()
    timer = h.timer_started(60)
    h.completed(chat, "summary")
    h.task()
    h.timer_canceled(timer)
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a")]]
    assert seen[0].after_end is None
    assert seen[0].error is None


async def test_a_reader_with_no_progress_reads_again_after_a_recheck() -> None:
    """A producer that dies between its append and its notify sends no
    progress. The reader's recheck timer reads again and finds the record."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    h.read(answer(record(1, text="a")))
    h.task()
    h.read(answer(cursor="cursor-1"))
    h.task()
    # No progress comes, so the recheck fires after a minute.
    recheck = h.timer_started(60)
    h.timer_fired(recheck, 60)
    h.task()
    h.read(answer(record(2, text="b")))
    h.task()
    h.read(answer(cursor="cursor-2"))
    h.task()
    # The next recheck waits twice as long. The completion comes first.
    second = h.timer_started(120, timer_id="2")
    h.completed(chat, "summary")
    h.task()
    h.timer_canceled(second, timer_id="2")
    h.read(answer(cursor="cursor-2", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a")], [Token("b")]]
    assert seen[0].error is None


async def test_a_record_too_large_to_cross_raises_and_the_reader_goes_on() -> None:
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    h.read(
        answer(
            record(1, text="a"),
            RecordWire(token="cursor-2", record=b"", error="too large"),
            record(3, text="c"),
        )
    )
    # a is handed over, the large record raises, then the rest of the answer
    # is handed over with no read again.
    h.task()
    h.read(answer(cursor="cursor-3"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-3", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a")], [Token("c")]]
    assert seen[0].undecodable == ["cursor-2"]


async def test_a_read_error_raises_from_next() -> None:
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    h.failed(h.scheduled(STREAMS_SERVICE, "read"), "read")
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen == [Seen(error="ApplicationError")]


async def test_a_failed_operation_raises_after_its_records_are_read() -> None:
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    h.read(answer(record(1, text="a")))
    h.task()
    h.read(answer(cursor="cursor-1"))
    h.task()
    h.failed(chat, "chat")
    h.task()
    h.read(answer(record(2, text="b")))
    h.task()
    # The read waited a moment and found nothing more.
    h.read(answer(cursor="cursor-2"))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen == [
        Seen(batches=[[Token("a")], [Token("b")]], error="ApplicationError")
    ]


async def test_the_reader_records_where_a_producer_attempt_was_superseded() -> None:
    """The second attempt of the producer rewrites from its first record. The
    batch carries both attempts' data, and the reader says where the stream
    service marked the second attempt taking over."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    h.read(
        answer(
            record(1, text="a"),
            record(2, text="b"),
            superseded(2, 1, 2),
            record(3, text="a2", attempt=2, sequence=1),
        )
    )
    h.task()
    h.read(answer(cursor="cursor-3"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-3", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a"), Token("b"), Token("a2")]]
    assert seen[0].supersessions == [
        ReadSupersession(
            index=2,
            cursor=Cursor("cursor-2"),
            supersession=Supersession(
                producer_id="chat", previous_attempt=1, attempt=2
            ),
        )
    ]


def superseded(position: int, previous_attempt: int, attempt: int) -> RecordWire:
    """What the stream service sends when its reader saw a later attempt start."""
    return RecordWire(
        token=f"cursor-{position}",
        record=b"",
        superseded=RecordWireSuperseded(
            producer_id="chat", previous_attempt=previous_attempt, attempt=attempt
        ),
    )


async def test_a_resumed_reader_takes_the_supersession_at_its_cursor() -> None:
    """A run after Continue-as-New reads after the earlier run's cursor, so
    its own tracker never saw the earlier attempt. The stream service did,
    from the record at the cursor, and its supersession is what tells the
    later attempt's records apart."""
    h = History()
    h.started("cursor-3")
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(5))
    h.read(
        answer(
            superseded(3, 1, 2),
            record(4, text="a2", attempt=2, sequence=1),
            record(5, text="b2", attempt=2, sequence=2),
        )
    )
    h.task()
    h.read(answer(cursor="cursor-5"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-5", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a2"), Token("b2")]]
    assert seen[0].supersessions == [
        ReadSupersession(
            index=0,
            cursor=Cursor("cursor-3"),
            supersession=Supersession(
                producer_id="chat", previous_attempt=1, attempt=2
            ),
        )
    ]


async def test_a_stale_record_is_reported_and_kept_out_of_the_batch() -> None:
    """Attempt 1 timed out but kept running, and wrote once more after attempt
    2 started. That token is not the current answer."""
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(4))
    h.read(
        answer(
            record(1, text="a"),
            record(2, text="a2", attempt=2, sequence=1),
            record(3, text="late", attempt=1, sequence=2, stale=True),
            record(4, text="b2", attempt=2, sequence=2),
        )
    )
    h.task()
    h.read(answer(cursor="cursor-4"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-4", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a"), Token("a2"), Token("b2")]]
    assert seen[0].stale_records == [
        ReadStaleRecord(
            index=2,
            cursor=Cursor("cursor-3"),
            producer_id="chat",
            attempt=1,
            value=Token("late"),
        )
    ]


async def test_a_stale_record_that_cannot_cross_is_reported_without_a_value() -> None:
    """A stale record isn't the current answer, so one the service couldn't
    hand over, or one that doesn't decode, is reported and raises nothing."""
    undecodable = to_wire(
        converter,
        topic=STREAM.topic,
        kind=RecordKind.DATA,
        value="not a token",
        producer_id="chat",
        attempt=1,
        sequence=3,
    )
    h = History()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(4))
    h.read(
        answer(
            record(1, text="a2", attempt=2, sequence=1),
            RecordWire(token="cursor-2", record=b"", error="too large", stale=True),
            RecordWire(
                token="cursor-3", record=undecodable.SerializeToString(), stale=True
            ),
            record(4, text="b2", attempt=2, sequence=2),
        )
    )
    h.task()
    h.read(answer(cursor="cursor-4"))
    h.task()
    h.completed(chat, "summary")
    h.task()
    h.read(answer(cursor="cursor-4", done=True))
    h.task()
    h.workflow_completed()
    await replay(WorkflowHistory(workflow_id="chat-reader", events=h.events))
    assert seen[0].batches == [[Token("a2"), Token("b2")]]
    assert seen[0].undecodable == []
    assert [(r.cursor, r.value) for r in seen[0].stale_records] == [
        (Cursor("cursor-2"), None),
        (Cursor("cursor-3"), None),
    ]


class _CompletedHandle:
    """An operation handle whose operation already completed."""

    operation_token = CHAT_TOKEN
    latest_progress = None

    async def progress(self, *, after_counter: int = 0) -> None:
        del after_counter
        return None

    def __await__(self) -> Any:
        async def result() -> str:
            return "summary"

        return result().__await__()


async def test_each_read_sends_the_state_the_last_answer_gave(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replay doesn't compare a Nexus command's input, so a fake client
    records what each read sends."""
    answers = [
        ReadOutput(
            records=[record(1, text="a")],
            next_token="cursor-1",
            done=False,
            state=b"s1",
        ),
        ReadOutput(records=[], next_token="cursor-1", done=False, state=b"s2"),
        ReadOutput(records=[], next_token="cursor-1", done=True, state=b"s3"),
    ]
    sent: list[bytes | None] = []

    class FakeClient:
        async def execute_operation(self, _operation: Any, input: ReadInput) -> Any:
            sent.append(input.state)
            return answers.pop(0)

    fake_workflow = types.SimpleNamespace(
        create_nexus_client=lambda **_kwargs: FakeClient(),
        payload_converter=lambda: converter,
        now=lambda: datetime.now(timezone.utc),
    )
    monkeypatch.setattr(reader_module, "workflow", fake_workflow)
    reader = StreamReader(
        _CompletedHandle(),  # type: ignore[arg-type]
        item_type=Token,
        endpoint=ENDPOINT,
        after=ReadPosition(cursor=Cursor(""), state=base64.b64encode(b"s0").decode()),
        replay_from_store=False,
    )
    assert await reader.next() == [Token("a")]
    assert await reader.next() is None
    assert sent == [b"s0", b"s1", b"s2"]
    assert reader.position.state == base64.b64encode(b"s3").decode()


class _OpenHandle:
    """An open operation handle that logs each progress wait in ``calls``."""

    operation_token = _CompletedHandle.operation_token
    latest_progress = None

    def __init__(self, calls: list[tuple[Any, ...]], waits: int) -> None:
        self._calls = calls
        self._waits = waits

    async def progress(self, *, after_counter: int = 0) -> Any:
        self._calls.append(("progress", after_counter))
        if self._waits == 0:
            return None
        self._waits -= 1
        return types.SimpleNamespace(counter=after_counter + 1)

    def __await__(self) -> Any:
        async def result() -> str:
            return "summary"

        return result().__await__()


def _fake_reads(
    monkeypatch: pytest.MonkeyPatch,
    answers: list[ReadOutput],
    calls: list[tuple[Any, ...]],
) -> None:
    class FakeClient:
        async def execute_operation(self, _operation: Any, input: ReadInput) -> Any:
            calls.append(("read", input.wait_ms))
            return answers.pop(0)

    fake_workflow = types.SimpleNamespace(
        create_nexus_client=lambda **_kwargs: FakeClient(),
        payload_converter=lambda: converter,
        now=lambda: datetime.now(timezone.utc),
    )
    monkeypatch.setattr(reader_module, "workflow", fake_workflow)


async def test_each_read_waits_the_idle_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A read with records answers at once, so only a gap longer than the idle
    wait puts the reader to sleep on progress."""
    calls: list[tuple[Any, ...]] = []
    _fake_reads(
        monkeypatch,
        [
            answer(record(1, text="a")),
            ReadOutput(records=[], next_token="cursor-1", done=False),
            answer(record(2, text="b")),
            ReadOutput(records=[], next_token="cursor-2", done=False),
            # The drain after the completion waits for the stream's end.
            answer(cursor="cursor-2", done=True),
        ],
        calls,
    )
    reader = StreamReader(
        _OpenHandle(calls, waits=1),  # type: ignore[arg-type]
        item_type=Token,
        endpoint=ENDPOINT,
        replay_from_store=False,
    )
    assert await reader.next() == [Token("a")]
    assert await reader.next() == [Token("b")]
    assert await reader.next() is None
    assert calls == [
        ("read", 2000),
        ("read", 2000),
        ("progress", 0),
        ("read", 2000),
        ("read", 2000),
        ("progress", 1),
        ("read", 2000),
    ]


async def test_a_zero_idle_wait_sleeps_on_the_first_empty_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty answer puts the reader to sleep on progress."""
    calls: list[tuple[Any, ...]] = []
    _fake_reads(
        monkeypatch,
        [
            answer(cursor=""),
            answer(record(1, text="a")),
        ],
        calls,
    )
    reader = StreamReader(
        _OpenHandle(calls, waits=1),  # type: ignore[arg-type]
        item_type=Token,
        endpoint=ENDPOINT,
        idle_wait=timedelta(0),
        replay_from_store=False,
    )
    assert await reader.next() == [Token("a")]
    assert calls == [("read", 0), ("progress", 0), ("read", 0)]


def test_the_idle_wait_is_not_negative() -> None:
    with pytest.raises(ValueError, match="idle_wait"):
        StreamReader(
            _OpenHandle([], waits=0),  # type: ignore[arg-type]
            item_type=Token,
            endpoint=ENDPOINT,
            idle_wait=timedelta(seconds=-1),
            replay_from_store=False,
        )


def _base_read_input(after_token: str | None, *, drain: bool = False) -> Payload:
    """A read as the reader sent it before it armed: no ``arm``, no idle wait."""
    return converter.to_payload(
        ReadInput(
            stream=STREAM,
            after_token=after_token,
            max_records=100,
            wait_ms=2000 if drain else 0,
        )
    )


async def test_a_history_from_before_readers_armed_replays() -> None:
    """The reads of an older reader carry other inputs, and their answers no
    ``armed``. Core matches a scheduled Nexus operation by its event type, so
    the History replays with no patch."""
    history = stream_history()
    inputs = iter(
        [
            _base_read_input(None),
            _base_read_input("cursor-2"),
            _base_read_input("cursor-2"),
            _base_read_input("cursor-3"),
            _base_read_input("cursor-3", drain=True),
        ]
    )
    for event in history.events:
        if event.HasField("nexus_operation_scheduled_event_attributes"):
            attributes = event.nexus_operation_scheduled_event_attributes
            if attributes.operation == "read":
                attributes.input.CopyFrom(next(inputs))
    assert next(inputs, None) is None
    await replay(history)
    assert seen == [
        Seen(batches=[[Token("a"), Token("b")], [Token("c")]], after_end=None)
    ]


async def test_a_bad_last_record_still_takes_the_answers_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bad = to_wire(
        converter,
        topic=STREAM.topic,
        kind=RecordKind.DATA,
        value="not a token",
        producer_id="chat",
        attempt=1,
        sequence=2,
    )
    answers = [
        ReadOutput(
            records=[
                record(1, text="a"),
                RecordWire(token="cursor-2", record=bad.SerializeToString()),
            ],
            next_token="cursor-9",
            done=False,
            state=b"s1",
        ),
        ReadOutput(records=[], next_token="cursor-9", done=True, state=b"s2"),
    ]
    sent: list[tuple[str | None, bytes | None]] = []

    class FakeClient:
        async def execute_operation(self, _operation: Any, input: ReadInput) -> Any:
            sent.append((input.after_token, input.state))
            return answers.pop(0)

    fake_workflow = types.SimpleNamespace(
        create_nexus_client=lambda **_kwargs: FakeClient(),
        payload_converter=lambda: converter,
        now=lambda: datetime.now(timezone.utc),
    )
    monkeypatch.setattr(reader_module, "workflow", fake_workflow)
    reader = StreamReader(
        _CompletedHandle(),  # type: ignore[arg-type]
        item_type=Token,
        endpoint=ENDPOINT,
        replay_from_store=False,
    )
    assert await reader.next() == [Token("a")]
    with pytest.raises(StreamRecordError):
        await reader.next()
    assert await reader.next() is None
    # The read after the bad last record sends that answer's state and token.
    assert sent[1] == ("cursor-9", b"s1")


async def test_a_direct_read_chains_only_after_a_read_of_the_same_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from temporalio.common import RawValue
    from temporalio.contrib.streams._replay_reads import ReadChain
    from temporalio.contrib.streams.nexus._direct import batch_payload

    answers = [
        ReadOutput(records=[record(1, text="a")], next_token="cursor-1", done=False),
        ReadOutput(records=[record(2, text="b")], next_token="cursor-2", done=False),
        ReadOutput(records=[], next_token="cursor-2", done=True),
    ]
    # The first read's marker is another run's, as a reset run replays it.
    runs = ["old-run", "this-run", "this-run"]
    chained: list[bool] = []

    async def execute_local_activity(_name: str, *, args: Any, **_: Any) -> Any:
        chained.append(args[4])
        seq = args[3]
        return RawValue(
            batch_payload(answers.pop(0), ReadChain(f"{runs[seq - 1]}/1", seq))
        )

    fake_workflow = types.SimpleNamespace(
        execute_local_activity=execute_local_activity,
        info=lambda: types.SimpleNamespace(run_id="this-run"),
        _Runtime=types.SimpleNamespace(current=lambda: types.SimpleNamespace()),
        payload_converter=lambda: converter,
        now=lambda: datetime.now(timezone.utc),
    )
    monkeypatch.setattr(reader_module, "workflow", fake_workflow)
    # No Workflow runs here, so there's no task to cut.
    monkeypatch.setattr(reader_module, "cut_task", lambda _cutoff: None)
    reader = StreamReader(
        _CompletedHandle(),  # type: ignore[arg-type]
        item_type=Token,
        endpoint=ENDPOINT,
        replay_from_store=True,
    )
    assert await reader.next() == [Token("a")]
    assert await reader.next() == [Token("b")]
    assert await reader.next() is None
    assert chained == [False, False, True]


async def test_bodies_reach_the_workflow_decoded_by_the_callers_codec() -> None:
    """A read's answer is one Nexus operation result, encoded by the handler
    Worker's codec with the bodies inside it in the clear. The caller Worker's
    codec opens it before Workflow code sees it."""
    codec = MarkingCodec()
    history = stream_history()
    for event in history.events:
        attributes = event.nexus_operation_completed_event_attributes
        if event.HasField("nexus_operation_completed_event_attributes"):
            [encoded] = await codec.encode([attributes.result])
            attributes.result.CopyFrom(encoded)
    await replay(
        history, dataclasses.replace(DataConverter.default, payload_codec=codec)
    )
    assert seen[0].batches == [[Token("a"), Token("b")], [Token("c")]]


# Live: a caller Workflow reads every record of a stream a handler hands it,
# through the stream service on the operation's own endpoint, with a payload
# codec on every side. Needs a server with Nexus progress and the stream
# notifier; any other server makes the test skip.


@dataclass
class LiveSeen:
    batches: list[list[Token]]
    after_end: list[Token] | None
    result: str


@workflow.defn(name="LiveChatReader")
class LiveChatReader:
    @workflow.run
    async def run(self, endpoint: str) -> LiveSeen:
        client = workflow.create_nexus_client(service=ChatService, endpoint=endpoint)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(
            handle, item_type=Token, endpoint=endpoint, replay_from_store=False
        )
        batches: list[list[Token]] = []
        while (batch := await reader.next()) is not None:
            batches.append(batch)
        return LiveSeen(
            batches=batches, after_end=await reader.next(), result=await handle
        )


@workflow.defn(name="LiveTokenOwner")
class LiveTokenOwner:
    """Publishes its tokens on its own stream, then waits to be told to end."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self, texts: list[str]) -> None:
        writer = workflow_writer(STREAM.topic)
        for text in texts:
            writer.publish(Token(text))
            await workflow.sleep(timedelta(milliseconds=100))
        await writer.finish()
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


async def _skip_without_notifier(client: Client, ref: StreamRef) -> None:
    try:
        await call_workflow_service(
            client,
            "DescribeStreamNotifier",
            DescribeStreamNotifierRequest(
                namespace=client.namespace,
                stream_ref=StreamReference(
                    owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                    workflow_id=ref.workflow_id,
                    topic=ref.topic,
                ),
            ),
            DescribeStreamNotifierResponse,
        )
    except RPCError as err:
        if err.status == RPCStatusCode.UNIMPLEMENTED:
            pytest.skip(f"server has no stream notifier: {err.message}")
        # A server that keys the notifier by run chain refuses the probe's
        # empty run id, which still shows it has the notifier.
        if err.status not in (
            RPCStatusCode.NOT_FOUND,
            RPCStatusCode.INVALID_ARGUMENT,
        ):
            raise


async def test_live_a_workflow_reads_every_record_then_none(
    client: Client, env: WorkflowEnvironment
) -> None:
    codec = MarkingCodec()
    data_converter = dataclasses.replace(DataConverter.default, payload_codec=codec)
    config = client.config()
    config["data_converter"] = data_converter
    client = Client(**config)
    owner_id = f"token-owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=STREAM.topic)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-reader-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    store = MemoryStreams().notify_on_append()
    client = await connect(client, store)
    texts = [f"token-{n}" for n in range(7)]

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            LiveTokenOwner.run, texts, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    streams = TemporalStreamsHandler()
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LiveChatReader, LiveTokenOwner],
        nexus_service_handlers=[ChatServiceHandler(), streams],
    ):
        caller = await client.start_workflow(
            LiveChatReader.run,
            endpoint,
            id=f"stream-reader-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        owner = client.get_workflow_handle(owner_id)

        async def owner_finished_publishing() -> bool:
            try:
                history = await owner.fetch_history()
            except RPCError:
                return False
            timers = sum(
                event.HasField("timer_fired_event_attributes")
                for event in history.events
            )
            return timers >= len(texts)

        for _ in range(200):
            if await owner_finished_publishing():
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("the owner never finished publishing")
        await owner.signal(LiveTokenOwner.done)
        await store.close_stream(client, ref, "7 tokens")
        seen_live = await asyncio.wait_for(caller.result(), 30)

    assert [token for batch in seen_live.batches for token in batch] == [
        Token(text) for text in texts
    ]
    assert seen_live.after_end is None
    assert seen_live.result == "7 tokens"

    # Each read's answer is one Nexus operation result, encoded once by the
    # handler Worker's codec, with the bodies inside it decoded.
    history = await caller.fetch_history()
    read_ids = {
        event.event_id
        for event in history.events
        if event.nexus_operation_scheduled_event_attributes.operation == "read"
    }
    reads = [
        event.nexus_operation_completed_event_attributes.result
        for event in history.events
        if event.nexus_operation_completed_event_attributes.scheduled_event_id
        in read_ids
    ]
    assert all(read.metadata.get("encoding") == codec.encoding for read in reads)
    assert reads, "no read result went through the codec"
    bodies = []
    for encoded in reads:
        [plain] = await codec.decode([encoded])
        answer = converter.from_payload(plain, ReadOutput)
        bodies += [
            WireRecord.FromString(stored.record).body for stored in answer.records
        ]
    data_bodies = [body for body in bodies if body.ByteSize()]
    assert data_bodies
    assert all(
        body.metadata.get("encoding") == b"json/plain" for body in data_bodies
    ), "a body crossed still encoded"

    await Replayer(
        workflows=[LiveChatReader], data_converter=data_converter
    ).replay_workflow(history)


@workflow.defn(name="LiveEndingOwner")
class LiveEndingOwner:
    """Publishes its tokens on its own stream, then completes or fails when
    told to, or waits until it is canceled. It never closes its stream."""

    def __init__(self) -> None:
        self._end = ""

    @workflow.run
    async def run(self, texts: list[str]) -> None:
        writer = workflow_writer(STREAM.topic)
        for text in texts:
            writer.publish(Token(text))
            await workflow.sleep(timedelta(milliseconds=100))
        await writer.finish()
        await workflow.wait_condition(lambda: bool(self._end))
        if self._end == "fail":
            raise ApplicationError(
                "the model gave up", type="OwnerGaveUp", non_retryable=True
            )

    @workflow.signal
    def end(self, how: str) -> None:
        self._end = how


@dataclass
class LiveEnding:
    texts: list[str]
    cause: str
    cause_type: str


@workflow.defn(name="LiveEndingReader")
class LiveEndingReader:
    """Reads every token, then reports how the operation ended."""

    @workflow.run
    async def run(self, endpoint: str) -> LiveEnding:
        client = workflow.create_nexus_client(service=ChatService, endpoint=endpoint)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(
            handle, item_type=Token, endpoint=endpoint, replay_from_store=False
        )
        texts: list[str] = []
        try:
            while (batch := await reader.next()) is not None:
                texts += [token.text for token in batch]
        except NexusOperationError as error:
            cause = error.cause
            return LiveEnding(
                texts,
                type(cause).__name__,
                cause.type
                if isinstance(cause, ApplicationError) and cause.type
                else "",
            )
        return LiveEnding(texts, "", "")


async def _read_an_ending_owner(
    client: Client, env: WorkflowEnvironment, end: str
) -> LiveEnding:
    """Runs a caller against an owner that publishes, then fails or is canceled
    without closing its stream, and answers with what the caller saw."""
    client = await connect(client, MemoryStreams().notify_on_append())
    owner_id = f"ending-owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=STREAM.topic)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-reader-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    texts = [f"token-{n}" for n in range(5)]

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            LiveEndingOwner.run, texts, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[LiveEndingReader, LiveEndingOwner],
        nexus_service_handlers=[ChatServiceHandler(), TemporalStreamsHandler()],
    ):
        caller = await client.start_workflow(
            LiveEndingReader.run,
            endpoint,
            id=f"stream-reader-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        owner = client.get_workflow_handle(owner_id)
        for _ in range(200):
            try:
                history = await owner.fetch_history()
            except RPCError:
                history = None
            if history is not None and (
                sum(
                    event.HasField("timer_fired_event_attributes")
                    for event in history.events
                )
                >= len(texts)
            ):
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("the owner never finished publishing")
        if end == "cancel":
            await owner.cancel()
        else:
            await owner.signal(LiveEndingOwner.end, end)
        ending = await asyncio.wait_for(caller.result(), 30)
    assert ending.texts == texts, "a record was lost before the failure"
    return ending


async def test_live_a_failed_owner_raises_after_every_record_is_read(
    client: Client, env: WorkflowEnvironment
) -> None:
    """The owner failed without closing its stream. The notifier's owner check
    closes it, so the reader hands over every record, then raises the check's
    failure. The check reads no History, so it has no failure of the run's."""
    ending = await _read_an_ending_owner(client, env, "fail")
    assert (ending.cause, ending.cause_type) == (
        "ApplicationError",
        "StreamOwnerFailed",
    )


async def test_live_a_completed_owner_ends_the_reader(
    client: Client, env: WorkflowEnvironment
) -> None:
    """The owner completed without closing its stream, so its callers complete
    with an empty result, and the reader ends."""
    ending = await _read_an_ending_owner(client, env, "complete")
    assert (ending.cause, ending.cause_type) == ("", "")


async def test_live_a_canceled_owner_raises_a_canceled_operation(
    client: Client, env: WorkflowEnvironment
) -> None:
    ending = await _read_an_ending_owner(client, env, "cancel")
    assert ending.cause == "CancelledError"


@workflow.defn(name="LiveLabelReader")
class LiveLabelReader:
    """Reads the stream and keeps each token's label, the text before its
    colon, so large bodies stay out of its result."""

    @workflow.run
    async def run(self, endpoint: str) -> list[str]:
        client = workflow.create_nexus_client(service=ChatService, endpoint=endpoint)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(
            handle, item_type=Token, endpoint=endpoint, replay_from_store=False
        )
        labels: list[str] = []
        while (batch := await reader.next()) is not None:
            labels += [token.text.split(":", 1)[0] for token in batch]
        await handle
        return labels


@workflow.defn(name="LiveIdleOwner")
class LiveIdleOwner:
    """Owns a stream that others write to, until told to end."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


class SlowCodec(MarkingCodec):
    """A codec that does real async work, as one calling a key service does."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        await asyncio.sleep(0.001)
        return await super().encode(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        await asyncio.sleep(0.001)
        return await super().decode(payloads)


# Memory, memory behind a codec that awaits, and Redis: each one's fetch takes
# real time, which a read that does not wait must still complete.
BACKINGS = ["memory", "memory-async-codec", "redis"]


async def connect(client: Client, store: StreamStorePlugin) -> Client:
    """A client of ``client``'s server, with its converter, that carries ``store``."""
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        data_converter=client.data_converter,
        plugins=[store],
    )
