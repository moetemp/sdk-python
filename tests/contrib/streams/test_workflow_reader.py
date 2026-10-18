"""Reading a stream in Workflow code, one Local Activity per batch.

A fake read Activity scripts the stream runtime's answers, and a Signal queues
each wake the way the Workflow instance queues a wake from a
``WorkflowTaskAttachments`` job. The live cases need a server
with the stream notifier and the Workflow wake target, and run in the
Jetty lane.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload, WorkflowExecution
from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.stream.v1 import StreamReference, StreamWake
from temporalio.api.workflowservice.v1 import (
    DescribeStreamNotifierRequest,
    ResetWorkflowExecutionRequest,
)
from temporalio.bridge.proto.streams import (
    ReadRangeRequest,
    ReadRecord,
    ReadResponse,
    Supersession,
)
from temporalio.bridge.proto.streams.v1 import StreamRecord as WireRecord
from temporalio.bridge.proto.streams.v1 import StreamRecordKind
from temporalio.client import Client, WorkflowFailureError, WorkflowHistory
from temporalio.common import RawValue
from temporalio.contrib.streams import (
    RecordKind,
    StreamPosition,
    StreamRecordError,
    StreamRef,
    StreamRefusedError,
    StreamStorePlugin,
    StreamUnsupportedError,
    _workflow,
    activity_handle,
    get_stream_handle,
    start_activity_with_stream,
    topic,
    workflow_reader,
)
from temporalio.contrib.streams._plugin import store_for_client
from temporalio.contrib.streams._replay_reads import (
    ReadChain,
    ReplayReadCodec,
    batch_payload,
    positions_payload,
)
from temporalio.contrib.streams._wire import decode_or_mark, to_record
from temporalio.contrib.streams._workflow import (
    READ_ACTIVITY,
    _read_summary,
    _ReadInput,
    _result,
    read_activity,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer
from tests.contrib.streams._support import (
    connect_with,
    describe_stream_notifier,
    new_workflow_id,
    postgres_streams,
)
from tests.contrib.streams.test_outside_handles import (
    Waits,
    owns_a_stream,
    running_owner,
)
from tests.helpers import new_worker

TOKENS = topic("tokens", str)


def new_store(kind: str) -> StreamStorePlugin:
    return postgres_streams() if kind == "postgres" else MemoryStreams()


class FakeReads:
    """The stream runtime's scripted answers for the read Activity, and the inputs it got."""

    def __init__(self, answers: list[Any]) -> None:
        self.answers = answers
        self.inputs: list[_ReadInput] = []

    async def until(self, count: int) -> None:
        async def reached() -> None:
            while len(self.inputs) < count:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(reached(), 10)


_fake: FakeReads | None = None
# An answer that makes the read run until it is canceled.
HANG = asyncio.CancelledError()


class Chained:
    """An answer as the codec turns a positions-only read back, with its chain."""

    def __init__(self, answer: ReadResponse) -> None:
        self.answer = answer


@activity.defn(name=READ_ACTIVITY)
async def fake_read(input: _ReadInput) -> RawValue:
    assert _fake is not None
    _fake.inputs.append(input)
    if input.unsubscribe:
        return _result(ReadResponse(cursor=input.after), "")
    answer = _fake.answers.pop(0)
    if answer is HANG:
        await asyncio.sleep(30)
    if isinstance(answer, BaseException):
        raise answer
    if isinstance(answer, Chained):
        return RawValue(
            batch_payload(
                answer.answer,
                "",
                activity.info().workflow_run_id,
                ReadChain(f"{activity.info().workflow_run_id}/1", input.read_seq),
            )
        )
    return _result(answer, "")


def data(cursor: str, value: str, producer: str = "p", attempt: int = 1) -> ReadRecord:
    [body] = DataConverter.default.payload_converter.to_payloads([value])
    return ReadRecord(
        cursor=cursor,
        stored=WireRecord(
            topic="tokens",
            kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
            producer_id=producer,
            attempt=attempt,
            body=body,
        ),
    )


def unconvertible(cursor: str) -> ReadRecord:
    return ReadRecord(
        cursor=cursor,
        stored=WireRecord(
            topic="tokens",
            kind=StreamRecordKind.STREAM_RECORD_KIND_DATA,
            body=Payload(metadata={"encoding": b"binary/unknown"}, data=b"?"),
        ),
    )


def superseded(cursor: str, producer: str, attempt: int) -> ReadRecord:
    return ReadRecord(
        cursor=cursor,
        superseded=Supersession(
            topic="tokens",
            producer_id=producer,
            previous_attempt=attempt - 1,
            attempt=attempt,
        ),
    )


def empty(cursor: str, wake_after: int) -> ReadResponse:
    # The stream runtime names the chain on every read of an owner that started.
    return ReadResponse(
        cursor=cursor, wake_after=wake_after, chain_first_run_id="chain"
    )


@workflow.defn
class ReadsBatches:
    """Reads the ``tokens`` topic of an owner and says what each batch held."""

    def __init__(self) -> None:
        self.position: StreamPosition | None = None

    @workflow.run
    async def run(
        self, owner: str, stop_after: int, after: StreamPosition | None
    ) -> list[list[str]]:
        batches: list[list[str]] = []
        # The fake read Activity answers with recorded batches.
        reader = workflow_reader(
            owner,
            topic=TOKENS,
            after=after or StreamPosition(""),
            replay_from_store=False,
        )
        async with reader:
            async for batch in reader:
                batches.append([_describe(r) for r in batch.records])
                self.position = batch.position
                if stop_after and len(batches) >= stop_after:
                    break
        return batches

    @workflow.signal
    def wake(self, wake: StreamWake) -> None:
        # Queued as the instance queues each wake it unpacks from the
        # attachments of a scheduled event.
        runtime: Any = workflow._Runtime.current()
        runtime._stream_wakes.append(wake)


def _failure_type(error: BaseException | None) -> str | None:
    # A Local Activity's failure can arrive wrapped in an ActivityError or not.
    if isinstance(error, ActivityError):
        error = error.cause
    return error.type if isinstance(error, ApplicationError) else None


def _describe(record: Any) -> str:
    if record.kind is RecordKind.SUPERSEDED:
        return f"superseded {record.producer_id}@{record.attempt}"
    return f"{record.kind.name.lower()} {record.value}"


async def _no_drive(_handle: Any, _fake: FakeReads) -> None:
    pass


async def _run_reader(
    client: Client,
    answers: list[ReadResponse | BaseException],
    drive: Callable[[Any, FakeReads], Any],
    *,
    stop_after: int = 0,
    after: StreamPosition | None = None,
) -> tuple[list[list[str]], FakeReads, Any]:
    global _fake
    _fake = fake = FakeReads(answers)
    async with new_worker(client, ReadsBatches, activities=[fake_read]) as worker:
        handle = await client.start_workflow(
            ReadsBatches.run,
            args=["owner", stop_after, after],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await drive(handle, fake)
        result = await asyncio.wait_for(handle.result(), 20)
    return result, fake, handle


async def test_a_reader_sleeps_after_an_empty_read_until_a_higher_counter(
    client: Client,
):
    async def drive(handle: Any, fake: FakeReads) -> None:
        await fake.until(2)
        # A wake at the empty read's counter names no new record, so the reader sleeps on.
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", counter=5)
        )
        await asyncio.sleep(1)
        assert len(fake.inputs) == 2
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", counter=6)
        )

    batches, fake, _ = await _run_reader(
        client,
        [
            ReadResponse(records=[data("c1", "a"), data("c2", "b")], cursor="c2"),
            empty("c2", 5),
            ReadResponse(
                records=[superseded("c2", "p", 2), data("c3", "c", attempt=2)],
                cursor="c3",
                done=True,
            ),
        ],
        drive,
    )
    assert batches == [
        ["data a", "data b"],
        ["superseded p@2", "data c"],
    ]
    # Each read names the subscriber, so the stream runtime attaches it and asks for a wake
    # at the empty read. No read attaches first, and each waits in the store.
    assert [(i.subscription_id, i.subscribe) for i in fake.inputs] == [
        ("1", False),
        ("1", False),
        ("1", False),
    ]
    assert [i.after for i in fake.inputs] == ["", "c2", "c2"]
    assert [i.wait_ms == 0 for i in fake.inputs] == [False, False, False]


async def test_a_reader_gets_one_wake_per_idle_stretch(client: Client):
    async def drive(handle: Any, fake: FakeReads) -> None:
        await fake.until(1)
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", counter=2)
        )
        await fake.until(4)
        # The second idle stretch sleeps on its own empty read, with no read
        # that attaches first.
        await asyncio.sleep(1)
        assert len(fake.inputs) == 4
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", counter=4)
        )

    batches, fake, _ = await _run_reader(
        client,
        [
            empty("c0", 1),
            ReadResponse(records=[data("c1", "a")], cursor="c1"),
            ReadResponse(records=[data("c2", "b")], cursor="c2"),
            empty("c2", 3),
            ReadResponse(records=[data("c3", "c")], cursor="c3", done=True),
        ],
        drive,
    )
    assert batches == [["data a"], ["data b"], ["data c"]]
    assert [(i.subscription_id, i.subscribe) for i in fake.inputs] == [("1", False)] * 5
    assert [i.wait_ms == 0 for i in fake.inputs] == [False] * 5
    assert {_read_summary(i).split(" tokens")[0] for i in fake.inputs} == {
        "stream read"
    }


async def test_a_recheck_reads_again_whatever_the_counter(client: Client):
    async def drive(handle: Any, fake: FakeReads) -> None:
        await fake.until(1)
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", recheck=True)
        )
        await fake.until(2)
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", recheck=True)
        )

    batches, fake, _ = await _run_reader(
        client,
        [
            empty("", 0),
            empty("", 0),
            ReadResponse(records=[data("c1", "lost")], cursor="c1", done=True),
        ],
        drive,
    )
    assert batches == [["data lost"]]
    assert len(fake.inputs) == 3


async def test_a_close_is_read_to_the_end(client: Client):
    async def drive(handle: Any, fake: FakeReads) -> None:
        await fake.until(1)
        await handle.signal(
            ReadsBatches.wake,
            StreamWake(subscription_id="1", counter=9, closed=True),
        )

    batches, fake, _ = await _run_reader(
        client,
        [
            empty("c0", 3),
            ReadResponse(records=[data("c1", "x")], cursor="c1"),
            ReadResponse(records=[data("c2", "y")], cursor="c2", done=True),
        ],
        drive,
    )
    assert batches == [["data x"], ["data y"]]
    # A closed stream needs no wake, so the reads after the close name no
    # subscriber.
    assert [i.subscription_id for i in fake.inputs] == ["1", "", ""]


async def test_a_close_with_a_failure_raises_it_after_the_last_batch(client: Client):
    failure = Failure(
        message="the agent gave up",
        application_failure_info=ApplicationFailureInfo(type="AgentGaveUp"),
    )

    async def drive(handle: Any, fake: FakeReads) -> None:
        await fake.until(1)
        await handle.signal(
            ReadsBatches.wake,
            StreamWake(
                subscription_id="1", counter=4, closed=True, close_failure=failure
            ),
        )

    with pytest.raises(WorkflowFailureError) as raised:
        await _run_reader(
            client,
            [
                empty("c0", 3),
                ReadResponse(records=[data("c1", "x")], cursor="c1", done=True),
            ],
            drive,
        )
    cause = raised.value.cause
    assert isinstance(cause, ApplicationError)
    assert cause.type == "AgentGaveUp"
    assert cause.message == "the agent gave up"


async def test_the_end_of_a_read_carries_the_close_failure(client: Client):
    # The store keeps the close's outcome and answers it with done, so a busy
    # reader that never slept still raises the failure after the last batch.
    failure = Failure(
        message="the agent gave up",
        application_failure_info=ApplicationFailureInfo(type="AgentGaveUp"),
    )
    with pytest.raises(WorkflowFailureError) as raised:
        await _run_reader(
            client,
            [
                ReadResponse(
                    records=[data("c1", "x")],
                    cursor="c1",
                    done=True,
                    close_failure=failure,
                    closed_by="@owner-run",
                )
            ],
            _no_drive,
        )
    assert _failure_type(raised.value.cause) == "AgentGaveUp"


async def test_a_resubscribe_attaches_again_under_a_new_id(client: Client):
    async def drive(handle: Any, fake: FakeReads) -> None:
        await fake.until(1)
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", resubscribe=True)
        )
        await fake.until(2)
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", counter=2)
        )

    batches, fake, _ = await _run_reader(
        client,
        [
            empty("c0", 1),
            empty("c0", 1),
            ReadResponse(records=[data("c1", "z")], cursor="c1", done=True),
        ],
        drive,
    )
    assert batches == [["data z"]]
    # After a resubscribe each read names the new generation, so the stream runtime
    # attaches again at the next empty read.
    assert [(i.subscription_id, i.generation) for i in fake.inputs] == [
        ("1", 0),
        ("1", 1),
        ("1", 1),
    ]


async def test_a_reset_run_attaches_itself_before_it_sleeps(client: Client):
    global _fake
    _fake = fake = FakeReads(
        [
            empty("c0", 5),
            ReadResponse(records=[data("c1", "x")], cursor="c1"),
        ]
    )
    async with new_worker(client, ReadsBatches, activities=[fake_read]) as worker:
        handle = await client.start_workflow(
            ReadsBatches.run,
            args=["owner", 1, None],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await fake.until(1)
        # A wake below the empty read's counter wakes nothing, but ends one more task
        # that the reset can go back to, past the empty read's marker.
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", counter=1)
        )

        async def task_after_signal() -> int:
            while True:
                events = list((await handle.fetch_history()).events)
                signaled = [
                    e.event_id
                    for e in events
                    if e.HasField("workflow_execution_signaled_event_attributes")
                ]
                completed = [
                    e.event_id
                    for e in events
                    if e.HasField("workflow_task_completed_event_attributes")
                ]
                if signaled and completed and completed[-1] > signaled[0]:
                    return completed[-1]
                await asyncio.sleep(0.05)

        event_id = await asyncio.wait_for(task_after_signal(), 10)
        reset = await client.workflow_service.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=client.namespace,
                workflow_execution=WorkflowExecution(workflow_id=handle.id),
                reason="test",
                workflow_task_finish_event_id=event_id,
                request_id=str(uuid.uuid4()),
            )
        )
        new_run = client.get_workflow_handle(handle.id, run_id=reset.run_id)
        batches = await asyncio.wait_for(new_run.result(), 20)
    assert batches == [["data x"]]
    # The reset run replayed the empty read and read once more, since the
    # notifier wakes only the run that attached. Then it detached.
    assert [(i.subscription_id, i.unsubscribe) for i in fake.inputs] == [
        ("1", False),
        ("1", False),
        ("1", True),
    ]


async def test_a_reader_stopped_early_detaches(client: Client):
    async def drive(handle: Any, fake: FakeReads) -> None:
        await fake.until(1)
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", counter=2)
        )

    batches, fake, _ = await _run_reader(
        client,
        [
            empty("c0", 1),
            ReadResponse(records=[data("c1", "a")], cursor="c1", state=b"seen"),
        ],
        drive,
        stop_after=1,
    )
    assert batches == [["data a"]]
    last = fake.inputs[-1]
    assert last.unsubscribe and last.subscription_id == "1"


async def test_a_reader_that_never_went_idle_has_nothing_to_detach(client: Client):
    batches, fake, _ = await _run_reader(
        client,
        [ReadResponse(records=[data("c1", "a")], cursor="c1")],
        _no_drive,
        stop_after=1,
    )
    assert batches == [["data a"]]
    # The stream runtime attaches only at an empty read, so this reader never attached.
    assert [(i.subscription_id, i.unsubscribe) for i in fake.inputs] == [("1", False)]


async def test_a_position_resumes_the_next_run(client: Client):
    batches, fake, _ = await _run_reader(
        client,
        [ReadResponse(records=[data("c9", "next")], cursor="c9", done=True)],
        _no_drive,
        after=StreamPosition("c8", "c2Vlbg=="),
    )
    assert batches == [["data next"]]
    assert (fake.inputs[0].after, fake.inputs[0].state) == ("c8", "c2Vlbg==")


async def test_a_read_that_fails_for_good_fails_with_the_stream_error(client: Client):
    with pytest.raises(WorkflowFailureError) as raised:
        await _run_reader(
            client,
            [
                ApplicationError(
                    "no owner", type="StreamNotFoundError", non_retryable=True
                )
            ],
            _no_drive,
        )
    assert _failure_type(raised.value.cause) == "StreamNotFoundError"


async def test_a_reader_replays_from_its_markers_and_wakes(client: Client):
    async def drive(handle: Any, fake: FakeReads) -> None:
        await fake.until(2)
        await handle.signal(
            ReadsBatches.wake, StreamWake(subscription_id="1", counter=8)
        )

    _, _, handle = await _run_reader(
        client,
        [
            ReadResponse(records=[data("c1", "a")], cursor="c1"),
            empty("c1", 7),
            ReadResponse(records=[data("c2", "b")], cursor="c2", done=True),
        ],
        drive,
    )
    # No Activity runs on replay, so the batches and the wake order come from History.
    history = await handle.fetch_history()
    await Replayer(workflows=[ReadsBatches]).replay_workflow(history)
    # The same run, with the wake on a Workflow Task scheduled event as the
    # server records it. Core then hands it over in a WorkflowTaskAttachments
    # job, which wakes the reader with no Signal to run.
    await Replayer(workflows=[ReadsBatches]).replay_workflow(
        _signal_as_stream_wake(history)
    )


def _signal_as_stream_wake(history: WorkflowHistory) -> WorkflowHistory:
    events = list(history.events)
    index = next(
        i
        for i, e in enumerate(events)
        if e.HasField("workflow_execution_signaled_event_attributes")
    )
    signaled = events.pop(index)
    [wake] = DataConverter.default.payload_converter.from_payloads(
        signaled.workflow_execution_signaled_event_attributes.input.payloads,
        [StreamWake],
    )
    removed = signaled.event_id
    for event in events[index:]:
        event.event_id -= 1
        attributes = getattr(event, event.WhichOneof("attributes") or "")
        for descriptor, value in attributes.ListFields():
            if descriptor.name.endswith("_event_id") and value > removed:
                setattr(attributes, descriptor.name, value - 1)
    scheduled = next(
        e
        for e in events[index:]
        if e.HasField("workflow_task_scheduled_event_attributes")
    )
    scheduled.workflow_task_scheduled_event_attributes.attachments.add().Pack(wake)
    return WorkflowHistory(history.workflow_id, events)


@workflow.defn
class TwoReaders:
    @workflow.run
    async def run(self) -> list[str]:
        first = workflow_reader("a", topic=TOKENS)
        second = workflow_reader("b", topic=TOKENS)
        return [first.subscription_id, second.subscription_id]


async def test_each_reader_of_a_run_has_its_own_subscription(client: Client):
    async with new_worker(client, TwoReaders) as worker:
        ids = await client.execute_workflow(
            TwoReaders.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
    assert ids == ["1", "2"]


@pytest.mark.parametrize("kind", ["memory", "postgres"])
async def test_the_read_activity_reads_and_decodes_through_core(
    client: Client, kind: str
):
    # Without the notifier the attach fails, so this drives the read alone.
    streams_client = await connect_with(client, new_store(kind))
    async with new_worker(streams_client, Waits) as worker:
        owner = await running_owner(streams_client, worker.task_queue)
        handle = streams_client.get_workflow_handle(owner)
        stream = get_stream_handle(streams_client, owner, topic=TOKENS)
        await stream.producer(producer_id="w", attempt=1).append("one", "two")
        env = ActivityEnvironment(client=streams_client)
        env.info = dataclasses.replace(env.info, is_local=True)
        raw = await env.run(
            read_activity,
            _ReadInput(
                workflow_id=owner,
                chain_run_id="",
                topic="tokens",
                after="",
                state="",
                wait_ms=100,
                max_records=10,
            ),
        )
        response = ReadResponse.FromString(raw.payload.data)
        values = [
            DataConverter.default.payload_converter.from_payloads([r.stored.body])[0]
            for r in response.records
        ]
        assert values == ["one", "two"]
        assert raw.payload.metadata["temporal.io/stream-read-run-id"] == b""
        await handle.signal(Waits.finish)


@workflow.defn
class ReadsOnAServerWithoutANotifier:
    @workflow.run
    async def run(self, owner: str) -> str:
        try:
            async for _ in workflow_reader(owner, topic=TOKENS):
                pass
        except (ActivityError, ApplicationError) as error:
            return f"failed: {_failure_type(error)}"
        return "read"


async def test_a_workflow_reader_needs_the_stream_notifier(client: Client):
    try:
        await describe_stream_notifier(
            client,
            DescribeStreamNotifierRequest(
                namespace=client.namespace,
                stream_ref=StreamReference(
                    owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                    workflow_id="probe",
                    run_id="probe",
                    topic="tokens",
                ),
            ),
        )
    except StreamUnsupportedError:
        pass
    except StreamRefusedError:
        pytest.skip("the server has the stream notifier")
    else:
        pytest.skip("the server has the stream notifier")
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(
        streams_client, Waits, ReadsOnAServerWithoutANotifier
    ) as worker:
        owner = await running_owner(streams_client, worker.task_queue)
        said = await streams_client.execute_workflow(
            ReadsOnAServerWithoutANotifier.run,
            owner,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await streams_client.get_workflow_handle(owner).signal(Waits.finish)
    assert said == "failed: StreamUnsupportedError"


@workflow.defn
class ReadsOnce:
    @workflow.run
    async def run(self, record_batches: bool) -> str:
        reader = (
            workflow_reader("owner", topic=TOKENS, replay_from_store=False)
            if record_batches
            else workflow_reader("owner", topic=TOKENS)
        )
        try:
            async for _ in reader:
                pass
        except StreamUnsupportedError as error:
            return str(error)
        return "read"


@pytest.mark.parametrize("record_batches", [False, True])
async def test_a_reader_replays_from_the_store_by_default(
    client: Client, record_batches: bool
):
    global _fake
    _fake = FakeReads([ReadResponse(records=[data("c1", "a")], cursor="c1", done=True)])
    async with new_worker(client, ReadsOnce, activities=[fake_read]) as worker:
        await client.execute_workflow(
            ReadsOnce.run,
            record_batches,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert _fake.inputs[0].positions_only is not record_batches


async def test_a_reader_at_the_end_keeps_its_hold_on_the_records(client: Client):
    global _fake
    _fake = FakeReads(
        [
            ReadResponse(
                records=[data("c1", "a")],
                cursor="c1",
                done=True,
                chain_first_run_id="chain",
            )
        ]
    )
    async with new_worker(client, ReadsOnce, activities=[fake_read]) as worker:
        await client.execute_workflow(
            ReadsOnce.run, False, id=new_workflow_id(), task_queue=worker.task_queue
        )
    # The run can still replay the read, so no Local Activity ends the hold.
    assert len(_fake.inputs) == 1


@activity.defn(name=READ_ACTIVITY)
async def positions_read(_input: _ReadInput) -> RawValue:
    # What the real read Activity answers in the default mode.
    return RawValue(
        positions_payload(
            ReadRangeRequest(after="", to="c1", stored_records=1, records_digest=b"d"),
            ReadResponse(cursor="c1", done=True),
            "",
        )
    )


async def test_a_worker_without_the_store_plugin_names_the_opt_out(client: Client):
    # The client carries no store plugin, so no codec turns the range back.
    async with new_worker(client, ReadsOnce, activities=[positions_read]) as worker:
        said = await client.execute_workflow(
            ReadsOnce.run, False, id=new_workflow_id(), task_queue=worker.task_queue
        )
    assert "replay_from_store=False" in said


async def test_a_replay_with_no_connected_store_names_the_opt_out():
    codec = ReplayReadCodec(MemoryStreams(), None, DataConverter.default)
    payload = positions_payload(
        ReadRangeRequest(after="", to="c1", stored_records=1, records_digest=b"d"),
        ReadResponse(cursor="c1"),
        "",
    )
    with pytest.raises(StreamUnsupportedError, match="replay_from_store=False"):
        await codec.decode([payload])


@workflow.defn
class ReadsAStream:
    @workflow.run
    async def run(self, ref: StreamRef, record_batches: bool) -> list[str]:
        seen: list[str] = []
        async with workflow_reader(ref, replay_from_store=not record_batches) as reader:
            async for batch in reader:
                seen.extend(_describe(r) for r in batch.records)
        return seen


async def test_a_reader_names_an_activity_owner_in_each_read(client: Client):
    global _fake
    # The Activity closed its stream, so the reader ends at once.
    _fake = FakeReads(
        [
            ReadResponse(
                records=[data("c1", "a")], cursor="c1", done=True, closed_by="act@run"
            )
        ]
    )
    ref = StreamRef.for_activity("act", run_id="run", topic=TOKENS)
    async with new_worker(client, ReadsAStream, activities=[fake_read]) as worker:
        seen = await client.execute_workflow(
            ReadsAStream.run,
            args=[ref, True],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert seen == ["data a"]
    [input] = _fake.inputs
    assert (input.owner_kind, input.activity_id, input.workflow_id) == (
        "activity",
        "act",
        "",
    )
    assert input.chain_run_id == "run"


def test_a_read_input_from_before_activity_owners_names_a_workflow():
    # A marker's input is plain data, and an older one has no owner fields.
    converter = DataConverter.default.payload_converter
    payload = converter.to_payloads(
        [
            {
                "workflow_id": "owner",
                "chain_run_id": "",
                "topic": "tokens",
                "after": "",
                "state": "",
                "wait_ms": 100,
                "max_records": 10,
            }
        ]
    )[0]
    [input] = converter.from_payloads([payload], [_ReadInput])
    assert (input.owner_kind, input.activity_id) == ("workflow", "")


@pytest.mark.parametrize("kind", ["memory", "postgres"])
@pytest.mark.parametrize("record_batches", [True, False])
async def test_a_workflow_reads_the_stream_of_a_standalone_activity(
    client: Client,
    record_batches: bool,
    kind: str,
):
    # The Activity closes its stream before the reader starts, so the reader
    # reads to the close and never sleeps on the notifier.
    streams = await connect_with(client, new_store(kind))
    activity_id = new_workflow_id()
    async with new_worker(streams, ReadsAStream, activities=[owns_a_stream]) as worker:
        owner = await streams.start_activity(
            owns_a_stream,
            ["a", "b"],
            id=activity_id,
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=10),
        )
        run_id = await owner.result()
        ref = StreamRef.for_activity(activity_id, run_id=run_id, topic=TOKENS)
        handle = await streams.start_workflow(
            ReadsAStream.run,
            args=[ref, record_batches],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        assert await handle.result() == ["data a", "data b"]
    history = await handle.fetch_history()
    if record_batches:
        # The markers hold the batches, so a replay needs no store and no server.
        await Replayer(
            workflows=[ReadsAStream], plugins=[MemoryStreams()]
        ).replay_workflow(history)
    else:
        store = store_for_client(streams)
        store._batches.clear()
        await Replayer(workflows=[ReadsAStream], plugins=[store]).replay_workflow(
            history
        )


@pytest.mark.parametrize("kind", ["memory", "postgres"])
async def test_the_read_activity_reads_a_standalone_activitys_stream(
    client: Client, kind: str
):
    streams = await connect_with(client, new_store(kind))
    activity_id = new_workflow_id()
    async with new_worker(streams, activities=[owns_a_stream]) as worker:
        owner = await streams.start_activity(
            owns_a_stream,
            ["one"],
            id=activity_id,
            task_queue=worker.task_queue,
            start_to_close_timeout=timedelta(seconds=10),
        )
        run_id = await owner.result()
    env = ActivityEnvironment(client=streams)
    env.info = dataclasses.replace(env.info, is_local=True)
    raw = await env.run(
        read_activity,
        _ReadInput(
            workflow_id="",
            owner_kind="activity",
            activity_id=activity_id,
            chain_run_id=run_id,
            topic="tokens",
            after="",
            state="",
            wait_ms=100,
            max_records=10,
        ),
    )
    response = ReadResponse.FromString(raw.payload.data)
    assert [
        DataConverter.default.payload_converter.from_payloads([r.stored.body])[0]
        for r in response.records
        if r.HasField("stored")
    ] == ["one"]
    # One run is the Activity's chain, so the read names it with no describe.
    assert raw.payload.metadata["temporal.io/stream-read-run-id"] == run_id.encode()


@workflow.defn
class ReadsAnActivityRun:
    @workflow.run
    async def run(self) -> list[str]:
        seen: list[str] = []
        ref = StreamRef.for_activity("act", run_id="run", topic=TOKENS)
        async with workflow_reader(ref, replay_from_store=False) as reader:
            async for batch in reader:
                seen.extend(_describe(r) for r in batch.records)
        return seen

    @workflow.signal
    def wake(self, wake: StreamWake) -> None:
        runtime: Any = workflow._Runtime.current()
        runtime._stream_wakes.append(wake)


async def test_a_reader_of_an_ended_activity_run_raises_the_notifiers_failure(
    client: Client,
):
    # The stream runtime ends the read when the run ends, with no outcome. Only the notifier
    # knows the run's failure, so the reader attaches and waits for its close.
    global _fake
    _fake = FakeReads(
        [
            ReadResponse(records=[data("c1", "x")], cursor="c1", done=True),
            ReadResponse(cursor="c1", done=True),
        ]
    )
    failure = Failure(
        message="boom", application_failure_info=ApplicationFailureInfo(type="Boom")
    )
    async with new_worker(client, ReadsAnActivityRun, activities=[fake_read]) as worker:
        handle = await client.start_workflow(
            ReadsAnActivityRun.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
        await _fake.until(2)
        await handle.signal(
            ReadsAnActivityRun.wake,
            StreamWake(subscription_id="1", closed=True, close_failure=failure),
        )
        with pytest.raises(WorkflowFailureError) as raised:
            await asyncio.wait_for(handle.result(), 20)
    cause = raised.value.cause
    assert isinstance(cause, ApplicationError) and cause.type == "Boom"
    # The second read attaches first, since the read is done and only the
    # notifier tells how the run ended.
    assert [(i.subscription_id, i.subscribe) for i in _fake.inputs] == [
        ("1", False),
        ("1", True),
    ]


async def test_a_reader_names_a_workflow_activity_owner_in_each_read(client: Client):
    global _fake
    _fake = FakeReads(
        [
            ReadResponse(
                records=[data("c1", "a")], cursor="c1", done=True, closed_by="act@run"
            )
        ]
    )
    ref = StreamRef.for_workflow_activity(
        "agent", run_id="run", activity_id="act", topic=TOKENS
    )
    async with new_worker(client, ReadsAStream, activities=[fake_read]) as worker:
        seen = await client.execute_workflow(
            ReadsAStream.run,
            args=[ref, True],
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert seen == ["data a"]
    [input] = _fake.inputs
    assert (input.owner_kind, input.activity_id, input.workflow_id) == (
        "workflow_activity",
        "act",
        "agent",
    )
    assert input.chain_run_id == "run"


@activity.defn
async def streams_a_tool_call(tokens: list[str]) -> str:
    await activity_handle().producer(topic=TOKENS).append(*tokens)
    return "ok"


@workflow.defn
class RunsAToolAndReadsIt:
    @workflow.run
    async def run(self, record_batches: bool) -> list[str]:
        handle, ref = start_activity_with_stream(
            streams_a_tool_call,
            ["a", "b"],
            start_to_close_timeout=timedelta(seconds=10),
            topic=TOKENS,
        )
        await handle
        seen: list[str] = []
        async with workflow_reader(ref, replay_from_store=not record_batches) as reader:
            async for batch in reader:
                seen.extend(_describe(r) for r in batch.records)
        return seen


@pytest.mark.parametrize("kind", ["memory", "postgres"])
@pytest.mark.parametrize("record_batches", [True, False])
async def test_a_workflow_reads_the_stream_of_its_own_activity(
    client: Client,
    record_batches: bool,
    kind: str,
):
    # The Worker closes the tool's stream when the run sees the tool end, so
    # the reader reads to that close and never sleeps on the notifier.
    streams = await connect_with(client, new_store(kind))
    async with new_worker(
        streams, RunsAToolAndReadsIt, activities=[streams_a_tool_call]
    ) as worker:
        handle = await streams.start_workflow(
            RunsAToolAndReadsIt.run,
            record_batches,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        assert await asyncio.wait_for(handle.result(), 30) == ["data a", "data b"]
    history = await handle.fetch_history()
    if record_batches:
        await Replayer(
            workflows=[RunsAToolAndReadsIt], plugins=[MemoryStreams()]
        ).replay_workflow(history)
    else:
        store = store_for_client(streams)
        store._batches.clear()
        await Replayer(
            workflows=[RunsAToolAndReadsIt], plugins=[store]
        ).replay_workflow(history)


@workflow.defn
class ReadsAToolStream:
    @workflow.run
    async def run(self) -> list[str]:
        seen: list[str] = []
        ref = StreamRef.for_workflow_activity(
            "agent", run_id="run", activity_id="tool-1", topic=TOKENS
        )
        async with workflow_reader(ref, replay_from_store=False) as reader:
            async for batch in reader:
                seen.extend(_describe(r) for r in batch.records)
        return seen

    @workflow.signal
    def wake(self, wake: StreamWake) -> None:
        runtime: Any = workflow._Runtime.current()
        runtime._stream_wakes.append(wake)


@workflow.defn
class SaysWhatAnActivityRunHad:
    @workflow.run
    async def run(self) -> list[str]:
        seen: list[str] = []
        ref = StreamRef.for_activity("act", run_id="run", topic=TOKENS)
        try:
            async with workflow_reader(ref, replay_from_store=False) as reader:
                async for batch in reader:
                    seen.extend(_describe(r) for r in batch.records)
        except ApplicationError as error:
            seen.append(f"failed {error.type}")
        return seen

    @workflow.signal
    def wake(self, wake: StreamWake) -> None:
        runtime: Any = workflow._Runtime.current()
        runtime._stream_wakes.append(wake)


async def test_a_reader_of_a_tool_stream_ends_at_the_notifiers_backstop_close(
    client: Client,
):
    # The store never closes a tool stream whose agent missed the close, and
    # The stream runtime ends the read only after its grace. The notifier's close is the end.
    global _fake
    _fake = FakeReads(
        [
            ReadResponse(records=[data("c1", "x")], cursor="c1"),
            empty("c1", 1),
            empty("c1", 1),
        ]
    )
    failure = Failure(
        message="the owner Activity ended without closing the stream",
        application_failure_info=ApplicationFailureInfo(),
    )
    async with new_worker(client, ReadsAToolStream, activities=[fake_read]) as worker:
        handle = await client.start_workflow(
            ReadsAToolStream.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
        await _fake.until(2)
        await handle.signal(
            ReadsAToolStream.wake,
            StreamWake(subscription_id="1", closed=True, close_failure=failure),
        )
        with pytest.raises(WorkflowFailureError) as raised:
            await asyncio.wait_for(handle.result(), 20)
    assert "without closing the stream" in str(raised.value.cause)
    assert len(_fake.inputs) == 3


async def test_the_last_records_of_an_ended_activity_run_are_delivered(
    client: Client,
):
    global _fake
    _fake = FakeReads(
        [
            ReadResponse(records=[data("c1", "x")], cursor="c1", done=True),
            ReadResponse(cursor="c1", done=True),
        ]
    )
    failure = Failure(
        message="boom", application_failure_info=ApplicationFailureInfo(type="Boom")
    )
    async with new_worker(
        client, SaysWhatAnActivityRunHad, activities=[fake_read]
    ) as worker:
        handle = await client.start_workflow(
            SaysWhatAnActivityRunHad.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await _fake.until(2)
        await handle.signal(
            SaysWhatAnActivityRunHad.wake,
            StreamWake(subscription_id="1", closed=True, close_failure=failure),
        )
        seen = await asyncio.wait_for(handle.result(), 20)
    assert seen == ["data x", "failed Boom"]


@workflow.defn
class ReadsPastBadRecords:
    @workflow.run
    async def run(self) -> list[str]:
        seen: list[str] = []
        async with workflow_reader("owner", topic=TOKENS) as reader:
            while True:
                try:
                    batch = await anext(reader)
                except StreamRecordError as error:
                    seen.append(f"error {error.cursor.token}")
                    continue
                except StopAsyncIteration:
                    return seen
                seen.extend(_describe(r) for r in batch.records)
                seen.append(f"at {batch.position.cursor}")


async def test_a_record_that_does_not_convert_is_raised_once_then_passed(
    client: Client,
):
    global _fake
    _fake = FakeReads(
        [
            ReadResponse(
                records=[
                    data("c1", "one"),
                    unconvertible("c2"),
                    unconvertible("c3"),
                    data("c4", "four"),
                ],
                cursor="c4",
            ),
            ReadResponse(records=[unconvertible("c5")], cursor="c5", done=True),
        ]
    )
    async with new_worker(client, ReadsPastBadRecords, activities=[fake_read]) as w:
        handle = await client.start_workflow(
            ReadsPastBadRecords.run, id=new_workflow_id(), task_queue=w.task_queue
        )
        seen = await asyncio.wait_for(handle.result(), 20)
    assert seen == [
        "data one",
        "at c1",
        "error c2",
        "error c3",
        "data four",
        "at c4",
        "error c5",
    ]
    # Two reads, so the bad records cost no read of their own.
    assert len(_fake.inputs) == 2
    await Replayer(workflows=[ReadsPastBadRecords]).replay_workflow(
        await handle.fetch_history()
    )


class FailingCodec(PayloadCodec):
    async def encode(self, payloads: Any) -> list[Payload]:
        return list(payloads)

    async def decode(self, payloads: Any) -> list[Payload]:
        raise RuntimeError("no key")


async def test_a_body_the_codec_cannot_decode_is_one_bad_record():
    read = data("c1", "one")
    await decode_or_mark(DataConverter(payload_codec=FailingCodec()), read)
    with pytest.raises(StreamRecordError, match="no key") as raised:
        to_record(DataConverter.default.payload_converter, read, str)
    assert raised.value.cursor.token == "c1"


@workflow.defn
class CancelsTheAttachingRead:
    @workflow.run
    async def run(self) -> None:
        reader = workflow_reader("owner", topic=TOKENS)
        try:
            await asyncio.wait_for(anext(reader), 1)
        except (asyncio.TimeoutError, ActivityError):
            # The canceled Local Activity comes back as its failure.
            pass
        await reader.close()


async def test_a_close_after_a_canceled_attach_detaches(client: Client):
    global _fake
    # Each read of an open stream names the reader, so the stream runtime may attach it.
    _fake = FakeReads([HANG])
    async with new_worker(
        client, CancelsTheAttachingRead, activities=[fake_read]
    ) as worker:
        await client.execute_workflow(
            CancelsTheAttachingRead.run,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert _fake.inputs[0].subscription_id == "1"
    last = _fake.inputs[-1]
    assert last.unsubscribe and last.subscription_id == "1"


async def test_an_attach_without_notifications_warns_once(
    client: Client, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(_workflow, "_warned_no_notify", False)
    streams_client = await connect_with(client, MemoryStreams().notify_on_append(False))
    env = ActivityEnvironment(client=streams_client)
    env.info = dataclasses.replace(env.info, is_local=True)
    input = _ReadInput(
        workflow_id="owner",
        chain_run_id="",
        topic="tokens",
        after="",
        state="",
        wait_ms=0,
        max_records=10,
        subscription_id="1",
    )
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            # The read itself may fail here, with no owner and no notifier.
            with contextlib.suppress(Exception):
                await env.run(read_activity, input)
    warned = [r for r in caplog.records if "notify_on_append" in r.getMessage()]
    assert len(warned) == 1


@workflow.defn
class ReadsPastAFailedRead:
    @workflow.run
    async def run(self) -> list[bool]:
        reader = workflow_reader("owner", topic=TOKENS)
        await anext(reader)
        try:
            await anext(reader)
        except (ActivityError, ApplicationError):
            pass
        batch = await anext(reader)
        return [
            reader.continue_as_new_suggested(max_bytes=10),
            reader.continue_as_new_suggested(),
            bool(batch.records),
        ]


async def test_a_failed_read_leaves_no_gap_in_the_read_chain(client: Client):
    global _fake
    _fake = FakeReads(
        [
            Chained(ReadResponse(records=[data("c1", "a")], cursor="c1", state=b"s1")),
            ApplicationError("gone", type="StreamNotFoundError", non_retryable=True),
            Chained(ReadResponse(records=[data("c2", "b")], cursor="c2", done=True)),
        ]
    )
    async with new_worker(client, ReadsPastAFailedRead, activities=[fake_read]) as w:
        handle = await client.start_workflow(
            ReadsPastAFailedRead.run, id=new_workflow_id(), task_queue=w.task_queue
        )
        suggested = await asyncio.wait_for(handle.result(), 20)
    reads = [(i.read_seq, i.chained, i.state) for i in _fake.inputs]
    # The failed read counts for nothing, so the next one takes its seq and
    # still chains from the first read's answer.
    assert reads == [(1, False, ""), (2, True, "czE="), (2, True, "czE=")]
    assert suggested == [True, False, True]
    await Replayer(workflows=[ReadsPastAFailedRead]).replay_workflow(
        await handle.fetch_history()
    )


async def test_a_read_before_the_owner_starts_reads_again_on_a_timer(client: Client):
    # The stream runtime answers an idle read inside the owner start grace with no chain
    # and attaches nothing, so no wake would come.
    batches, fake, _ = await _run_reader(
        client,
        [
            ReadResponse(cursor=""),
            ReadResponse(records=[data("c1", "a")], cursor="c1", done=True),
        ],
        _no_drive,
    )
    assert batches == [["data a"]]
    assert len(fake.inputs) == 2


async def test_a_read_stops_at_its_byte_budget_past_the_first_record(client: Client):
    # Two large records in one recorded answer would pass the server's blob
    # limit, so the read hands over the first one and the next read the other.
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, Waits) as worker:
        owner = await running_owner(streams_client, worker.task_queue)
        stream = get_stream_handle(streams_client, owner, topic=TOKENS)
        big = "x" * (1200 << 10)
        await stream.producer(producer_id="w", attempt=1).append(big, big)
        env = ActivityEnvironment(client=streams_client)
        env.info = dataclasses.replace(env.info, is_local=True)
        read = _ReadInput(
            workflow_id=owner,
            chain_run_id="",
            topic="tokens",
            after="",
            state="",
            wait_ms=0,
            max_records=10,
            max_bytes=1 << 20,
        )
        first = ReadResponse.FromString(
            (await env.run(read_activity, read)).payload.data
        )
        assert len(first.records) == 1
        read.after = first.cursor
        second = ReadResponse.FromString(
            (await env.run(read_activity, read)).payload.data
        )
        assert len(second.records) == 1
        await streams_client.get_workflow_handle(owner).signal(Waits.finish)


@workflow.defn
class RegistersReaders:
    @workflow.run
    async def run(self, owner: str) -> dict[str, str]:
        workflow_reader(owner)
        workflow_reader(StreamRef.for_activity("tool"))
        runtime: Any = workflow._Runtime.current()
        contexts: dict[str, Any] = runtime._stream_wake_contexts
        return {
            sub: f"{type(c).__name__}/{getattr(c, 'activity_id', None) or c.workflow_id}"
            for sub, c in contexts.items()
        }


async def test_each_reader_registers_its_owner_context(client: Client):
    async with new_worker(client, RegistersReaders) as worker:
        result = await client.execute_workflow(
            RegistersReaders.run,
            "owner-wf",
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert result == {
        "1": "WorkflowSerializationContext/owner-wf",
        "2": "ActivitySerializationContext/tool",
    }
