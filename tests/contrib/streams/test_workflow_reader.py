"""Reading a stream in Workflow code, one Local Activity per batch.

A fake read Activity scripts the stream runtime's answers, and a Signal queues
each wake the way the Workflow instance queues a wake from a
``WorkflowTaskAttachments`` job. The live cases need a server
with the stream notifier and the Workflow wake target, and run in the
Jetty lane.
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from collections.abc import Callable
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
    StreamRefusedError,
    StreamUnsupportedError,
    stream_handle,
    topic,
    workflow_reader,
)
from temporalio.contrib.streams._replay_reads import (
    ReadChain,
    batch_payload,
)
from temporalio.contrib.streams._workflow import (
    READ_ACTIVITY,
    _read_summary,
    _ReadInput,
    _result,
    read_activity,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import ActivityEnvironment
from temporalio.worker import Replayer
from tests.contrib.streams._support import (
    connect_with,
    describe_stream_notifier,
    new_workflow_id,
)
from tests.contrib.streams.test_outside_handles import (
    Waits,
    running_owner,
)
from tests.helpers import new_worker

TOKENS = topic("tokens", str)


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


async def test_the_read_activity_reads_and_decodes_through_core(
    client: Client,
):
    # Without the notifier the attach fails, so this drives the read alone.
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, Waits) as worker:
        owner = await running_owner(streams_client, worker.task_queue)
        handle = streams_client.get_workflow_handle(owner)
        stream = stream_handle(streams_client, owner, topic=TOKENS)
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
