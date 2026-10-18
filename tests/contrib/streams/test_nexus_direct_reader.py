"""A Workflow's :class:`StreamReader` with ``replay_from_store=True``.

Each read runs in a Local Activity, whose marker holds the range that crossed
and Core's digest, not the records. These tests replay histories built by
hand with such markers, so the batches come back through the codec step of
:class:`StreamDirectReads` and a fake transport stands in for the endpoint.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import MarkerRecordedEventAttributes
from temporalio.client import WorkflowHistory
from temporalio.contrib.streams import (
    StreamRecordError,
    StreamReplayMismatchError,
    StreamReplayRangeLostError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._record import RecordKind
from temporalio.contrib.streams._replay_reads import ReadChain
from temporalio.contrib.streams.nexus import (
    ReadInput,
    ReadOutput,
    ReadStaleRecord,
    ReadSupersession,
    RecordWire,
    StreamDirectReads,
    StreamReader,
)
from temporalio.contrib.streams.nexus import _reader as reader_module
from temporalio.contrib.streams.nexus._direct import (
    DIRECT_READ_ACTIVITY,
    positions_payload,
)
from temporalio.contrib.streams.nexus._generated import (
    ReadOutputReplay,
    ReadRangeInput,
    RecordWireSuperseded,
)
from temporalio.exceptions import NexusOperationError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner
from tests.contrib.streams.test_nexus_reader import (
    ENDPOINT,
    STREAM,
    ChatService,
    History,
    Token,
    answer,
    progress,
    record,
    stream_history,
)

HOLDER = "default/run-id/1"


@dataclass
class Seen:
    batches: list[list[Token]] = field(default_factory=list)
    error: str | None = None
    supersessions: list[ReadSupersession] = field(default_factory=list)
    stale_records: list[ReadStaleRecord] = field(default_factory=list)
    undecodable: list[str] = field(default_factory=list)


seen: list[Seen] = []


@workflow.defn(name="ChatReader")
class DirectChatReader:
    @workflow.run
    async def run(self) -> None:
        client = workflow.create_nexus_client(service=ChatService, endpoint=ENDPOINT)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(
            handle, item_type=Token, endpoint=ENDPOINT, replay_from_store=True
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
        except StreamUnsupportedError:
            # A deployment fault, which fails the Workflow Task for a retry.
            raise
        except Exception as error:
            cause = error.cause if isinstance(error, NexusOperationError) else error
            observed.error = type(cause).__name__
        observed.supersessions = list(reader.supersessions)
        observed.stale_records = list(reader.stale_records)
        seen.append(observed)


class FakeTransport:
    """The endpoint's stream service as a replay sees it."""

    def __init__(self, *answers: ReadOutput | Exception) -> None:
        self.answers = list(answers)
        self.asked: list[ReadRangeInput] = []

    async def read(self, request: ReadInput) -> ReadOutput:
        raise AssertionError(f"a replay never reads, but read {request}")

    async def read_range(self, request: ReadRangeInput) -> ReadOutput:
        self.asked.append(request)
        found = self.answers.pop(0)
        if isinstance(found, Exception):
            raise found
        return found


class DirectHistory(History):
    """Builds the History of a direct reader, a Local Activity marker per read."""

    def __init__(self) -> None:
        super().__init__()
        self._seq = 0
        self._state = b""

    def direct_read(
        self, output: ReadOutput, *, digest: bytes = b"digest"
    ) -> ReadOutput:
        """One read in the task just completed. Returns the answer read_range gives again."""
        self._seq += 1
        after = self._last_token
        stored = sum(1 for r in output.records if r.superseded is None)
        state = b"s%d" % self._seq
        output = ReadOutput(
            records=output.records,
            next_token=output.next_token,
            done=output.done,
            state=state,
            replay=ReadOutputReplay(
                chain_first_run_id="run-id",
                after_token=after,
                stored_records=stored,
                records_digest=digest,
            ),
        )
        # Past the reader's first read, the marker leaves the input state out.
        range = ReadRangeInput(
            stream=STREAM,
            chain_first_run_id="run-id",
            after_token=after,
            to_token=output.next_token,
            stored_records=stored,
            state=(self._state or None) if self._seq == 1 else None,
            records_digest=digest,
            floor_holder=HOLDER,
        )
        self._state = state
        self._last_token = output.next_token
        data = {
            "seq": self._seq,
            "attempt": 1,
            "activity_id": str(self._seq),
            "activity_type": DIRECT_READ_ACTIVITY,
            "complete_time": {"seconds": 1_791_590_400, "nanos": 0},
            "backoff": None,
            "original_schedule_time": None,
        }
        self.add(
            EventType.EVENT_TYPE_MARKER_RECORDED,
            marker_recorded_event_attributes=MarkerRecordedEventAttributes(
                marker_name="core_local_activity",
                details={
                    "data": Payloads(
                        payloads=[
                            Payload(
                                metadata={"encoding": b"json/plain"},
                                data=json.dumps(data).encode(),
                            )
                        ]
                    ),
                    "result": Payloads(
                        payloads=[
                            positions_payload(
                                ENDPOINT,
                                range,
                                output,
                                ReadChain("chat-reader/1", self._seq),
                                state_elided=self._seq > 1,
                            )
                        ]
                    ),
                },
                workflow_task_completed_event_id=self._completed,
            ),
        )
        return ReadOutput(
            records=output.records, next_token=output.next_token, done=False
        )

    _last_token = ""


def direct_history() -> tuple[WorkflowHistory, list[ReadOutput]]:
    """The stream gets a and b, then c, then closes, read directly.

    Returns the History and the answers each read_range gives, in order.
    """
    h = DirectHistory()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    # Local Activities resolve within the task, so the reader reads on until
    # an empty answer, then waits for progress.
    again = [h.direct_read(answer(record(1, text="a"), record(2, text="b")))]
    h.direct_read(answer(cursor="cursor-2"))
    recheck = h.timer_started(60)
    h.task(progress(2))
    h.timer_canceled(recheck)
    again.append(h.direct_read(answer(record(3, text="c"))))
    h.direct_read(answer(cursor="cursor-3"))
    second = h.timer_started(60, timer_id="2")
    h.completed(chat, "summary")
    h.task()
    h.timer_canceled(second, timer_id="2")
    again.append(h.direct_read(answer(record(4, RecordKind.FINISH), done=True)))
    h.workflow_completed()
    return WorkflowHistory(workflow_id="chat-reader", events=h.events), again


async def replay(history: WorkflowHistory, *plugins: StreamDirectReads) -> None:
    await Replayer(
        workflows=[DirectChatReader],
        workflow_runner=UnsandboxedWorkflowRunner(),
        plugins=list(plugins),
    ).replay_workflow(history)


@pytest.fixture(autouse=True)
def clear_seen() -> None:
    seen.clear()


async def test_a_replay_reads_each_range_again_through_the_endpoint() -> None:
    history, again = direct_history()
    transport = FakeTransport(*again)
    await replay(history, StreamDirectReads({ENDPOINT: transport}))
    assert seen == [Seen(batches=[[Token("a"), Token("b")], [Token("c")]])]
    # Empty answers need no call, and each call names the reader's holder and
    # the read's input state.
    assert [r.after_token for r in transport.asked] == ["", "cursor-2", "cursor-3"]
    assert [r.state for r in transport.asked] == [None, b"s2", b"s4"]
    assert {r.floor_holder for r in transport.asked} == {HOLDER}


async def test_the_markers_hold_no_record() -> None:
    history, _ = direct_history()
    markers = [
        e.marker_recorded_event_attributes
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_MARKER_RECORDED
    ]
    assert len(markers) == 5
    for marker in markers:
        [result] = marker.details["result"].payloads
        assert b"cursor-" in result.data
        assert len(result.data) < 1024
        assert "records" not in json.loads(result.data)


async def test_a_replay_without_the_plugin_fails_the_task_and_names_the_opt_out(
    caplog: pytest.LogCaptureFixture,
) -> None:
    history, _ = direct_history()
    with pytest.raises(Exception):
        await replay(history)
    assert seen == []
    assert "replay_from_store=False" in caplog.text


async def test_a_replay_with_no_transport_for_the_endpoint_names_the_opt_out(
    caplog: pytest.LogCaptureFixture,
) -> None:
    history, _ = direct_history()
    with pytest.raises(Exception):
        await replay(history, StreamDirectReads({"elsewhere": FakeTransport()}))
    assert seen == []
    assert "has no transport" in caplog.text
    assert "replay_from_store=False" in caplog.text


async def test_other_records_on_replay_are_nondeterminism() -> None:
    history, _ = direct_history()
    transport = FakeTransport(StreamReplayMismatchError("other records"))
    # The Replayer reports a failure that lang found as a failed activation,
    # with the nondeterminism cause, not as Core's NondeterminismError.
    with pytest.raises(Exception) as raised:
        await replay(history, StreamDirectReads({ENDPOINT: transport}))
    assert "replayed other records" in str(raised.value)
    assert "NonDeterministicError" in str(raised.value)


@pytest.mark.parametrize(
    "failure",
    [StreamReplayRangeLostError("trimmed"), StreamStorageError("503 from the store")],
)
async def test_a_lost_range_or_an_outage_fails_the_task_for_a_retry(
    failure: Exception,
) -> None:
    history, _ = direct_history()
    transport = FakeTransport(failure)
    with pytest.raises(Exception) as raised:
        await replay(history, StreamDirectReads({ENDPOINT: transport}))
    assert type(failure).__name__ in str(raised.value)
    assert "NonDeterministicError" not in str(raised.value)
    assert seen == []


async def test_an_error_mark_replays_as_the_live_read_saw_it() -> None:
    h = DirectHistory()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    bad = RecordWire(token="cursor-2", record=b"", error="codec: no key")
    again = h.direct_read(answer(record(1, text="a"), bad, record(3, text="c")))
    # The codec works again on replay, but Workflow code must see the error.
    again.records[1] = record(2, text="b")
    h.direct_read(answer(cursor="cursor-3"))
    recheck = h.timer_started(60)
    h.completed(chat, "summary")
    h.task()
    h.timer_canceled(recheck)
    h.direct_read(answer(cursor="cursor-3", done=True))
    h.workflow_completed()
    await replay(
        WorkflowHistory(workflow_id="chat-reader", events=h.events),
        StreamDirectReads({ENDPOINT: FakeTransport(again)}),
    )
    assert seen[0].batches == [[Token("a")], [Token("c")]]
    assert seen[0].undecodable == ["cursor-2"]


async def test_supersessions_and_stale_records_replay_the_same() -> None:
    h = DirectHistory()
    h.started()
    h.task()
    chat = h.scheduled("ChatService", "chat")
    h.chat_started(chat)
    h.task(progress(1))
    superseded = RecordWire(
        token="cursor-1",
        record=b"",
        superseded=RecordWireSuperseded(
            producer_id="chat", previous_attempt=1, attempt=2
        ),
    )
    again = h.direct_read(
        answer(
            superseded,
            record(2, text="b", attempt=2),
            record(3, text="old", stale=True),
        )
    )
    h.direct_read(answer(cursor="cursor-3"))
    recheck = h.timer_started(60)
    h.completed(chat, "summary")
    h.task()
    h.timer_canceled(recheck)
    h.direct_read(answer(cursor="cursor-3", done=True))
    h.workflow_completed()
    await replay(
        WorkflowHistory(workflow_id="chat-reader", events=h.events),
        StreamDirectReads({ENDPOINT: FakeTransport(again)}),
    )
    [observed] = seen
    assert observed.batches == [[Token("b")]]
    assert [s.supersession.attempt for s in observed.supersessions] == [2]
    assert [r.value for r in observed.stale_records] == [Token("old")]


async def test_a_recorded_history_replays_with_the_plugin_on() -> None:
    # A reader that records each answer keeps working beside the plugin.
    from tests.contrib.streams import test_nexus_reader

    await Replayer(
        workflows=[test_nexus_reader.ChatReader],
        workflow_runner=UnsandboxedWorkflowRunner(),
        plugins=[StreamDirectReads({})],
    ).replay_workflow(stream_history())
    assert test_nexus_reader.seen[-1].batches == [
        [Token("a"), Token("b")],
        [Token("c")],
    ]


class _Cut(Exception):
    pass


class _Runtime:
    def __init__(self, commands: int) -> None:
        self.commands = commands
        self.heartbeats = 0

    def _task_progress(self) -> tuple[int, float]:
        return self.commands, 0.0

    def _request_task_heartbeat(self) -> None:
        self.heartbeats += 1


@pytest.mark.parametrize("commands", [0, 100])
async def test_a_direct_read_cuts_its_task_at_the_command_bound(
    monkeypatch: pytest.MonkeyPatch, commands: int
) -> None:
    runtime = _Runtime(commands)

    async def local_activity(*_args: Any, **_kwargs: Any) -> Any:
        # The cut comes before the read's Local Activity.
        raise _Cut(runtime.heartbeats)

    monkeypatch.setattr(reader_module.workflow._Runtime, "current", lambda: runtime)
    monkeypatch.setattr(
        reader_module.workflow, "execute_local_activity", local_activity
    )
    monkeypatch.setattr(
        reader_module.workflow, "info", lambda: SimpleNamespace(run_id="run-id")
    )
    reader = StreamReader(
        SimpleNamespace(),  # type: ignore[arg-type]
        item_type=Token,
        endpoint=ENDPOINT,
        replay_from_store=True,
    )
    with pytest.raises(_Cut) as raised:
        await reader._direct_read(ReadInput(stream=STREAM, max_records=1))
    assert raised.value.args == ((1 if commands else 0),)
