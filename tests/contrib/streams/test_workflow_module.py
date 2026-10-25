import asyncio
import dataclasses
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.client import (
    Client,
    WorkflowFailureError,
    WorkflowHandle,
)
from temporalio.common import RetryPolicy, streams
from temporalio.common.streams._backend import _ExtensionCalls
from temporalio.common.streams._defn import definition_of
from temporalio.contrib import stream as redis
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner
from tests.contrib.streams._support import on_native
from tests.helpers import assert_task_fail_eventually, new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


async def read(handle: WorkflowHandle[Any, Any], name: str = "events") -> list[object]:
    stream = handle.stream(streams.with_name(Events, name))
    return [item async for item in stream.subscribe(wait_new_records=False)]


def data(items: Sequence[object]) -> list[object]:
    return [item.data for item in items if isinstance(item, streams.Record)]


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        stream = await workflow.create_stream(Events)
        turns = stream.topic(Events.turns)
        await turns.publish(Turn("user", prompt), Turn("model"))
        await stream.topic("tokens").publish(*prompt.split())
        # A run end closes nothing, so the run closes its Stream before it returns.
        await stream.close(result=prompt.upper())
        return prompt.upper()


async def test_create_publish_and_close(stream_client: Client):
    async with new_worker(stream_client, Agent) as worker:
        handle = await stream_client.start_workflow(
            Agent.run, "hi there", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "HI THERE"
        items = await read(handle)
    assert data(items) == [Turn("user", "hi there"), Turn("model"), "hi", "there"]
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.result == "HI THERE"


@workflow.defn
class Turns:
    @workflow.run
    async def run(self, fail_at: int) -> None:
        for n in range(2):
            try:
                async with workflow.create_stream(
                    streams.with_name(Events, f"turn-{n}")
                ) as turn:
                    await turn.topic(Events.tokens).publish(f"token-{n}")
                    if n == fail_at:
                        raise ApplicationError("turn failed")
            except ApplicationError:
                pass
            await workflow.sleep(timedelta(milliseconds=10))


async def test_async_with_closes_the_stream(stream_client: Client):
    async with new_worker(stream_client, Turns) as worker:
        handle = await stream_client.start_workflow(
            Turns.run, 1, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        first = await read(handle, "turn-0")
        second = await read(handle, "turn-1")
    assert data(first) == ["token-0"]
    assert isinstance(first[-1], streams.Closed) and first[-1].failure is None
    assert data(second) == ["token-1"]
    assert isinstance(second[-1], streams.Closed)
    assert str(second[-1].failure) == "turn failed"


@workflow.defn
class Continuing:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events] | None) -> str:
        if stream is None:
            stream = await workflow.create_stream(Events)
            await stream.topic(Events.tokens).publish("run 1")
            workflow.continue_as_new(stream)
        assert stream.definition is Events
        assert stream == workflow.stream_handle(Events)
        await stream.topic(Events.tokens).publish("run 2")
        await stream.close(result="done")
        return "done"


@workflow.defn
class RenamedAcrossRuns:
    @workflow.run
    async def run(self, first: bool) -> list[str]:
        renamed = streams.with_name(Events, "turn-1")
        if first:
            turn = await workflow.create_stream(renamed)
            await turn.topic(Events.tokens).publish("run 1")
            workflow.continue_as_new(False)
        turn = workflow.stream_handle(renamed)
        await turn.topic(Events.tokens).publish("run 2")
        await turn.close()
        return [workflow.stream_handle(Events).id, turn.id]


async def test_renamed_definition_is_another_stream(stream_client: Client):
    async with new_worker(stream_client, RenamedAcrossRuns) as worker:
        handle = await stream_client.start_workflow(
            RenamedAcrossRuns.run,
            True,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        plain_id, turn_id = await handle.result()
        turn = await read(handle, "turn-1")
    assert plain_id.endswith(":events") and turn_id.endswith(":turn-1")
    assert data(turn) == ["run 1", "run 2"]


def test_with_name_leaves_the_definition_unchanged():
    renamed = streams.with_name(Events, "turn-1")
    assert definition_of(Events).name == "events"
    assert definition_of(renamed).name == "turn-1"
    assert definition_of(renamed).topics == definition_of(Events).topics


async def test_continue_as_new_passes_the_stream(stream_client: Client):
    async with new_worker(stream_client, Continuing) as worker:
        handle = await stream_client.start_workflow(
            Continuing.run, None, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == "done"
        items = await read(handle)
    assert data(items) == ["run 1", "run 2"]
    closed = [item for item in items if isinstance(item, streams.Closed)]
    assert len(closed) == 1 and closed[0].result == "done"


@workflow.defn
class Mistakes:
    def __init__(self) -> None:
        self._stream: streams.WorkflowStream[Events] | None = None

    @workflow.run
    async def run(self) -> list[str]:
        errors = []
        stream = await workflow.create_stream(Events)
        self._stream = stream
        try:
            stream.topic("unknown")
        except streams.StreamError as err:
            errors.append(err.reason)
        await stream.close(result="early")
        try:
            await stream.topic(Events.tokens).publish("late")
        except streams.StreamClosedError as err:
            errors.append(err.reason)
        try:
            await workflow.create_stream(Events)
        except streams.StreamClosedError as err:
            errors.append(err.reason)
        return errors

    @workflow.query
    def publish_in_query(self) -> str:
        assert self._stream is not None
        try:
            self._stream.topic(Events.tokens)._stream._append("tokens", ("x",))
        except streams.ReadOnlyContextError as err:
            return type(err).__name__
        return "wrote"


async def test_replay_reproducible_errors_raise_in_workflow_code(
    stream_client: Client,
):
    async with new_worker(stream_client, Mistakes) as worker:
        handle = await stream_client.start_workflow(
            Mistakes.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await handle.result() == [
            "unknown_topic",
            "stream_closed",
            "stream_closed",
        ]
        assert await handle.query(Mistakes.publish_in_query) == "ReadOnlyContextError"
        items = await read(handle)
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "early"


@workflow.defn
class ClosesAReceivedStream:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> str:
        await stream.topic(Events.tokens).publish("from workflow")
        try:
            await stream.close(result="ended by a writer")
        except streams.StreamNotOwnerError as err:
            return err.reason
        return "closed"


async def test_workflow_closes_only_its_own_streams(stream_client: Client):
    stream = await stream_client.create_stream(Events, id=f"owned-{uuid.uuid4()}")
    async with new_worker(stream_client, ClosesAReceivedStream) as worker:
        assert (
            await stream_client.execute_workflow(
                ClosesAReceivedStream.run,
                streams.WorkflowStream.from_handle(stream),
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "stream_not_owner"
        )
    # The writer can't end the Stream for every reader, so it stays open.
    items = [item async for item in stream.subscribe(wait_new_records=False)]
    assert data(items) == ["from workflow"]


@workflow.defn
class Failing:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("before")
        raise ApplicationError("run failed", non_retryable=True)


async def test_run_end_closes_only_a_native_stream(stream_client: Client):
    async with new_worker(stream_client, Failing) as worker:
        handle = await stream_client.start_workflow(
            Failing.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()
        items = await read(handle)
    assert data(items) == ["before"]
    if not on_native(stream_client):
        # The Stream stays open, so a later reader still waits for a close.
        assert not any(isinstance(item, streams.Closed) for item in items)
        return
    # The server closes the Stream when the run chain ends, with the run's failure.
    closed = items[-1]
    assert isinstance(closed, streams.Closed) and closed.failure is not None
    assert closed.status == streams.CloseStatus.FAILED


@workflow.defn
class FailsInItsBlockOnce:
    @workflow.run
    async def run(self) -> str:
        attempt = workflow.info().attempt
        async with workflow.create_stream(Events) as stream:
            await stream.topic(Events.tokens).publish(f"run-{attempt}")
            if attempt == 1:
                raise ApplicationError("the first run fails")
        return f"done on attempt {attempt}"


async def test_block_end_leaves_the_stream_open_for_a_retry_run(stream_client: Client):
    async with new_worker(stream_client, FailsInItsBlockOnce) as worker:
        handle = await stream_client.start_workflow(
            FailsInItsBlockOnce.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=100), maximum_attempts=3
            ),
        )
        assert await handle.result() == "done on attempt 2"
        items = await read(handle)
    written = [item for item in items if isinstance(item, streams.Record)]
    superseded = [item for item in items if isinstance(item, streams.Superseded)]
    assert [r.data for r in written] == ["run-1", "run-2"]
    assert [r.data for r in written if superseded[0].withdraws(r)] == ["run-1"]
    # Only the block end of the run that succeeds closes the Stream.
    closes = [item for item in items if isinstance(item, streams.Closed)]
    assert len(closes) == 1 and items[-1] is closes[0]
    assert closes[0].failure is None


@workflow.defn
class CronBlock:
    @workflow.run
    async def run(self) -> str:
        async with workflow.create_stream(Events) as stream:
            await stream.topic(Events.tokens).publish("turn")
        return "done"


async def test_block_end_leaves_a_cron_stream_open(stream_client: Client):
    # The Stream spans the cron chain, so a close would fail the next run's create.
    async with new_worker(stream_client, CronBlock) as worker:
        handle = await stream_client.start_workflow(
            CronBlock.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            cron_schedule="@every 1s",
        )
        reader = handle.stream(Events).subscribe(create_wait=timedelta(seconds=20))
        first = await asyncio.wait_for(anext(reader), 20)
        second = await asyncio.wait_for(anext(reader), 20)
        await reader.aclose()
        # Read before the terminate, since a native server closes the Stream with it.
        items = await read(handle)
        await handle.terminate()
    assert isinstance(first, streams.Record) and isinstance(second, streams.Record)
    assert not any(isinstance(item, streams.Closed) for item in items)


failed_close_tasks: set[str] = set()


@workflow.defn
class SchedulesACloseThenFailsItsTask:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("token")
        stream.close_nowait(result="closed")
        workflow_id = workflow.info().workflow_id
        if workflow_id not in failed_close_tasks:
            failed_close_tasks.add(workflow_id)
            raise RuntimeError("fail the Workflow Task that schedules the close")


async def test_failed_task_writes_no_close(stream_client: Client):
    # The test's set must survive the retry of the task, so no sandbox.
    async with new_worker(
        stream_client,
        SchedulesACloseThenFailsItsTask,
        workflow_runner=UnsandboxedWorkflowRunner(),
    ) as worker:
        handle = await stream_client.start_workflow(
            SchedulesACloseThenFailsItsTask.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
        items = await read(handle)
    closes = [item for item in items if isinstance(item, streams.Closed)]
    assert len(closes) == 1 and closes[0].result == "closed"
    history = await handle.fetch_history()
    # The failed task's close never went out, so History has one close: an Activity on
    # Redis, and a System Nexus operation on native.
    activities = [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    ]
    operations = [
        event.nexus_operation_scheduled_event_attributes.operation
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED
    ]
    if on_native(stream_client):
        assert (activities, operations) == ([], ["CloseStream"])
    else:
        assert (activities, operations) == (["__temporal_streams_close"], [])


@workflow.defn
class CreatesOn:
    @workflow.run
    async def run(self, backend: str | None) -> None:
        await workflow.create_stream(Events, backend=backend)


async def test_worker_config_errors_fail_the_task(stream_client: Client):
    # Every Worker has the native backend, so only a backend that the Worker lacks fails the
    # Workflow Task, and a Worker with the right config can take the run on.
    async with new_worker(stream_client, CreatesOn) as worker:
        handle = await stream_client.start_workflow(
            CreatesOn.run,
            "elsewhere",
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await assert_task_fail_eventually(handle, message_contains="backend elsewhere")
        await handle.terminate()


@workflow.defn
class CronTurn:
    @workflow.run
    async def run(self) -> str:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("turn")
        return "done"


async def test_cron_runs_share_an_open_stream(stream_client: Client):
    # No run end closes the chain's Stream, so each cron run writes to it again.
    async with new_worker(stream_client, CronTurn) as worker:
        handle = await stream_client.start_workflow(
            CronTurn.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
            cron_schedule="@every 1s",
        )
        stream = handle.stream(Events)
        reader = stream.subscribe(create_wait=timedelta(seconds=20))
        first = await reader.__anext__()
        second = await reader.__anext__()
        await reader.aclose()
        # Read before the terminate, since a native server closes the Stream with it.
        items = [item async for item in stream.subscribe(wait_new_records=False)]
        await handle.terminate()
    assert isinstance(first, streams.Record) and first.data == "turn"
    assert isinstance(second, streams.Record) and second.data == "turn"
    assert not any(isinstance(item, streams.Closed) for item in items)


@workflow.defn
class SyncHandlers:
    @workflow.init
    def __init__(self, prompt: str) -> None:
        # The create goes first, so the signal's writes find the Stream.
        self._stream = workflow.create_stream_nowait(Events)
        self._prompt = prompt
        self._done = False

    @workflow.run
    async def run(self, prompt: str) -> None:
        assert prompt == self._prompt
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def token(self, text: str) -> None:
        self._stream.topic(Events.tokens).publish_nowait(text, self._prompt)

    @workflow.update
    def finish(self) -> str:
        self._stream.close_nowait(result="finished")
        try:
            self._stream.topic(Events.tokens).publish_nowait("late")
        except streams.StreamClosedError as err:
            return err.reason
        finally:
            self._done = True
        return "wrote"


async def test_sync_handlers_publish_and_close_with_no_await(stream_client: Client):
    async with new_worker(stream_client, SyncHandlers) as worker:
        handle = await stream_client.start_workflow(
            SyncHandlers.run, "p", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.signal(SyncHandlers.token, "a")
        assert await handle.execute_update(SyncHandlers.finish) == "stream_closed"
        await handle.result()
        items = await read(handle)
    assert data(items) == ["a", "p"]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "finished"


@workflow.defn
class CreatesInInit:
    @workflow.init
    def __init__(self, prompt: str) -> None:
        self._stream = workflow.create_stream_nowait(Events)
        self._stream.topic(Events.tokens).publish_nowait(prompt)

    @workflow.run
    async def run(self, prompt: str) -> str:
        await self._stream.topic(Events.tokens).publish(prompt.upper())
        await self._stream.close(result="done")
        return "done"


async def test_init_creates_and_publishes_with_no_await(stream_client: Client):
    async with new_worker(stream_client, CreatesInInit) as worker:
        handle = await stream_client.start_workflow(
            CreatesInInit.run,
            "hi",
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        assert await handle.result() == "done"
        items = await read(handle)
    assert data(items) == ["hi", "HI"]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "done"


@workflow.defn
class Gathers:
    @workflow.run
    async def run(self) -> list[int]:
        tokens = (await workflow.create_stream(Events)).topic(Events.tokens)
        tokens.publish_nowait("0")
        # The first write starts, so the next ones wait behind it.
        await asyncio.sleep(0)
        return list(
            await asyncio.gather(*(tokens.publish(str(n)) for n in range(1, 5)))
        )


async def test_concurrent_publishes_keep_their_order(stream_client: Client):
    async with new_worker(stream_client, Gathers) as worker:
        handle = await stream_client.start_workflow(
            Gathers.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        offsets = await handle.result()
        items = await read(handle)
    assert data(items) == ["0", "1", "2", "3", "4"]
    # The four writes that waited went as one Local Activity, and each call got the offset of
    # its own Record.
    assert (
        offsets
        == [item.offset for item in items if isinstance(item, streams.Record)][1:]
    )
    history = await handle.fetch_history()
    markers = [
        e
        for e in history.events
        if e.event_type == EventType.EVENT_TYPE_MARKER_RECORDED
    ]
    # The create, the first write and the four that waited.
    assert len(markers) == 3
    # Replay answers from the markers and writes nothing.
    await Replayer(
        workflows=[Gathers], stream_backends=stream_client.config()["stream_backends"]
    ).replay_workflow(history)
    assert data(await read(handle)) == ["0", "1", "2", "3", "4"]


class _RandomCodec(PayloadCodec):
    # Adds random bytes to each payload, as the random IV of an encryption codec does.
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/random"},
                data=os.urandom(8) + p.SerializeToString(),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            _inner(p.data[8:]) if _encoded(p, b"binary/random") else p for p in payloads
        ]


class _GrowingCodec(PayloadCodec):
    # Triples each payload, so a merge under the Workflow's cap passes the Store's batch limit
    # after the codec.
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={
                    "encoding": b"binary/grown",
                    "size": str(len(p.SerializeToString())).encode(),
                },
                data=p.SerializeToString() * 3,
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            _inner(p.data[: int(p.metadata["size"])])
            if _encoded(p, b"binary/grown")
            else p
            for p in payloads
        ]


def _encoded(payload: Payload, encoding: bytes) -> bool:
    return payload.metadata.get("encoding") == encoding


def _inner(data: bytes) -> Payload:
    inner = Payload()
    inner.ParseFromString(data)
    return inner


def with_codec(client: Client, codec: PayloadCodec) -> Client:
    config = client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=codec
    )
    return Client(**config)


async def test_retried_write_goes_in_once(
    stream_client: Client, monkeypatch: pytest.MonkeyPatch
):
    original = _ExtensionCalls.call_async
    sent: list[list[bytes]] = []

    async def loses_the_first_answer(
        self: Any, method: str, request: Any, response: Any
    ) -> Any:
        answer = await original(self, method, request, response)
        if method == "AppendRecords":
            sent.append([record.body.SerializeToString() for record in request.records])
            if len(sent) == 1:
                raise RuntimeError("the answer is lost")
        return answer

    monkeypatch.setattr(_ExtensionCalls, "call_async", loses_the_first_answer)
    client = with_codec(stream_client, _RandomCodec())
    async with new_worker(client, Gathers) as worker:
        handle = await client.start_workflow(
            Gathers.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
        items = await read(handle)
    assert data(items) == ["0", "1", "2", "3", "4"]
    # The retry sends the bytes of the first attempt, so the Store's dedupe takes it as the
    # same batch, though the codec adds random bytes.
    assert sent[0] == sent[1]


@workflow.defn
class PublishesLargeValues:
    @workflow.run
    async def run(self, size: int) -> list[int]:
        tokens = (await workflow.create_stream(Events)).topic(Events.tokens)
        return list(
            await asyncio.gather(
                tokens.publish(*["a" * size] * 3), tokens.publish(*["b" * size] * 3)
            )
        )


async def test_merge_that_the_codec_grows_past_the_batch_limit(stream_client: Client):
    # The two publishes merge under the Workflow's cap, and the codec makes the merge larger
    # than one batch may be, so each publish goes as its own batch.
    client = with_codec(stream_client, _GrowingCodec())
    # The native server takes half the bytes of one append, so each publish there is smaller.
    size = 150_000 if on_native(stream_client) else 300_000
    async with new_worker(client, PublishesLargeValues) as worker:
        handle = await client.start_workflow(
            PublishesLargeValues.run,
            size,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        offsets = await handle.result()
        items = await read(handle)
    written = [item for item in items if isinstance(item, streams.Record)]
    assert [len(r.data) for r in written] == [size] * 6
    assert offsets == [written[2].offset, written[5].offset]


@workflow.defn
class CreatesOnTheDefault:
    @workflow.run
    async def run(self) -> str:
        return workflow.stream_handle(Events).backend or ""


@workflow.defn
class CreatesOnRedis:
    @workflow.run
    async def run(self) -> str:
        stream = await redis.workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("on redis")
        await redis.workflow.stream_handle(Events).close(result="closed")
        return stream.backend or ""


async def test_default_is_native_with_no_default_backend(client: Client):
    # With no backend marked default=True, the default is native, which every Worker has.
    # The Redis calls still pick the registered Redis backend, and the memory one stands in
    # for it.
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend()]
    memory_client = Client(**config)
    async with new_worker(memory_client, CreatesOnTheDefault, CreatesOnRedis) as worker:
        assert (
            await memory_client.execute_workflow(
                CreatesOnTheDefault.run,
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == "native"
        )
        on_redis = await memory_client.start_workflow(
            CreatesOnRedis.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert await on_redis.result() == "memory"
    assert on_redis.stream(Events).backend == "native"
    stream = on_redis.stream(Events, backend="memory")
    items = [item async for item in stream.subscribe()]
    assert data(items) == ["on redis"]
    assert isinstance(items[-1], streams.Closed) and items[-1].result == "closed"


class _RecordsCloses(MemoryStreamBackend):
    # A backend that closes another way overrides the close of a Workflow's Stream.
    closes: list[str] = []

    async def _close_in_workflow(self, request: Any) -> int:
        self.closes.append(request.stream.id)
        return await super()._close_in_workflow(request)


async def test_backend_carries_the_workflow_close(client: Client):
    config = client.config()
    config["stream_backends"] = [_RecordsCloses(default=True)]
    recording = Client(**config)
    async with new_worker(recording, Agent) as worker:
        handle = await recording.start_workflow(
            Agent.run, "hi", id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        await handle.result()
    assert _RecordsCloses.closes == [handle.stream(Events).id]
