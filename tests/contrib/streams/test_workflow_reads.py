import dataclasses
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest

import temporalio.api.streamservice.v1 as stream_pb
from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import streams
from temporalio.common.streams import _errors, _workflow_reader
from temporalio.common.streams._native import _DirectReadCodec
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError
from tests.contrib.streams._support import create_owned, on_native
from tests.contrib.streams.test_redis import _Rot13Codec
from tests.helpers import new_worker


@dataclass
class Turn:
    kind: str
    text: str = ""


@streams.defn(name="events")
class Events:
    turns = streams.topic(Turn)
    tokens = streams.topic(str)


@workflow.defn
class Writer:
    @workflow.run
    async def run(self, backend: str | None) -> str:
        stream = await workflow.create_stream(Events, backend=backend)
        await stream.topic(Events.turns).publish(Turn("user", "hi"))
        for token in ("a", "b", "c"):
            # Each token goes out in its own Workflow Task, so the reader waits for some.
            await workflow.sleep(0.3)
            await stream.topic(Events.tokens).publish(token)
        await stream.close(result="written")
        return "written"


@dataclass
class ReadInput:
    stream: streams.WorkflowStream[Events]
    tokens_only: bool = False
    wait_new_records: bool = True


@workflow.defn
class Reader:
    @workflow.run
    async def run(self, input: ReadInput) -> list[Any]:
        seen: list[Any] = []
        items = (
            input.stream.subscribe(
                Events.tokens,
                wait_new_records=input.wait_new_records,
                create_wait=timedelta(seconds=20),
            )
            if input.tokens_only
            else input.stream.subscribe(
                wait_new_records=input.wait_new_records,
                create_wait=timedelta(seconds=20),
            )
        )
        async for item in items:
            # The Topic's type decodes each body in Workflow code.
            if isinstance(item, streams.Record) and isinstance(item.data, Turn):
                seen.append(f"turn {item.data.kind} {item.data.text}")
            elif isinstance(item, streams.Record):
                seen.append(item.data)
            elif isinstance(item, streams.Closed):
                seen.append(f"closed {item.result}")
        return seen


def backends_of(client: Client) -> list[Any]:
    return list(client.config()["stream_backends"])


async def write_and_read(
    client: Client, backend: str | None, **read_options: Any
) -> tuple[list[Any], Any]:
    async with new_worker(client, Writer, Reader) as worker:
        writer = await client.start_workflow(
            Writer.run,
            backend,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        stream = writer.stream(Events, backend=backend)
        reader = await client.start_workflow(
            Reader.run,
            ReadInput(streams.WorkflowStream.from_handle(stream), **read_options),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        result = await reader.result()
        assert await writer.result() == "written"
    return result, reader


async def test_workflow_reads_another_workflows_stream(stream_client: Client):
    seen, reader = await write_and_read(stream_client, None)
    assert seen == ["turn user hi", "a", "b", "c", "closed written"]
    # Each page is in History, so a replay reads the same items.
    replayer = Replayer(workflows=[Reader], stream_backends=backends_of(stream_client))
    await replayer.replay_workflow(await reader.fetch_history())


async def test_workflow_reads_one_topic(stream_client: Client):
    seen, _ = await write_and_read(stream_client, None, tokens_only=True)
    assert seen == ["a", "b", "c", "closed written"]


@workflow.defn
class ReadsMissing:
    @workflow.run
    async def run(self, stream_id: str) -> str:
        stream = streams.WorkflowStream[Events](id=stream_id)
        try:
            async for _ in stream.subscribe():
                pass
        except streams.StreamNotFoundError as err:
            return f"not found: {err.reason}"
        return "read"


async def test_a_missing_stream_raises_in_workflow_code(stream_client: Client):
    async with new_worker(stream_client, ReadsMissing) as worker:
        result = await stream_client.execute_workflow(
            ReadsMissing.run,
            f"missing-{uuid.uuid4()}",
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
    assert result == "not found: stream_not_found"


@workflow.defn
class ReadsOwnWrites:
    @workflow.run
    async def run(self) -> list[Any]:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("mine")
        # A read's Local Activity first writes the run's buffer, so the read sees it.
        return [
            item.data
            async for item in stream.subscribe(wait_new_records=False)
            if isinstance(item, streams.Record)
        ]


async def test_a_redis_read_sees_the_runs_own_writes(stream_client: Client):
    async with new_worker(stream_client, ReadsOwnWrites) as worker:
        assert await stream_client.execute_workflow(
            ReadsOwnWrites.run,
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        ) == ["mine"]


@workflow.defn
class ReadsInQuery:
    def __init__(self) -> None:
        self._stream: streams.WorkflowStream[Events] | None = None

    @workflow.run
    async def run(self) -> None:
        # Set in the first task, since a query may come before a create's Local Activity ends.
        self._stream = workflow.create_stream_nowait(Events)
        await workflow.wait_condition(lambda: False)

    @workflow.query
    def read(self) -> str:
        assert self._stream is not None
        items = self._stream.subscribe()
        try:
            # A query can't await, so it takes the read's first step by hand.
            cast(Coroutine[Any, Any, Any], items.__anext__()).send(None)
        except streams.ReadOnlyContextError as err:
            return str(err)
        except StopIteration:
            pass
        return "read"


async def test_a_read_in_a_query_raises(stream_client: Client):
    async with new_worker(stream_client, ReadsInQuery) as worker:
        handle = await stream_client.start_workflow(
            ReadsInQuery.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        assert "read-only" in await handle.query(ReadsInQuery.read)
        await handle.terminate()


@workflow.defn
class FailsOnStreamError:
    @workflow.run
    async def run(self, stream_id: str) -> None:
        stream = streams.WorkflowStream[Events](id=stream_id)
        async for _ in stream.subscribe():
            pass


async def test_an_uncaught_read_error_fails_the_run(stream_client: Client):
    # A Stream error is in History, so it fails the run, not the Workflow Task.
    async with new_worker(stream_client, FailsOnStreamError) as worker:
        with pytest.raises(WorkflowFailureError) as failure:
            await stream_client.execute_workflow(
                FailsOnStreamError.run,
                f"missing-{uuid.uuid4()}",
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
    assert isinstance(failure.value.cause, ApplicationError)
    assert failure.value.cause.type == "StreamNotFoundError"


async def test_workflow_reads_go_through_the_codec(stream_client: Client):
    # The Store keeps encoded bodies, and Workflow code can't run the codec, so the read's
    # Local Activity decodes them.
    config = stream_client.config()
    config["data_converter"] = dataclasses.replace(
        DataConverter.default, payload_codec=_Rot13Codec()
    )
    coded = Client(**config)
    seen, _ = await write_and_read(coded, None)
    assert seen == ["turn user hi", "a", "b", "c", "closed written"]


@workflow.defn
class WritesAndWaits:
    @workflow.run
    async def run(self) -> None:
        stream = await workflow.create_stream(Events)
        await stream.topic(Events.tokens).publish("a")
        await workflow.wait_condition(lambda: False)


@workflow.defn
class ReadsUntilItEnds:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> list[Any]:
        items = stream.subscribe(create_wait=timedelta(seconds=20))
        seen = [item.data async for item in items if isinstance(item, streams.Record)]
        return [seen, items.owner_ended, items.next_offset is not None]


async def test_a_read_ends_when_the_owner_ended_with_no_close(
    stream_client: Client, monkeypatch: pytest.MonkeyPatch
):
    # A terminated run writes no close, so with no end of its own the reader would wait
    # until the Stream expires. The native server closes the Stream with the run instead.
    monkeypatch.setattr(_workflow_reader, "_OWNER_END_GRACE", timedelta())
    async with new_worker(stream_client, WritesAndWaits, ReadsUntilItEnds) as worker:
        writer = await stream_client.start_workflow(
            WritesAndWaits.run, id=f"wf-{uuid.uuid4()}", task_queue=worker.task_queue
        )
        stream = writer.stream(Events)
        reader = await stream_client.start_workflow(
            ReadsUntilItEnds.run,
            streams.WorkflowStream.from_handle(stream),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        async for item in stream.subscribe(create_wait=timedelta(seconds=20)):
            if isinstance(item, streams.Record):
                break
        await writer.terminate()
        assert await reader.result() == [["a"], not on_native(stream_client), True]


@workflow.defn
class CountsTokens:
    @workflow.run
    async def run(self, stream: streams.WorkflowStream[Events]) -> int:
        items = stream.subscribe(Events.tokens, wait_new_records=False)
        return len([item async for item in items if isinstance(item, streams.Record)])


async def test_a_workflow_page_stays_below_the_marker_limit(stream_client: Client):
    # A Redis page is a History marker, and the server terminates a run whose marker is over
    # 2 MiB. So a page of large Records splits.
    stream = await create_owned(stream_client, Events, id=f"big-{uuid.uuid4()}")
    for _ in range(25):
        await stream.topic(Events.tokens).publish("x" * 100_000)
    async with new_worker(stream_client, CountsTokens) as worker:
        assert (
            await stream_client.execute_workflow(
                CountsTokens.run,
                streams.WorkflowStream.from_handle(stream),
                id=f"wf-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            == 25
        )


def test_a_server_failure_maps_by_its_reason():
    by_type = _errors.from_application_error(
        ApplicationError("closed", type="stream_closed")
    )
    assert isinstance(by_type, streams.StreamClosedError)
    by_message = _errors.from_application_error(
        ApplicationError("stream_not_found: Nobody created the Stream")
    )
    assert isinstance(by_message, streams.StreamNotFoundError)
    assert _errors.from_application_error(ApplicationError("other")) is None


async def test_a_native_replay_checks_the_range_it_reads_again():
    # A replay reads a recorded range again. Other Records there mean other code ran, and
    # fewer mean the Store lost the range, which a retry of the task can't fix.
    backend = SimpleNamespace()
    codec = _DirectReadCodec(lambda: cast(Any, backend), None, None)

    def record(topic: str) -> stream_pb.StreamRecord:
        return stream_pb.StreamRecord(
            kind=stream_pb.StreamRecordKind.STREAM_RECORD_KIND_DATA,
            offset=5,
            topic=topic,
        )

    request = stream_pb.ReadRecordsRequest(
        namespace="ns", stream=stream_pb.StreamRef(id="s"), from_offset=5
    )
    live = stream_pb.ReadRecordsResponse(records=[record("tokens")], next_offset=6)
    positions = (
        await streams.NativeStreamBackend()._read_result(
            "run", request, live, False, DataConverter.default
        )
    ).payload
    reads: list[stream_pb.ReadRecordsRequest] = []
    answers: list[Any] = [
        live,
        stream_pb.ReadRecordsResponse(records=[record("turns")], next_offset=6),
        stream_pb.ReadRecordsResponse(next_offset=6),
        streams.StreamExpiredError("trimmed", reason="stream_expired"),
    ]

    async def call_async(_method: str, request: Any, _response: Any) -> Any:
        reads.append(request)
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    backend.call_async = call_async
    # The live task takes the page from the cache, and a replay reads it again.
    for _ in range(2):
        [page] = await codec.decode([positions])
        assert stream_pb.ReadRecordsResponse.FromString(page.data) == live
    assert len(reads) == 1
    assert reads[0].to_offset == 6 and reads[0].from_offset == 5
    with pytest.raises(NondeterminismError):
        await codec.decode([positions])
    for _ in range(2):
        with pytest.raises(ApplicationError) as gone:
            await codec.decode([positions])
        assert gone.value.type == "STREAM_RANGE_GONE" and not gone.value.non_retryable
