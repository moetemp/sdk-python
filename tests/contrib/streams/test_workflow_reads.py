import dataclasses
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import pytest

from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib import streams
from temporalio.contrib.streams import _errors
from temporalio.contrib.streams import workflow as stream_workflow
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer
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
        stream = await stream_workflow.create_stream(Events, backend=backend)
        await stream.topic(Events.turns).publish(Turn("user", "hi"))
        for token in ("a", "b", "c"):
            # Each token goes out in its own Workflow Task, so the reader waits for some.
            await workflow.sleep(0.3)
            await stream.topic(Events.tokens).publish(token)
        return "written"


@dataclass
class ReadInput:
    stream: stream_workflow.WorkflowStream[Events]
    tokens_only: bool = False
    wait_new_records: bool = True


@workflow.defn
class Reader:
    @workflow.run
    async def run(self, input: ReadInput) -> list[Any]:
        seen: list[Any] = []
        items = (
            input.stream.topic(Events.tokens).subscribe(
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


def plugins_of(client: Client) -> list[Any]:
    return [
        plugin
        for plugin in client.config()["plugins"]
        if isinstance(plugin, streams.StreamPlugin)
    ]


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
        stream = streams.StreamClient(client).workflow_stream(
            writer, Events, backend=backend
        )
        reader = await client.start_workflow(
            Reader.run,
            ReadInput(
                stream_workflow.WorkflowStream.from_handle(stream), **read_options
            ),
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
    replayer = Replayer(workflows=[Reader], plugins=plugins_of(stream_client))
    await replayer.replay_workflow(await reader.fetch_history())


async def test_workflow_reads_one_topic(stream_client: Client):
    seen, _ = await write_and_read(stream_client, None, tokens_only=True)
    assert seen == ["a", "b", "c", "closed written"]


@workflow.defn
class ReadsMissing:
    @workflow.run
    async def run(self, stream_id: str) -> str:
        stream = stream_workflow.WorkflowStream[Events](id=stream_id)
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
        stream = await stream_workflow.create_stream(Events, close_with_run=False)
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
        self._stream: stream_workflow.WorkflowStream[Events] | None = None

    @workflow.run
    async def run(self) -> None:
        # Set in the first task, since a query may come before a create's Local Activity ends.
        self._stream = stream_workflow.create_stream_nowait(Events)
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
        stream = stream_workflow.WorkflowStream[Events](id=stream_id)
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
