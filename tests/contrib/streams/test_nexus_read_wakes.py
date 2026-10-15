"""A Bridge reader reads first and wakes only when it waits.

The stream operation attaches the caller's callback idle, so it hears no
progress until a read that finds nothing asks the notifier to wake it after
the read's position. A busy reader never asks, so it gets no deliveries. These
live tests need the Bridge lane's server, with idle callbacks and
``WaitStreamCallback``. Any other server makes them skip.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest

import temporalio.nexus
from temporalio import workflow
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.streams import StreamRef, close_workflow_stream, workflow_writer
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    StreamReader,
    TemporalStreamsHandler,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from tests.contrib.streams.test_nexus_reader import (
    STREAM,
    ChatService,
    Token,
    _skip_without_notifier,
)
from tests.helpers.nexus import make_nexus_endpoint_name


@workflow.defn(name="WakeOwner")
class WakeOwner:
    """Publishes the values each Signal names, and closes on ``close``."""

    def __init__(self) -> None:
        self._queue: list[list[str]] = []
        self._close = False

    @workflow.run
    async def run(self, first: list[str]) -> None:
        writer = workflow_writer(STREAM.topic)
        for text in first:
            writer.publish(Token(text))
        while True:
            await workflow.wait_condition(lambda: bool(self._queue) or self._close)
            while self._queue:
                for text in self._queue.pop(0):
                    writer.publish(Token(text))
            if self._close:
                await close_workflow_stream(STREAM.topic, result="closed")
                return

    @workflow.signal
    def publish(self, texts: list[str]) -> None:
        self._queue.append(texts)

    @workflow.signal
    def close(self) -> None:
        self._close = True


@workflow.defn(name="WakeReader")
class WakeReader:
    @workflow.run
    async def run(self, endpoint: str) -> list[str]:
        client = workflow.create_nexus_client(service=ChatService, endpoint=endpoint)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(
            handle, item_type=Token, endpoint=endpoint, replay_from_store=False
        )
        texts: list[str] = []
        while (batch := await reader.next()) is not None:
            texts.extend(token.text for token in batch)
        return texts


class _Lane:
    def __init__(self, client: Client, endpoint: str, task_queue: str) -> None:
        self.client = client
        self.endpoint = endpoint
        self.task_queue = task_queue
        self.owner_id = f"wake-owner-{uuid.uuid4()}"
        self.first: list[str] = []

    def owner(self) -> WorkflowHandle[Any, Any]:
        return self.client.get_workflow_handle(self.owner_id)

    async def read(self) -> WorkflowHandle[Any, Any]:
        return await self.client.start_workflow(
            WakeReader.run,
            self.endpoint,
            id=f"wake-reader-{uuid.uuid4()}",
            task_queue=self.task_queue,
        )


@pytest.fixture
async def lane(client: Client, env: WorkflowEnvironment) -> AsyncIterator[_Lane]:
    ref = StreamRef.for_workflow(f"probe-{uuid.uuid4()}", topic=STREAM.topic)
    await _skip_without_notifier(client, ref)
    task_queue = f"wake-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    streams = await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        plugins=[MemoryStreams().notify_on_append()],
    )
    lane = _Lane(streams, endpoint, task_queue)

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            WakeOwner.run, lane.first, id=lane.owner_id, task_queue=task_queue
        )
        return StreamRef.for_workflow(lane.owner_id, topic=STREAM.topic)

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        streams,
        task_queue=task_queue,
        workflows=[WakeOwner, WakeReader],
        nexus_service_handlers=[ChatServiceHandler(), TemporalStreamsHandler()],
    ):
        yield lane


def _deliveries(history: Any) -> int:
    return sum(
        len(e.workflow_task_scheduled_event_attributes.nexus_operation_progress)
        for e in history.events
        if e.HasField("workflow_task_scheduled_event_attributes")
    )


async def _owner_up(lane: _Lane) -> None:
    for _ in range(100):
        with contextlib.suppress(Exception):
            await lane.owner().describe()
            return
        await asyncio.sleep(0.1)
    raise AssertionError("the owner never started")


async def test_live_a_reader_that_never_waits_gets_no_progress(lane: _Lane):
    # Every record is there before the reader reads, so each read finds some,
    # and the close comes as the completion, not as progress.
    lane.first = [f"t{n}" for n in range(20)]
    reader = await lane.read()
    await _owner_up(lane)
    await lane.owner().signal(WakeOwner.close)
    assert await asyncio.wait_for(reader.result(), 30) == lane.first
    assert _deliveries(await reader.fetch_history()) == 0


async def test_live_an_idle_reader_is_woken_once_per_idle_stretch(lane: _Lane):
    lane.first = ["first"]
    reader = await lane.read()
    await _owner_up(lane)
    # The reader reads "first", finds nothing more and waits.
    await asyncio.sleep(4)
    await lane.owner().signal(WakeOwner.publish, ["second", "third"])
    await asyncio.sleep(4)
    await lane.owner().signal(WakeOwner.close)
    assert await asyncio.wait_for(reader.result(), 30) == ["first", "second", "third"]
    # One wait, so one delivery for the two appends of that stretch.
    assert _deliveries(await reader.fetch_history()) == 1


async def test_live_a_record_between_the_empty_read_and_the_wait_is_not_lost(
    lane: _Lane,
):
    # Appends come every few hundred milliseconds, so some land while the
    # reader's empty read asks for its wait. The service reads once more
    # after it asks, so the reader misses nothing and never stalls.
    lane.first = ["first"]
    reader = await lane.read()
    await _owner_up(lane)
    texts = [f"t{n}" for n in range(15)]
    for text in texts:
        await lane.owner().signal(WakeOwner.publish, [text])
        await asyncio.sleep(0.3)
    await lane.owner().signal(WakeOwner.close)
    assert await asyncio.wait_for(reader.result(), 60) == ["first", *texts]
