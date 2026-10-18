"""Bridge readers woken through producer notifications.

The handler's store notifies the server after each append. The caller reads
directly through ``ClientTransport``. These live tests need the
Bridge lane's server, with the stream notifier, Nexus progress, the debounce
and ``ExecuteSyncNexusOperation``. Any other server makes them skip.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest

import temporalio.nexus
from temporalio import workflow
from temporalio.api.nexus.v1 import NexusOperationProgress
from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamRef,
    StreamStorePlugin,
    stream_producer,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    ClientTransport,
    StreamDirectReads,
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

# A wake lands within the server's debounce window (1 second by default).
QUICK = 5.0


def _store(kind: str) -> StreamStorePlugin:
    from temporalio.contrib.streams.redis import RedisStreams

    if kind == "memory":
        return MemoryStreams()
    env = "STREAMS_REDIS_CLUSTER_URL" if kind == "cluster" else "STREAMS_REDIS_URL"
    url = os.environ.get(env, "")
    if not url:
        pytest.skip(f"set {env} to run on {kind}")
    return RedisStreams(
        url, cluster=kind == "cluster", key_prefix=f"push-{uuid.uuid4().hex}"
    )


@workflow.defn(name="PushOwner")
class PushOwner:
    """Publishes ``first``, then on each Signal ``publish`` the values it names."""

    def __init__(self) -> None:
        self._queue: list[list[str]] = []
        self._done = False

    @workflow.run
    async def run(self) -> None:
        writer = stream_producer(STREAM.topic)
        writer.append(Token("first"))
        while not self._done:
            await workflow.wait_condition(lambda: bool(self._queue) or self._done)
            while self._queue:
                for text in self._queue.pop(0):
                    writer.append(Token(text))
                    await workflow.sleep(timedelta(milliseconds=100))

    @workflow.signal
    def publish(self, texts: list[str]) -> None:
        self._queue.append(texts)

    @workflow.signal
    def done(self) -> None:
        self._done = True


@workflow.defn(name="PushReader")
class PushReader:
    @workflow.run
    async def run(self, endpoint: str, count: int) -> list[str]:
        client = workflow.create_nexus_client(service=ChatService, endpoint=endpoint)
        handle = await client.start_operation(ChatService.chat, "hello")
        reader = StreamReader(
            handle,
            item_type=Token,
            endpoint=endpoint,
            idle_wait=timedelta(milliseconds=50),
            replay_from_store=True,
        )
        texts: list[str] = []
        while len(texts) < count and (batch := await reader.next()) is not None:
            texts.extend(token.text for token in batch)
        return texts


class _Lane:
    def __init__(self, client: Client, endpoint: str, task_queue: str) -> None:
        self.client = client
        self.endpoint = endpoint
        self.task_queue = task_queue
        self.owner_id = f"push-owner-{uuid.uuid4()}"


@pytest.fixture(params=["memory", "redis", "cluster"])
async def lane(
    request: pytest.FixtureRequest, client: Client, env: WorkflowEnvironment
) -> AsyncIterator[_Lane]:
    ref = StreamRef.for_workflow(f"probe-{uuid.uuid4()}", topic=STREAM.topic)
    await _skip_without_notifier(client, ref)
    task_queue = f"push-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    store = _store(request.param).notify_on_append()
    reads = StreamDirectReads({endpoint: ClientTransport(client, endpoint)})
    streams = await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        plugins=[store, reads],
    )
    lane = _Lane(streams, endpoint, task_queue)

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            PushOwner.run, id=lane.owner_id, task_queue=task_queue
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
        workflows=[PushOwner, PushReader],
        nexus_service_handlers=[ChatServiceHandler(), TemporalStreamsHandler()],
    ):
        yield lane
        with contextlib.suppress(Exception):
            await streams.get_workflow_handle(lane.owner_id).signal(PushOwner.done)


async def test_live_a_sleeping_nexus_reader_wakes_on_the_next_append(lane: _Lane):
    reader = await lane.client.start_workflow(
        PushReader.run,
        args=[lane.endpoint, 2],
        id=f"push-reader-{uuid.uuid4()}",
        task_queue=lane.task_queue,
    )
    # The reader reads the first record, then sleeps on progress.
    await asyncio.sleep(4)
    started = time.monotonic()
    await lane.client.get_workflow_handle(lane.owner_id).signal(
        PushOwner.publish, ["second"]
    )
    assert await asyncio.wait_for(reader.result(), 30) == ["first", "second"]
    # The owner's notification woke it, not a recheck.
    assert time.monotonic() - started < QUICK


async def test_live_a_busy_stream_notifies_at_most_once_per_debounce_window(
    lane: _Lane,
):
    reader = await lane.client.start_workflow(
        PushReader.run,
        args=[lane.endpoint, 31],
        id=f"push-reader-{uuid.uuid4()}",
        task_queue=lane.task_queue,
    )
    await asyncio.sleep(3)
    started = time.monotonic()
    await lane.client.get_workflow_handle(lane.owner_id).signal(
        PushOwner.publish, [str(n) for n in range(30)]
    )
    assert len(await asyncio.wait_for(reader.result(), 60)) == 31
    elapsed = time.monotonic() - started
    deliveries = sum(
        sum(
            a.Is(NexusOperationProgress.DESCRIPTOR)
            for a in e.workflow_task_scheduled_event_attributes.attachments
        )
        for e in (await reader.fetch_history()).events
        if e.HasField("workflow_task_scheduled_event_attributes")
    )
    # One leading and one trailing delivery per window, whatever the rate.
    assert 1 <= deliveries <= 2 * (int(elapsed) + 1), deliveries
