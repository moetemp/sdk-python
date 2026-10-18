"""Turning stream notifications on, and closing a stream with an outcome.

A close ends every read in the store, then Core closes the stream's notifier
on the server with the outcome. The stock test server has no notifier, so a
close there ends the stream in the store only. A server with the notifier
records the outcome, which ``DescribeStreamNotifier`` shows.
"""

from __future__ import annotations

import pytest

from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import (
    DescribeStreamNotifierRequest,
    DescribeStreamNotifierResponse,
)
from temporalio.client import Client
from temporalio.contrib.streams import (
    RecordKind,
    StreamClosedError,
    StreamRefusedError,
    StreamUnsupportedError,
    get_stream_handle,
    topic,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.exceptions import ApplicationError
from tests.contrib.streams._support import (
    connect_with,
    describe_stream_notifier,
    read_all,
)
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker

TOKENS = topic("tokens", str)


def test_notifications_are_on_until_turned_off() -> None:
    store = MemoryStreams()
    assert store._config.notify_on_append
    assert store.notify_on_append(False) is store
    assert not store._config.notify_on_append
    assert store.notify_on_append()._config.notify_on_append
    assert store._config.max_notifiers == 0


def test_notify_on_append_takes_a_cap() -> None:
    store = MemoryStreams().notify_on_append(max_notifiers=5)
    assert store._config.max_notifiers == 5
    with pytest.raises(ValueError, match="at least 1"):
        MemoryStreams().notify_on_append(max_notifiers=0)


async def test_notifications_turn_on_only_before_a_connect(client: Client) -> None:
    store = MemoryStreams()
    await connect_with(client, store)
    with pytest.raises(ValueError, match="before a client connects"):
        store.notify_on_append()


async def test_a_close_takes_a_result_or_an_error_not_both(client: Client) -> None:
    store = MemoryStreams()
    streams_client = await connect_with(client, store)
    handle = get_stream_handle(streams_client, "any-owner", topic=TOKENS)
    with pytest.raises(ValueError, match="result or an error"):
        await handle.close("done", error=ApplicationError("no"))
    with pytest.raises(ValueError, match="result or an error"):
        await store.close_stream(
            streams_client, handle.ref, "done", error=ApplicationError("no")
        )


async def test_a_close_without_an_outcome_ends_reads_and_refuses_appends(
    client: Client,
) -> None:
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, Waits) as worker:
        workflow_id = await running_owner(streams_client, worker.task_queue)
        handle = get_stream_handle(streams_client, workflow_id, topic=TOKENS)
        producer = handle.producer(producer_id="writer", attempt=1)
        await producer.append("a", "b")
        await handle.close()
        records = await read_all(handle.read())
        assert [(r.kind, r.value) for r in records] == [
            (RecordKind.DATA, "a"),
            (RecordKind.DATA, "b"),
        ]
        with pytest.raises(StreamClosedError):
            await producer.append("c")
        # A retried close is accepted.
        await handle.close("again")
        await streams_client.get_workflow_handle(workflow_id).signal(Waits.finish)


async def test_a_close_with_an_outcome_reaches_the_notifier(client: Client) -> None:
    streams_client = await connect_with(client, MemoryStreams())
    async with new_worker(streams_client, Waits) as worker:
        workflow_id = await running_owner(streams_client, worker.task_queue)
        handle = get_stream_handle(streams_client, workflow_id, topic=TOKENS)
        await handle.producer(producer_id="writer", attempt=1).append("a")
        failure = ApplicationError("the agent gave up", type="AgentGaveUp")
        await handle.close(error=failure)
        if await _server_has_notifier(streams_client, workflow_id):
            notifier = await _describe_notifier(streams_client, workflow_id)
            assert notifier.closed
            assert notifier.close_failure.message == "the agent gave up"
            assert notifier.close_failure.application_failure_info.type == "AgentGaveUp"
        # The read raises the close's error after the last record.
        records = handle.read()
        assert (await records.__anext__()).value == "a"
        with pytest.raises(ApplicationError) as raised:
            await records.__anext__()
        assert raised.value.type == "AgentGaveUp"
        await streams_client.get_workflow_handle(workflow_id).signal(Waits.finish)


async def _describe_notifier(
    client: Client, workflow_id: str
) -> DescribeStreamNotifierResponse:
    """The notifier of the owner's ``tokens`` stream."""
    described = await client.get_workflow_handle(workflow_id).describe()
    request = DescribeStreamNotifierRequest(
        namespace=client.namespace,
        stream_ref=StreamReference(
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
            workflow_id=workflow_id,
            run_id=described.raw_description.workflow_execution_info.first_run_id,
            topic=TOKENS.name,
        ),
    )
    return await describe_stream_notifier(client, request)


async def _server_has_notifier(client: Client, workflow_id: str) -> bool:
    try:
        await _describe_notifier(client, workflow_id)
    except StreamUnsupportedError:
        return False
    except StreamRefusedError:
        pass
    return True
