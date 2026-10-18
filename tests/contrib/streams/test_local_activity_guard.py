"""The stream Local Activities refuse to run as normal Activities and carry a summary.

The store plugin registers the read and close Activities on every Worker, so
Workflow code can name them in a normal Activity call. Each refuses that call
for good. Each Local Activity call sets a summary, which History shows on its
marker, since every marker of a kind has the same Activity type.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.bridge.proto.streams import ReadResponse
from temporalio.client import Client, WorkflowHistory
from temporalio.common import RawValue, RetryPolicy
from temporalio.contrib.streams import close_workflow_stream, workflow_reader
from temporalio.contrib.streams._workflow import READ_ACTIVITY, _ReadInput
from temporalio.contrib.streams._workflow_follow import FOLLOW_ACTIVITY, _FollowInput
from temporalio.contrib.streams._workflow_writer import CLOSE_ACTIVITY
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.converter import DataConverter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import Replayer
from tests.contrib.streams import test_workflow_reader as reader_tests
from tests.contrib.streams._support import connect_with, new_workflow_id
from tests.contrib.streams.test_workflow_reader import (
    TOKENS,
    FakeReads,
    data,
    fake_read,
)
from tests.helpers import new_worker

_HISTORIES = Path(__file__).parent / "histories"


@workflow.defn
class CallsAsNormalActivity:
    @workflow.run
    async def run(self, which: str) -> list[Any]:
        if which == "read":
            args: list[Any] = [
                _ReadInput(
                    workflow_id="owner",
                    chain_run_id="",
                    topic="tokens",
                    after="",
                    state="",
                    wait_ms=100,
                    max_records=10,
                )
            ]
            name = READ_ACTIVITY
        elif which == "follow":
            args = [
                _FollowInput(
                    workflow_id="owner",
                    chain_run_id="",
                    topic="tokens",
                    after="",
                    state="",
                    max_depth=4,
                    max_open=16,
                )
            ]
            name = FOLLOW_ACTIVITY
        else:
            args = [TOKENS.name, False, RawValue(Payload())]
            name = CLOSE_ACTIVITY
        try:
            await workflow.execute_activity(
                name,
                args=args,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=0),
            )
        except ActivityError as error:
            cause = error.cause
            assert isinstance(cause, ApplicationError)
            return [cause.type, cause.non_retryable, str(cause)]
        return ["ran"]


@pytest.mark.parametrize("which", ["read", "close", "follow"])
async def test_a_normal_activity_call_fails_for_good(client: Client, which: str):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, CallsAsNormalActivity) as worker:
        said = await streams.execute_workflow(
            CallsAsNormalActivity.run,
            which,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
    assert said[:2] == ["StreamUnsupportedError", True]
    assert "only as the Local Activity" in said[2]


@workflow.defn
class ReadsTwoBatches:
    @workflow.run
    async def run(self) -> list[str]:
        values: list[str] = []
        reader = workflow_reader("owner", topic=TOKENS, replay_from_store=False)
        async for batch in reader:
            values.extend(r.value for r in batch.records if r.value is not None)
        return values


@workflow.defn
class ClosesWithAResult:
    @workflow.run
    async def run(self) -> None:
        await close_workflow_stream(TOKENS, result=2)


def _summaries(history: WorkflowHistory) -> list[str]:
    converter = DataConverter.default.payload_converter
    return [
        converter.from_payloads([event.user_metadata.summary])[0]
        for event in history.events
        if event.event_type == EventType.EVENT_TYPE_MARKER_RECORDED
    ]


async def _read_two_batches(client: Client) -> WorkflowHistory:
    reader_tests._fake = FakeReads(
        [
            ReadResponse(records=[data("c1", "a")], cursor="c1"),
            ReadResponse(records=[data("c2", "b")], cursor="c2", done=True),
        ]
    )
    async with new_worker(client, ReadsTwoBatches, activities=[fake_read]) as worker:
        handle = await client.start_workflow(
            ReadsTwoBatches.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
        assert await handle.result() == ["a", "b"]
    return await handle.fetch_history()


async def test_each_read_marker_says_what_it_read(client: Client):
    history = await _read_two_batches(client)
    assert _summaries(history) == [
        "stream read tokens after the start",
        "stream read tokens after c1",
    ]


async def test_the_close_marker_says_how_it_closed(client: Client):
    streams = await connect_with(client, MemoryStreams())
    async with new_worker(streams, ClosesWithAResult) as worker:
        handle = await streams.start_workflow(
            ClosesWithAResult.run, id=new_workflow_id(), task_queue=worker.task_queue
        )
        await handle.result()
    assert _summaries(await handle.fetch_history()) == [
        "stream close tokens with a result"
    ]


@pytest.mark.parametrize(
    "workflow_class", [ReadsTwoBatches, ClosesWithAResult], ids=lambda c: c.__name__
)
async def test_a_history_from_before_the_summaries_replays(workflow_class: type):
    # Recorded with the earlier summaries, which replay doesn't compare.
    path = _HISTORIES / f"{workflow_class.__name__}.json"
    history = WorkflowHistory.from_json("recorded", json.loads(path.read_text()))
    await Replayer(
        workflows=[workflow_class], plugins=[MemoryStreams()]
    ).replay_workflow(history)
