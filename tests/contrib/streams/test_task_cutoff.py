"""A Workflow reader cuts its Workflow Task after N commands or a time limit.

The cut sets ``heartbeat_task`` on the activation completion, so Core
completes the task with the commands and markers so far while the Local
Activities run on.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

import tests.contrib.streams.test_workflow_reader as reader_tests
from temporalio import activity, workflow
from temporalio.api.enums.v1 import EventType
from temporalio.bridge.proto.streams import ReadResponse
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.streams import TaskCutoff, topic, workflow_reader
from temporalio.worker import Replayer
from tests.contrib.streams._support import new_workflow_id
from tests.contrib.streams.test_workflow_reader import FakeReads, data, fake_read
from tests.helpers import new_worker

TOKENS = topic("tokens", str)

_started = asyncio.Event()
_release = asyncio.Event()


@activity.defn
async def held() -> None:
    _started.set()
    await _release.wait()


@workflow.defn
class HeartbeatsWhileHeld:
    @workflow.run
    async def run(self, heartbeat: bool) -> None:
        if heartbeat:
            runtime: Any = workflow._Runtime.current()
            runtime._request_task_heartbeat()
        await workflow.execute_local_activity(
            held, start_to_close_timeout=timedelta(seconds=30)
        )


async def _completed_tasks(handle: WorkflowHandle) -> int:
    history = await handle.fetch_history()
    return sum(
        e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
        for e in history.events
    )


@pytest.mark.parametrize("heartbeat", [True, False])
async def test_a_task_heartbeat_completes_the_task_while_a_local_activity_runs(
    client: Client, heartbeat: bool
):
    _started.clear()
    _release.clear()
    async with new_worker(client, HeartbeatsWhileHeld, activities=[held]) as worker:
        handle = await client.start_workflow(
            HeartbeatsWhileHeld.run,
            heartbeat,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        await asyncio.wait_for(_started.wait(), 10)
        # Without the request, the task stays open until the Local Activity
        # heartbeat, which is seconds away.
        await asyncio.sleep(1)
        assert await _completed_tasks(handle) == (1 if heartbeat else 0)
        _release.set()
        await handle.result()
    await Replayer(workflows=[HeartbeatsWhileHeld]).replay_workflow(
        await handle.fetch_history()
    )


@workflow.defn
class ReadsWithACutoff:
    @workflow.run
    async def run(self, commands: int) -> int:
        cutoff = TaskCutoff(commands=commands) if commands else None
        batches = 0
        async for _ in workflow_reader("owner", topic=TOKENS, cutoff=cutoff):
            batches += 1
        return batches


async def _read_with(client: Client, commands: int) -> int:
    answers: list[ReadResponse | BaseException] = [
        ReadResponse(records=[data(f"c{n}", str(n))], cursor=f"c{n}") for n in range(5)
    ]
    answers.append(ReadResponse(cursor="c4", done=True))
    reader_tests._fake = FakeReads(answers)
    async with new_worker(client, ReadsWithACutoff, activities=[fake_read]) as worker:
        handle = await client.start_workflow(
            ReadsWithACutoff.run,
            commands,
            id=new_workflow_id(),
            task_queue=worker.task_queue,
        )
        assert await handle.result() == 5
    await Replayer(workflows=[ReadsWithACutoff]).replay_workflow(
        await handle.fetch_history()
    )
    return await _completed_tasks(handle)


async def test_a_busy_reader_cuts_its_task_at_the_command_bound(client: Client):
    uncut = await _read_with(client, 0)
    cut = await _read_with(client, 1)
    # Uncut, every read lands in the first task. Cut, each read past the
    # first command asks for its own task.
    assert uncut == 1
    assert cut > uncut


def test_the_cutoff_defaults_are_100_commands_and_2_seconds() -> None:
    assert TaskCutoff() == TaskCutoff(commands=100, time=timedelta(seconds=2))
