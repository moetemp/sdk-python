"""Jetty v2, "A Workflow reads another Workflow's Stream".

An orchestrator Workflow follows the turns of an agent Workflow. It gets the agent's Stream as
an argument and reads it with ``subscribe``, an async iterator in Workflow code. Each page is in
History, so a replay reads the same items. On a native Stream a read at the end waits on the
server. On Redis it waits on a Local Activity, then on a durable timer. The Workflow code is
the same on both.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

from temporalio import common, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@common.defn(name="events")
class Events:
    tokens = common.topic(str)


@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        stream = await workflow.create_stream(Events)
        for word in prompt.split():
            # Each word goes out in its own Workflow Task, so the follower waits for some.
            await workflow.sleep(timedelta(milliseconds=200))
            await stream.topic(Events.tokens).publish(word)
        # The server would close a native Stream when the run chain ends. A Redis Stream
        # stays open, so the agent closes it with its answer.
        answer = prompt.upper()
        await stream.close(result=answer)
        return answer


@workflow.defn
class Follower:
    @workflow.run
    async def run(self, agent: common.WorkflowStream[Events]) -> list[str]:
        seen = []
        async for item in agent.subscribe(create_wait=timedelta(seconds=30)):
            if isinstance(item, common.Record):
                seen.append(item.data)
            elif isinstance(item, common.Closed):
                seen.append(f"agent said {item.result}")
        return seen


async def main(client: Client, task_queue: str) -> list[str]:
    async with Worker(client, task_queue=task_queue, workflows=[Agent, Follower]):
        agent = await client.start_workflow(
            Agent.run,
            "find the answer",
            id=f"agent-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        # The handle has the first run id, so the client builds the Stream with no call.
        stream = agent.stream(Events)
        return await client.execute_workflow(
            Follower.run,
            common.WorkflowStream.from_handle(stream),
            id=f"follower-{uuid.uuid4()}",
            task_queue=task_queue,
        )
