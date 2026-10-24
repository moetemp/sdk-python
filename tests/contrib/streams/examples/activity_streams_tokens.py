"""Brook API proposal, "An Activity streams tokens to the Workflow's Stream".

An async Activity publishes each token with an awaited ``publish``, which returns the offset,
and batches the rest with ``BufferedPublisher``. A sync Activity takes ``SyncActivityStream``,
which works in a thread pool executor.
"""

from __future__ import annotations

import concurrent.futures
import uuid
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import streams
from temporalio.worker import Worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


@dataclass
class CallModelInput:
    prompt: str
    stream: streams.ActivityStream[Events]


@dataclass
class SummarizeInput:
    text: str
    stream: streams.SyncActivityStream[Events]


@activity.defn
async def call_model(input: CallModelInput) -> int:
    tokens = input.stream.topic(Events.tokens)
    words = input.prompt.split()
    # Each awaited publish has written when it returns.
    last = await tokens.publish(words[0])
    # A fast model sends many small tokens, so the helper batches them.
    async with streams.BufferedPublisher(
        tokens, max_delay=timedelta(milliseconds=100)
    ) as fast:
        for word in words[1:]:
            fast.publish(word)
    return last


@activity.defn
def summarize(input: SummarizeInput) -> int:
    return input.stream.topic(Events.tokens).publish(f"summary: {input.text}")


@workflow.defn
class Model:
    @workflow.run
    async def run(self, prompt: str) -> list[int]:
        stream = await workflow.create_stream(Events)
        first = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, streams.ActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(minutes=5),
        )
        summary = await workflow.execute_activity(
            summarize,
            SummarizeInput(prompt, streams.SyncActivityStream.from_handle(stream)),
            start_to_close_timeout=timedelta(minutes=5),
        )
        await stream.close()
        return [first, summary]


async def main(
    client: Client, task_queue: str
) -> tuple[list[int], list[streams.Record[str]]]:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[Model],
            activities=[call_model, summarize],
            activity_executor=executor,
        ):
            handle = await client.start_workflow(
                Model.run,
                "one two three",
                id=f"model-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            offsets = await handle.result()
    stream = handle.stream(Events)
    records = [
        item async for item in stream.subscribe() if isinstance(item, streams.Record)
    ]
    return offsets, records
