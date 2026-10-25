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

from temporalio import activity, common, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@common.defn(name="tokens")
class Tokens(common.StreamDefinition[str]): ...


@dataclass
class CallModelInput:
    prompt: str
    tokens: common.ActivityStream[str]  # the Stream that the Workflow passes


@dataclass
class SummarizeInput:
    text: str
    tokens: common.SyncActivityStream[str]


@activity.defn
async def call_model(input: CallModelInput) -> int:
    words = input.prompt.split()
    # Each awaited publish has written its Record when it returns.
    last = await input.tokens.publish(words[0])
    # A fast model sends many small tokens, so the helper batches them.
    async with common.BufferedPublisher(
        input.tokens, max_delay=timedelta(milliseconds=100)
    ) as fast:
        for word in words[1:]:
            fast.publish(word)
    return last


@activity.defn
def summarize(input: SummarizeInput) -> int:
    return input.tokens.publish(f"summary: {input.text}")


@workflow.defn
class Model:
    @workflow.query
    def stream(self) -> common.WorkflowStream[str]:
        return workflow.stream_handle(Tokens)

    @workflow.run
    async def run(self, prompt: str) -> list[int]:
        tokens = await workflow.create_stream(Tokens)
        first = await workflow.execute_activity(
            call_model,
            CallModelInput(prompt, common.ActivityStream.from_handle(tokens)),
            start_to_close_timeout=timedelta(minutes=5),
        )
        summary = await workflow.execute_activity(
            summarize,
            SummarizeInput(prompt, common.SyncActivityStream.from_handle(tokens)),
            start_to_close_timeout=timedelta(minutes=5),
        )
        await tokens.close()
        return [first, summary]


async def main(
    client: Client, task_queue: str
) -> tuple[list[int], list[common.Record[str]]]:
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
            stream = await handle.query("stream", result_type=common.ClientStream[str])
    records = [r async for r in stream.subscribe() if isinstance(r, common.Record)]
    return offsets, records
