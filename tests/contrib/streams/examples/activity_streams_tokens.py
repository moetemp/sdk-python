"""Streams API proposal, "An Activity streams tokens to the Workflow's Stream".

The Workflow passes its Stream's ref to the Activity as a field of the input. An async Activity
gets the Stream from ``activity.client()``, so it writes as the attempt, and batches the model's
deltas with ``BufferedAppender``. A sync Activity has no ``activity.client()``, so it keeps the
Worker's client. It wraps its Stream in ``SyncStream``, whose calls block its thread, so it works
in a thread pool executor.
"""

from __future__ import annotations

import concurrent.futures
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, streams, workflow
from temporalio.client import BufferedAppender, Client, SyncStream
from temporalio.worker import Worker

Message = str


@dataclass
class Reply:
    text: str


class FakeResponse:
    """A model's streamed response, one delta for each word."""

    def __init__(self, messages: list[Message]) -> None:
        self.words = messages[-1].split()

    async def __aiter__(self) -> AsyncIterator[str]:
        for word in self.words:
            yield word

    def reply(self) -> Reply:
        return Reply(" ".join(self.words).upper())


class FakeLLM:
    """Stands in for a model client."""

    def stream(self, messages: list[Message]) -> FakeResponse:
        return FakeResponse(messages)


llm = FakeLLM()


@dataclass
class CallModelInput:
    messages: list[Message]
    tokens: streams.StreamRef[str]  # the ref that the Workflow passes


@dataclass
class SummarizeInput:
    text: str
    tokens: streams.StreamRef[str]


@activity.defn
async def call_model(input: CallModelInput) -> Reply:
    stream = activity.client().get_stream(input.tokens)  # writes as this attempt
    response = llm.stream(input.messages)
    async with BufferedAppender(stream) as tokens:  # batches deltas
        async for delta in response:
            tokens.append(delta)
    return response.reply()


class Summarizer:
    def __init__(self, client: Client) -> None:
        self._client = client  # the Worker's client

    @activity.defn
    def summarize(self, input: SummarizeInput) -> str:
        tokens = SyncStream(self._client.get_stream(input.tokens))
        # The sync append blocks until the Record is written, and returns its offset.
        return str(tokens.append(f"summary: {input.text}"))


@workflow.defn
class Model:
    def __init__(self) -> None:
        self._tokens: workflow.WorkflowStream[str] | None = None

    @workflow.query
    def stream(self) -> streams.StreamRef[str] | None:
        return self._tokens.ref if self._tokens else None

    @workflow.run
    async def run(self, prompt: str) -> str:
        self._tokens = tokens = await workflow.create_stream(
            f"{workflow.info().workflow_id}-tokens", payload_type=str
        )
        reply = await workflow.execute_activity(
            call_model,
            CallModelInput([prompt], tokens.ref),
            start_to_close_timeout=timedelta(minutes=5),
        )
        summary = await workflow.execute_activity_method(
            Summarizer.summarize,
            SummarizeInput(reply.text, tokens.ref),
            start_to_close_timeout=timedelta(minutes=5),
        )
        await tokens.close()
        return summary


async def main(
    client: Client, task_queue: str
) -> tuple[str, list[streams.Record[str]]]:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        async with Worker(
            client,
            task_queue=task_queue,
            workflows=[Model],
            activities=[call_model, Summarizer(client).summarize],
            activity_executor=executor,
        ):
            handle = await client.start_workflow(
                Model.run,
                "one two three",
                id=f"model-{uuid.uuid4()}",
                task_queue=task_queue,
            )
            summary = await handle.result()
            ref = await handle.query(Model.stream)
    assert ref is not None
    stream = client.get_stream(ref)
    records = [r async for r in stream.read() if isinstance(r, streams.Record)]
    return summary, records
