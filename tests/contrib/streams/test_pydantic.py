import uuid
from datetime import timedelta

import pydantic

from temporalio import activity, streams, workflow
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.streams.system import models
from tests.contrib.streams._support import stream_status
from tests.helpers import new_worker


class CallModelInput(pydantic.BaseModel):
    prompt: str
    stream: streams.StreamRef[str]


class TurnResult(pydantic.BaseModel):
    text: str
    stream: streams.StreamRef[str]


@activity.defn
async def call_model(input: CallModelInput) -> str:
    stream = activity.client().get_stream(input.stream)
    return str(await stream.append(*input.prompt.split()))


@workflow.defn
class TurnWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> TurnResult:
        stream = await workflow.create_stream(
            f"{workflow.info().workflow_id}-tokens", payload_type=str
        )
        await workflow.execute_activity(
            call_model,
            CallModelInput(prompt=prompt, stream=stream.ref),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.close()
        return TurnResult(text=prompt.upper(), stream=stream.ref)


async def test_refs_in_models_round_trip_on_the_memory_store(client: Client):
    config = client.config()
    config["stream_config"] = streams.StreamConfig(backends=[MemoryStreamBackend()])
    config["data_converter"] = pydantic_data_converter
    client = Client(**config)
    async with new_worker(client, TurnWorkflow, activities=[call_model]) as worker:
        handle = await client.start_workflow(
            TurnWorkflow.run,
            "hi there",
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        result = await handle.result()
    assert result.text == "HI THERE"
    assert result.stream.id == f"{handle.id}-tokens"
    assert result.stream.backend == "memory"
    stream = client.get_stream(result.stream)
    items = [item async for item in stream.read()]
    assert [i.data for i in items if isinstance(i, streams.Record)] == ["hi", "there"]
    status = await stream_status(client, result.stream.id)
    assert status == models.StreamStatus.StreamStatusClosed
