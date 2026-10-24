import uuid
from datetime import timedelta

import pydantic

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import streams
from temporalio.common.streams import workflow as stream_workflow
from temporalio.common.streams.testing import MemoryStreamPlugin
from temporalio.contrib.pydantic import pydantic_data_converter
from tests.helpers import new_worker


@streams.defn(name="events")
class Events:
    tokens = streams.topic(str)


class CallModelInput(pydantic.BaseModel):
    prompt: str
    stream: streams.ActivityStream[Events]


class TurnResult(pydantic.BaseModel):
    text: str
    stream: streams.ClientStream[Events]


@activity.defn
async def call_model(input: CallModelInput) -> int:
    assert input.stream.definition is Events
    return await input.stream.topic(Events.tokens).publish(*input.prompt.split())


@workflow.defn
class TurnWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> TurnResult:
        stream = await stream_workflow.create_stream(Events)
        await workflow.execute_activity(
            call_model,
            CallModelInput(
                prompt=prompt, stream=streams.ActivityStream.from_handle(stream)
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )
        return TurnResult(
            text=prompt.upper(), stream=streams.ClientStream.from_handle(stream)
        )


async def test_handles_in_models_round_trip_on_the_memory_store(client: Client):
    config = client.config()
    config["plugins"] = [MemoryStreamPlugin()]
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
    assert result.stream.workflow_id == handle.id
    assert result.stream.first_execution_run_id == handle.first_execution_run_id
    assert result.stream.backend == "memory"
    stream = streams.StreamClient(client).get_handle(Events, id=result.stream.id)
    items = [item async for item in stream.subscribe()]
    assert [i.data for i in items if isinstance(i, streams.Record)] == ["hi", "there"]
    assert isinstance(items[-1], streams.Closed) and items[-1].result is not None
