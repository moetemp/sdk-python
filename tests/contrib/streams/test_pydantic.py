import uuid
from datetime import timedelta

import pydantic

from temporalio import activity, common, workflow
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.stream.testing import MemoryStreamBackend
from tests.helpers import new_worker


@common.defn(name="tokens")
class Tokens(common.StreamDefinition[str]): ...


class CallModelInput(pydantic.BaseModel):
    prompt: str
    stream: common.ActivityStream[str]


class TurnResult(pydantic.BaseModel):
    text: str
    stream: common.ClientStream[str]


@activity.defn
async def call_model(input: CallModelInput) -> int:
    assert input.stream._payload_type is str
    return await input.stream.publish(*input.prompt.split())


@workflow.defn
class TurnWorkflow:
    @workflow.run
    async def run(self, prompt: str) -> TurnResult:
        stream = await workflow.create_stream(Tokens)
        await workflow.execute_activity(
            call_model,
            CallModelInput(
                prompt=prompt, stream=common.ActivityStream.from_handle(stream)
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )
        await stream.close(result=prompt.upper())
        return TurnResult(
            text=prompt.upper(), stream=common.ClientStream.from_handle(stream)
        )


async def test_handles_in_models_round_trip_on_the_memory_store(client: Client):
    config = client.config()
    config["stream_backends"] = [MemoryStreamBackend(default=True)]
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
    stream = client.stream(Tokens, id=result.stream.id)
    items = [item async for item in stream.subscribe()]
    assert [i.data for i in items if isinstance(i, common.Record)] == ["hi", "there"]
    assert isinstance(items[-1], common.Closed) and items[-1].result is not None
