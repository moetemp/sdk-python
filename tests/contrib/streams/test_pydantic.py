import json
import uuid

import pydantic

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib import streams
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.streams.testing import MemoryStreamPlugin
from tests.helpers import new_worker


class Turn(pydantic.BaseModel):
    text: str
    stream: streams.StreamRef


OWNED = streams.StreamRef("wf:run", workflow_id="wf", first_execution_run_id="run")


def test_model_dumps_the_proto_json():
    dumped = json.loads(Turn(text="hi", stream=OWNED).model_dump_json())
    assert dumped["stream"] == {
        "id": "wf:run",
        "workflowId": "wf",
        "firstExecutionRunId": "run",
    }
    plain = json.loads(Turn(text="hi", stream=streams.StreamRef("s")).model_dump_json())
    assert plain["stream"] == {"id": "s"}


def test_model_loads_proto_and_snake_case_names():
    for stream in (
        {"id": "wf:run", "workflowId": "wf", "firstExecutionRunId": "run"},
        {"id": "wf:run", "workflow_id": "wf", "first_execution_run_id": "run"},
    ):
        loaded = Turn.model_validate_json(json.dumps({"text": "hi", "stream": stream}))
        assert loaded.stream == OWNED


def test_payload_converter_writes_the_proto_json():
    converter = pydantic_data_converter.payload_converter
    payload = converter.to_payload(Turn(text="hi", stream=OWNED))
    assert json.loads(payload.data)["stream"]["workflowId"] == "wf"
    assert converter.from_payload(payload, Turn) == Turn(text="hi", stream=OWNED)


@workflow.defn
class TurnWorkflow:
    @workflow.run
    async def run(self, turn: Turn) -> Turn:
        scoped = streams.create_workflow_stream("turn")
        scoped.topic("t", type=str).publish(turn.text)
        return Turn(text=turn.text.upper(), stream=scoped.ref)


async def test_workflow_round_trip_on_the_memory_store(client: Client):
    config = client.config()
    config["plugins"] = [MemoryStreamPlugin()]
    config["data_converter"] = pydantic_data_converter
    client = Client(**config)
    async with new_worker(client, TurnWorkflow) as worker:
        handle = await client.start_workflow(
            TurnWorkflow.run,
            Turn(text="hi", stream=streams.StreamRef("input")),
            id=f"wf-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        result = await handle.result()
        assert result.text == "HI"
        assert result.stream.workflow_id == handle.id
        assert result.stream.first_execution_run_id == handle.first_execution_run_id
        stream = streams.for_ref(client, result.stream, end_with_run=True)
        items = [item async for item in stream.subscribe(type=str)]
    assert [i.data for i in items if isinstance(i, streams.Record)] == ["hi"]
