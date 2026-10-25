from pathlib import Path
from typing import Any

import pytest

import temporalio.api.streamservice.v1 as stream_pb
from temporalio.common.streams import _ids, _items
from temporalio.common.streams.system import models
from tests.contrib.streams._support import crate_file, vectors

_KINDS = {
    "workflow": stream_pb.ProducerKind.PRODUCER_KIND_WORKFLOW,
    "local_activity": stream_pb.ProducerKind.PRODUCER_KIND_LOCAL_ACTIVITY,
    "activity": stream_pb.ProducerKind.PRODUCER_KIND_ACTIVITY,
    "client": stream_pb.ProducerKind.PRODUCER_KIND_CLIENT,
    "standalone_activity": stream_pb.ProducerKind.PRODUCER_KIND_STANDALONE_ACTIVITY,
}

_SCRIPTS = Path(__file__).parents[3] / "scripts" / "_proto"


@pytest.mark.parametrize(
    "relative",
    [
        "temporal/api/streamservice/v1/stream_service.proto",
        "temporal/sdk/streams/v1/extension.proto",
    ],
)
def test_vendored_protos_match_the_crate(relative: str):
    crate = crate_file(f"protos/{relative}").read_text(encoding="utf-8")
    assert (_SCRIPTS / relative).read_text(encoding="utf-8") == crate


def test_stream_ids_match_the_crate():
    data: Any = vectors("ids.json")
    for case in data["streams"]:
        # Every Stream has a name, so the crate's cases with no name don't apply.
        if case["name"] is None:
            continue
        built = _ids.scoped_stream_id(
            case["workflow_id"], case["first_run_id"], case["name"]
        )
        assert built == case["stream_id"]


def test_producer_ids_match_the_crate():
    producers: Any = vectors("ids.json")
    producers = producers["producers"]
    assert (
        _ids.workflow_producer_id(producers["workflow"]["run_id"])
        == (producers["workflow"]["id"])
    )
    la = producers["local_activity"]
    assert _ids.local_activity_producer_id(la["run_id"], la["call_key"]) == la["id"]
    act = producers["activity"]
    assert (
        _ids.activity_producer_id(
            act["run_id"], act["activity_id"], act["schedule_key"]
        )
        == act["id"]
    )
    sa = producers["standalone_activity"]
    assert (
        _ids.standalone_activity_producer_id(sa["activity_id"], sa["activity_run_id"])
        == sa["id"]
    )
    client = producers["client"]
    assert (
        _ids.client_producer_id(client["identity"], client["process_key"])
        == client["id"]
    )
    named = producers["named_client"]
    assert _ids.named_client_producer_id(named["name"]) == named["id"]


def _time(value: list[int] | None) -> models.Timestamp | None:
    return models.Timestamp(seconds=value[0], nanos=value[1]) if value else None


def _epoch(value: dict[str, Any] | None) -> models.ProducerEpoch | None:
    if value is None:
        return None
    return models.ProducerEpoch(
        task_started_event_id=value.get("task_started_event_id"),
        task_attempt=value.get("task_attempt"),
        task_started_time=_time(value.get("task_started_time")),
        attempt_scheduled_time=_time(value.get("attempt_scheduled_time")),
        attempt=value.get("attempt"),
    )


def test_withdrawals_match_the_crate():
    cases: Any = vectors("withdrawals.json")
    assert cases
    for case in cases:
        record = case["record"]["producer"]
        producer = models.Producer(
            kind=_KINDS[record["kind"]],
            id=record["id"],
            run_id=record.get("run_id"),
            epoch=_epoch(record.get("epoch")),
        )
        given = case["supersession"]
        completed = given.get("previous_completed_task")
        supersession = models.Supersession(
            producer_kind=_KINDS[given["producer_kind"]],
            producer_id=given["producer_id"],
            epoch=_epoch(given.get("epoch")),
            previous_completed_task=models.WorkflowTaskEpoch(
                started_event_id=completed.get("started_event_id"),
                attempt=completed.get("attempt"),
                started_time=_time(completed.get("started_time")),
            )
            if completed
            else None,
            failed_tasks=given.get("failed_tasks", []),
            withdraw_run=given.get("withdraw_run"),
            run_id=given.get("run_id"),
        )
        assert _items.withdraws(supersession, producer) == case["withdraws"], case[
            "name"
        ]
