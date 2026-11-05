from pathlib import Path
from typing import Any

from temporalio.streams import _ids, _items
from temporalio.streams.system import models
from tests.contrib.streams._support import crate_file, vectors

_SCRIPTS = Path(__file__).parents[3] / "scripts" / "_proto"


def test_vendored_redis_proto_matches_the_crate():
    # The other Stream protos come from the Core submodule. The Redis config comes from the
    # Redis crate, so the SDK keeps a copy.
    relative = "temporal/sdk/streams/v1/redis.proto"
    crate = crate_file(f"protos/{relative}", "temporalio-streams-redis")
    expected = crate.read_text(encoding="utf-8")
    assert (_SCRIPTS / relative).read_text(encoding="utf-8") == expected


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


def _task(value: dict[str, Any], prefix: str = "") -> models.WorkflowTaskEpoch:
    return models.WorkflowTaskEpoch(
        started_event_id=value.get(f"{prefix}started_event_id"),
        attempt=value.get(f"{prefix}attempt"),
        started_time=_time(value.get(f"{prefix}started_time")),
    )


def _epoch(kind: str, value: dict[str, Any] | None) -> models.ProducerEpoch | None:
    # The vectors keep the flat fields of every kind, so the case comes from the kind.
    if value is None:
        return None
    activity = models.ActivityEpoch(
        attempt_scheduled_time=_time(value.get("attempt_scheduled_time")),
        attempt=value.get("attempt"),
    )
    cases: dict[str, models.ProducerEpochKind] = {
        "workflow": models.ProducerEpochKindWorkflow(_task(value, "task_")),
        "local_activity": models.ProducerEpochKindLocalActivity(
            models.LocalActivityEpoch(
                task=_task(value, "task_"), attempt=value.get("attempt")
            )
        ),
        "activity": models.ProducerEpochKindActivity(activity),
        "standalone_activity": models.ProducerEpochKindStandaloneActivity(activity),
        "client": models.ProducerEpochKindClient(
            models.ClientEpoch(attempt=value.get("attempt"))
        ),
    }
    return models.ProducerEpoch(kind=cases[kind])


def test_withdrawals_match_the_crate():
    cases: Any = vectors("withdrawals.json")
    assert cases
    for case in cases:
        record = case["record"]["producer"]
        producer = models.Producer(
            id=record["id"],
            run_id=record.get("run_id"),
            epoch=_epoch(record["kind"], record.get("epoch")),
        )
        given = case["superseded"]
        completed = given.get("previous_completed_task")
        writer = models.StreamWriter(
            producer=models.Producer(
                id=given["producer_id"],
                epoch=_epoch(given["producer_kind"], given.get("epoch")),
                run_id=given.get("run_id"),
            ),
            previous_completed_task=_task(completed) if completed else None,
            failed_tasks=given.get("failed_tasks", []),
            withdraw_run=given.get("withdraw_run"),
        )
        facts = models.StreamSuperseded(writer=writer)
        assert _items.withdraws(facts, producer) == case["withdraws"], case["name"]


def test_epoch_order_matches_the_crate():
    cases: Any = vectors("epochs.json")
    keys: list[tuple[tuple[Any, ...], bytes]] = []
    for case in cases:
        epoch = _epoch(case["kind"], case["epoch"]) if case["kind"] != "empty" else None
        python = _items._epoch_key(epoch or models.ProducerEpoch())
        keys.append((python, bytes.fromhex(case["key"])))
    assert len(keys) > 1
    # A byte compare of the crate's keys orders the epochs, so the Python keys compare the same.
    for python_a, crate_a in keys:
        for python_b, crate_b in keys:
            assert (python_a < python_b) == (crate_a < crate_b)
            assert (python_a == python_b) == (crate_a == crate_b)
