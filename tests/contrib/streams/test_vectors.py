from pathlib import Path
from typing import Any

from temporalio.streams import _ids
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
