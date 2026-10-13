#!/usr/bin/env bash
# Runs the streams-over-Nexus live tests against a server built from the stream notifier branch.
#
# The stock dev server has no stream notifier and no Nexus progress, so the lifecycle tests skip
# there. This lane builds a server that has both, starts it with the settings the guide lists,
# and fails unless every gated test below passes. Run it from a built sdk-python checkout.
#
# Environment:
#   SERVER_REPO        git URL of the server (default: the moetemp fork)
#   SERVER_REF         commit to build (default: the notifier frontend commit the release names)
#   SERVER_BIN         use this server binary instead of building one
#   PORT_BASE          frontend gRPC port, the other ports follow it (default: 7881)
#   STREAMS_REDIS_URL  Redis to use. Without it, the script starts a scratch redis-server.
#   CARGO_TARGET_DIR   where Core's live test builds (default: under WORK_DIR)
#   WORK_DIR           where to build and keep logs (default: a new temporary directory)
set -euo pipefail

SERVER_REPO=${SERVER_REPO:-https://github.com/moetemp/temporal.git}
SERVER_REF=${SERVER_REF:-760276f591a6d640230ae8ea805e123f2de64c4b}
PORT_BASE=${PORT_BASE:-7881}
WORK_DIR=${WORK_DIR:-$(mktemp -d -t streams-live-lane)}
SDK_DIR=$(cd "$(dirname "$0")/.." && pwd)

GRPC_PORT=$PORT_BASE
HTTP_PORT=$((PORT_BASE + 1))
REDIS_PORT=$((PORT_BASE + 9))
pids=()

cleanup() {
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

mkdir -p "$WORK_DIR/data"
echo "work dir: $WORK_DIR"

if [[ -z "${SERVER_BIN:-}" ]]; then
  echo "building the server from $SERVER_REPO at $SERVER_REF"
  # By commit, so two green runs test the same server.
  git init --quiet "$WORK_DIR/temporal"
  git -C "$WORK_DIR/temporal" fetch --quiet --depth 1 "$SERVER_REPO" "$SERVER_REF"
  git -C "$WORK_DIR/temporal" checkout --quiet FETCH_HEAD
  (cd "$WORK_DIR/temporal" && go build -o "$WORK_DIR/temporal-server" ./cmd/server)
  SERVER_BIN="$WORK_DIR/temporal-server"
fi

cat >"$WORK_DIR/dc.yaml" <<EOF
history.enableChasm:
  - value: true
history.enableCHASMCallbacks:
  - value: true
history.enableCHASMSignalBacklinks:
  - value: true
nexusoperation.enableChasmWorkflowOperations:
  - value: true
nexusoperation.chasmWorkflowOperationsRolloutPercent:
  - value: 100
nexusoperation.enableProgress:
  - value: true
nexusoperation.callback.endpoint.template:
  - value: "http://localhost:$HTTP_PORT/namespaces/{{.NamespaceName}}/nexus/callback"
callback.allowedAddresses:
  - value:
      - Pattern: "localhost:$HTTP_PORT"
        AllowInsecure: true
      - Pattern: "127.0.0.1:$HTTP_PORT"
        AllowInsecure: true
      # Core's live notifier test takes the notifier's callbacks on this address.
      - Pattern: "127.0.0.1:7899"
        AllowInsecure: true
system.forceSearchAttributesCacheRefreshOnRead:
  - value: true
streamnotifier.enabled:
  - value: true
# The owner check closes a stopped owner's stream by its close status, so the
# terminated and timed-out cases finish in seconds, not minutes.
streamnotifier.ownerCheckInterval:
  - value: 2s
# A completed, failed or canceled owner is left alone this long after it
# closed, so the Worker's close with the real result wins.
streamnotifier.ownerCloseGrace:
  - value: 10s
history.enableSignalWithStartFromWorkflow:
  - value: true
# A follower is a standalone operation with a callback to a Worker's task queue.
nexusoperation.enableStandalone:
  - value: true
nexusoperation.enabledCallbackKinds:
  - value:
      - nexus
      - nexusHandler
EOF

sqlite_store() {
  cat <<EOF
      sql:
        pluginName: "sqlite"
        databaseName: "$WORK_DIR/data/temporal.db"
        connectAddr: "localhost"
        connectProtocol: "tcp"
        connectAttributes:
          setup: true
          journal_mode: wal
          synchronous: 2
          cache: "private"
        maxConns: 1
        maxIdleConns: 1
EOF
}

service() {
  cat <<EOF
  $1:
    rpc:
      grpcPort: $2
      membershipPort: $(($2 - 1000))
      bindOnLocalHost: true
EOF
}

{
  cat <<EOF
log:
  stdout: true
  level: warn
persistence:
  defaultStore: sqlite-default
  visibilityStore: sqlite-visibility
  numHistoryShards: 1
  datastores:
    sqlite-default:
$(sqlite_store)
    sqlite-visibility:
$(sqlite_store)
global:
  membership:
    maxJoinDuration: 30s
    broadcastAddress: "127.0.0.1"
services:
$(service frontend "$GRPC_PORT")
      httpPort: $HTTP_PORT
$(service matching $((PORT_BASE + 2)))
$(service history $((PORT_BASE + 3)))
$(service worker $((PORT_BASE + 4)))
clusterMetadata:
  enableGlobalNamespace: false
  failoverVersionIncrement: 10
  masterClusterName: "active"
  currentClusterName: "active"
  clusterInformation:
    active:
      enabled: true
      initialFailoverVersion: 1
      rpcName: "frontend"
      rpcAddress: "localhost:$GRPC_PORT"
      httpAddress: "localhost:$HTTP_PORT"
dcRedirectionPolicy:
  policy: "noop"
dynamicConfigClient:
  filepath: "$WORK_DIR/dc.yaml"
  pollInterval: "10s"
EOF
} >"$WORK_DIR/config.yaml"

"$SERVER_BIN" --config-file "$WORK_DIR/config.yaml" --allow-no-auth start \
  >"$WORK_DIR/server.log" 2>&1 &
pids+=($!)

if [[ -z "${STREAMS_REDIS_URL:-}" ]]; then
  redis-server --port "$REDIS_PORT" --save "" --appendonly no >"$WORK_DIR/redis.log" 2>&1 &
  pids+=($!)
  export STREAMS_REDIS_URL="redis://127.0.0.1:$REDIS_PORT/0"
fi

cd "$SDK_DIR"
export PYTHONPATH="$SDK_DIR${PYTHONPATH:+:$PYTHONPATH}"

# The server takes a few seconds to join its own ring. The tests need the default namespace, and
# the cross-namespace demo needs streams-b.
uv run --no-sync python - "$GRPC_PORT" <<'EOF'
import asyncio
import sys
from datetime import timedelta

from google.protobuf.duration_pb2 import Duration

from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode


async def main() -> None:
    target = f"127.0.0.1:{sys.argv[1]}"
    pending = ["default", "streams-b"]
    for _ in range(120):
        try:
            client = await Client.connect(target)
            while pending:
                try:
                    await client.workflow_service.register_namespace(
                        RegisterNamespaceRequest(
                            namespace=pending[0],
                            workflow_execution_retention_period=Duration(
                                seconds=int(timedelta(days=1).total_seconds())
                            ),
                        )
                    )
                except RPCError as err:
                    if err.status != RPCStatusCode.ALREADY_EXISTS:
                        raise
                pending.pop(0)
            return
        except (RPCError, RuntimeError):
            pass
        await asyncio.sleep(1)
    raise SystemExit(f"the server on {target} did not come up")


asyncio.run(main())
EOF
# A new namespace takes up to the namespace cache refresh to reach every service.
sleep 12

status=0
uv run --no-sync pytest \
  tests/contrib/streams \
  tests/nexus/test_workflow_operation_progress*.py \
  tests/nexus/test_temporal_system_nexus.py \
  tests/nexus/test_notification_service.py \
  tests/nexus/test_standalone_callbacks.py \
  -E "127.0.0.1:$GRPC_PORT" -p no:cacheprovider --junitxml="$WORK_DIR/junit.xml" 2>&1 |
  tee "$WORK_DIR/pytest.log" || status=$?

# A lifecycle test that skips here proves nothing, so a skip for a missing notifier, missing
# progress or missing Redis fails the lane.
gated='no stream notifier|refused Nexus progress|STREAMS_REDIS_URL|no NexusHandler callbacks'
if grep -oE "<skipped [^>]*message=\"[^\"]*($gated)[^\"]*\"" "$WORK_DIR/junit.xml"; then
  echo "live lane: lifecycle tests skipped, see above" >&2
  status=1
fi

# The release gate. Each id must have run and passed, in every parameter it has, so a renamed,
# moved or reworded test fails the lane instead of passing unseen.
gated_tests=()
gate() {
  local file=$1
  shift
  for name in "$@"; do
    gated_tests+=("$file::$name")
  done
}
gate tests/contrib/streams/test_nexus_samples.py \
  test_the_slack_router_files_each_message_under_its_channel
gate tests/contrib/streams/test_nexus_stream_operation.py \
  test_a_caller_sees_appends_as_progress_and_the_close_as_the_result \
  test_a_close_in_the_task_of_the_last_publishes_comes_after_them \
  test_a_workflow_closes_its_own_stream \
  test_a_workflows_own_publishes_reach_the_caller_as_progress \
  test_an_append_after_an_outside_close_is_refused \
  test_a_worker_flushes_on_stop_and_a_replay_after_it_runs \
  test_a_worker_without_notifications_still_delivers_the_close_result \
  test_live_a_failed_owner_fails_the_operation \
  test_live_a_close_with_an_error_fails_the_operation \
  test_live_an_outside_close_with_an_error_fails_the_operation \
  test_live_a_terminated_owner_fails_the_operation
gate tests/contrib/streams/test_nexus_reader.py \
  test_bodies_reach_the_workflow_decoded_by_the_callers_codec \
  test_live_a_close_with_the_last_publishes_loses_none_of_them \
  test_live_a_new_reader_resumes_after_a_cursor \
  test_live_a_start_on_a_closed_stream_still_names_it \
  test_live_a_workflow_reads_every_record_then_none \
  test_live_a_reader_after_continue_as_new_sees_a_later_attempt \
  test_live_each_read_carries_the_state_its_last_answer_gave \
  test_live_a_reader_reads_on_past_answers_cut_by_the_byte_budget \
  test_live_a_reader_gets_every_record_of_a_burst \
  test_live_a_position_keeps_another_producers_stale_mark_across_continue_as_new \
  test_live_a_failed_owner_raises_after_every_record_is_read \
  test_live_a_completed_owner_ends_the_reader \
  test_live_a_canceled_owner_raises_a_canceled_operation
gate tests/nexus/test_standalone_callbacks.py \
  test_live_a_worker_gets_the_completion_of_a_standalone_operation
gate tests/contrib/streams/test_nexus_follower.py \
  test_live_a_follower_gets_each_batch_then_the_close \
  test_live_a_follower_resumes_on_another_worker \
  test_live_a_late_follower_reads_the_backlog_in_pages \
  test_live_a_failed_owner_reaches_on_close_as_a_failure
gate tests/contrib/streams/test_nexus_adaptive.py \
  test_live_a_busy_stream_sends_no_notification \
  test_live_an_idle_reader_wakes_once_per_burst \
  test_live_a_short_gap_costs_no_notification \
  test_live_two_readers_wake_from_one_notification \
  test_live_a_follower_arms_and_wakes \
  test_live_a_follower_on_an_empty_stream_wakes_on_the_first_append \
  test_live_a_close_reaches_an_unarmed_caller \
  test_live_every_append_mode_notifies_each_append
gate tests/nexus/test_workflow_operation_progress.py \
  test_live_progress_is_ordered_folded_ended_and_replayed
gate tests/contrib/streams/test_output_commit.py \
  test_a_committed_close_leaves_the_topic_closed
gate tests/contrib/streams/test_samples.py \
  test_path_a_a_client_reads_what_a_workflow_published \
  test_path_b_a_reader_drops_what_a_failed_attempt_wrote
gate tests/contrib/streams/test_scenario_demos.py \
  test_s1a_a_backend_sees_every_stage_in_order_then_the_end \
  test_s2_every_segment_url_resolves_in_order \
  test_s3a_the_text_matches_and_the_turn_is_visible_at_task_acceptance \
  test_s3b_a_retry_and_a_failed_turn_read_in_one_order \
  test_s3c_a_failed_last_attempt_leaves_its_deltas \
  test_s5a_a_late_dashboard_reads_what_the_app_read \
  test_s5b_a_dashboard_reads_every_record_of_the_child \
  test_s6_positions_follow_the_wake_record \
  test_s7a_the_read_ends_and_the_backend_learns_the_failure \
  test_s7b_a_retry_is_superseded_and_the_late_write_is_stale \
  test_s8_a_publisher_replays_with_its_streams_deleted
gate tests/contrib/streams/test_nexus_scenario_demos.py \
  test_n1b_a_parent_relays_every_record_and_gets_the_close_result \
  test_n1c_a_relay_across_namespaces_gets_every_record \
  test_n2_every_segment_url_crosses_namespaces_and_resolves \
  test_n3d_an_agent_hears_every_message_whatever_bs_store \
  test_n4_a_connector_posts_every_message_and_idles_with_no_task \
  test_n5b_a_dashboard_and_a_relay_both_get_every_record \
  test_n5c_followers_that_attach_late_get_every_record \
  test_n6_a_second_operation_follows_the_location_after_the_wake \
  test_n7a_a_caller_reads_every_stage_then_gets_the_failure \
  test_n7b_the_reader_marks_the_retry_and_the_stale_write \
  test_n8_a_caller_replays_with_the_owners_streams_and_worker_gone
gate tests/contrib/streams/test_nexus_scenario_demos.py \
  test_u1a_the_app_worker_gets_each_stage_pushed_then_the_close \
  test_u4_a_standalone_connector_posts_each_message_once \
  test_u5c_dashboards_read_the_backlog_in_pages_and_after_the_close
uv run --no-sync python - "$WORK_DIR/junit.xml" "${gated_tests[@]}" <<'PY' || status=1
import sys
import xml.etree.ElementTree as ElementTree

junit, gated = sys.argv[1], sys.argv[2:]
outcomes: dict[str, list[str]] = {}
for case in ElementTree.parse(junit).iter("testcase"):
    path = case.get("classname", "").replace(".", "/") + ".py"
    test_id = f"{path}::{case.get('name', '').split('[')[0]}"
    outcome = next(
        (child.tag for child in case if child.tag in ("skipped", "failure", "error")),
        "passed",
    )
    outcomes.setdefault(test_id, []).append(outcome)
failed = False
for test_id in gated:
    seen = outcomes.get(test_id)
    if not seen:
        print(f"live lane: gated test did not run: {test_id}", file=sys.stderr)
        failed = True
    elif set(seen) != {"passed"}:
        print(f"live lane: gated test {seen}: {test_id}", file=sys.stderr)
        failed = True
sys.exit(1 if failed else 0)
PY

# Core sends the notifications and the closes, so its own live test runs against the server too.
# It returns early without a server, so the lane checks that it ran.
core_test=streams::tests::a_live_notifier_hears_appends_as_progress_and_the_close_as_the_result
STREAMS_NOTIFIER_SERVER="http://127.0.0.1:$GRPC_PORT" \
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$WORK_DIR/cargo-target}" \
  cargo test --manifest-path temporalio/bridge/sdk-core/Cargo.toml -p temporalio-sdk-core \
  --features streams --lib -- "$core_test" --exact --nocapture 2>&1 |
  tee "$WORK_DIR/core-live.log" || status=$?
if grep -q "set STREAMS_NOTIFIER_SERVER" "$WORK_DIR/core-live.log" ||
  ! grep -q "test result: ok. 1 passed" "$WORK_DIR/core-live.log"; then
  echo "live lane: Core's live notifier test did not run and pass" >&2
  status=1
fi
exit "$status"
