#!/usr/bin/env bash
# Runs the stream notifier and streams-over-Nexus live tests against a server built with the
# stream notifier, the Workflow wake target and Nexus progress.
#
# The stock dev server has no stream notifier and no Nexus progress, so the live tests skip there.
# This lane builds a server that has both, starts it with the settings the guides list and short
# recheck and poll limits, and fails unless every gated test below passes. Run it from a built
# sdk-python checkout.
#
# Environment:
#   SERVER_REPO        git URL of the server (default: the moetemp fork)
#   SERVER_REF         commit to build (default: the Bridge server commit the release names)
#   SERVER_BIN         use this server binary instead of building one
#   PORT_BASE          frontend gRPC port, the other ports follow it (default: 7961)
#   STREAMS_REDIS_URL  Redis to use. Without it, the script starts a scratch redis-server on 6428.
#   CARGO_TARGET_DIR   where Core's live test builds (default: under WORK_DIR)
#   WORK_DIR           where to build and keep logs (default: a new temporary directory)
#   PYTEST_TARGETS     the tests to run (default: the stream and Nexus tests). A narrower run fails
#                      the gate, since the gated ids outside it don't run.
set -euo pipefail

SERVER_REPO=${SERVER_REPO:-https://github.com/moetemp/temporal.git}
SERVER_REF=${SERVER_REF:-af03cc33024701b096392d58cd859e5cfff01626}
PORT_BASE=${PORT_BASE:-7961}
WORK_DIR=${WORK_DIR:-$(mktemp -d -t streams-live-lane)}
SDK_DIR=$(cd "$(dirname "$0")/.." && pwd)

GRPC_PORT=$PORT_BASE
HTTP_PORT=$((PORT_BASE + 1))
REDIS_PORT=6428
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
  git -C "$WORK_DIR/temporal" fetch --quiet "$SERVER_REPO" "$SERVER_REF"
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
history.enableSignalWithStartFromWorkflow:
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
streamnotifier.enabled:
  - value: true
# The owner check closes a stopped owner's stream by its close status, so the
# cases where an owner ends without a close finish in seconds, not minutes.
streamnotifier.ownerCheckInterval:
  - value: 2s
streamnotifier.ownerCloseGrace:
  - value: 10s
# A client reader reads again at the poll limit, which finds a record whose
# notification was lost. The default is 60 seconds.
streamnotifier.pollMaxWait:
  - value: 5s
# The notifier asks an idle Workflow reader to read again after this gap,
# doubling up to the maximum. The defaults are 1 minute and 1 hour.
streamnotifier.recheckInitial:
  - value: 3s
streamnotifier.recheckMax:
  - value: 30s
system.forceSearchAttributesCacheRefreshOnRead:
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
    for _ in range(120):
        try:
            client = await Client.connect(target)
            for namespace in ["default", "streams-b"]:
                try:
                    await client.workflow_service.register_namespace(
                        RegisterNamespaceRequest(
                            namespace=namespace,
                            workflow_execution_retention_period=Duration(
                                seconds=int(timedelta(days=1).total_seconds())
                            ),
                        )
                    )
                except RPCError as err:
                    if err.status != RPCStatusCode.ALREADY_EXISTS:
                        raise
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
default_targets=(
  tests/contrib/streams
  tests/nexus/test_workflow_operation_progress*.py
  tests/nexus/test_temporal_system_nexus.py
  tests/nexus/test_notification_service.py
  tests/nexus/test_standalone_callbacks.py
)
uv run --no-sync pytest ${PYTEST_TARGETS:-${default_targets[*]}} \
  -E "127.0.0.1:$GRPC_PORT" -p no:cacheprovider --junitxml="$WORK_DIR/junit.xml" 2>&1 |
  tee "$WORK_DIR/pytest.log" || status=$?

# A live test that skips here proves nothing, so a skip for a missing notifier, missing Nexus
# progress or missing Redis fails the lane.
gated='no stream notifier|refused Nexus progress|STREAMS_REDIS_URL|no NexusHandler callbacks'
if grep -oE "<skipped [^>]*message=\"[^\"]*($gated)[^\"]*\"" "$WORK_DIR/junit.xml"; then
  echo "live lane: live tests skipped, see above" >&2
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
gate tests/contrib/streams/test_live_notifications.py \
  test_live_an_idle_client_reader_wakes_on_the_next_append \
  test_live_a_busy_stream_sends_no_notification \
  test_live_a_client_reader_finds_a_lost_notification_at_the_poll_limit \
  test_live_every_append_mode_notifies_each_append \
  test_live_a_close_with_an_outcome_reaches_the_notifier \
  test_live_a_workflow_reader_sleeps_and_wakes_on_an_append \
  test_live_a_workflow_reader_on_an_empty_stream_wakes_on_the_first_append \
  test_live_two_readers_wake_from_one_notification \
  test_live_a_workflow_close_ends_both_readers_with_its_outcome \
  test_live_a_position_resumes_the_reader_after_continue_as_new \
  test_live_a_recheck_finds_a_record_whose_notification_was_lost \
  test_live_a_positions_only_reader_records_ranges_and_replays_from_the_store \
  test_live_a_replay_that_finds_other_records_is_nondeterminism \
  test_live_a_replay_of_a_lost_range_fails_the_task
gate tests/contrib/streams/test_notify_and_close.py \
  test_a_close_with_an_outcome_reaches_the_notifier
gate tests/contrib/streams/test_store_plugin.py \
  test_a_worker_flushes_notifications_after_it_stops
gate tests/contrib/streams/test_sleeping_reader_demos.py \
  test_j1_a_client_wakes_on_each_burst_and_ends_at_the_close \
  test_j2_a_connector_posts_every_message_once_in_order \
  test_j3_a_tail_reads_every_line_of_a_sparse_log
gate tests/contrib/streams/test_workflow_close.py \
  test_a_workflow_close_ends_reads_while_the_workflow_runs \
  test_a_rerun_after_the_close_finds_its_flushes_landed
gate tests/contrib/streams/test_nexus_samples.py \
  test_the_slack_router_files_each_message_under_its_channel
gate tests/contrib/streams/test_nexus_stream_operation.py \
  test_a_caller_sees_appends_as_progress_and_the_close_as_the_result \
  test_a_close_right_after_the_last_publishes_comes_after_them \
  test_a_workflow_closes_its_own_stream \
  test_a_workflows_own_publishes_reach_the_caller_as_progress \
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
gate tests/nexus/test_workflow_operation_progress.py \
  test_live_progress_is_ordered_folded_ended_and_replayed
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
