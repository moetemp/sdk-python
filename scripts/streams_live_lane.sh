#!/usr/bin/env bash
# Runs the stream notifier live tests against a server built with the stream notifier and the
# Workflow wake target.
#
# The stock dev server has no stream notifier, so the live tests skip there. This lane builds a
# server that has it, starts it with the settings the guide lists and short recheck and poll
# limits, and fails unless every gated test below passes. Run it from a built sdk-python checkout.
#
# Environment:
#   SERVER_REPO        git URL of the server (default: the moetemp fork)
#   SERVER_REF         commit to build (default: the wake target commit the release names)
#   SERVER_BIN         use this server binary instead of building one
#   PORT_BASE          frontend gRPC port, the other ports follow it (default: 7941)
#   STREAMS_REDIS_URL  Redis to use. Without it, the script starts a scratch redis-server on 6441.
#   WORK_DIR           where to build and keep logs (default: a new temporary directory)
set -euo pipefail

SERVER_REPO=${SERVER_REPO:-https://github.com/moetemp/temporal.git}
SERVER_REF=${SERVER_REF:-3d0297b377c0520c098391450046c8fb11bd163e}
PORT_BASE=${PORT_BASE:-7941}
WORK_DIR=${WORK_DIR:-$(mktemp -d -t streams-live-lane)}
SDK_DIR=$(cd "$(dirname "$0")/.." && pwd)

GRPC_PORT=$PORT_BASE
HTTP_PORT=$((PORT_BASE + 1))
REDIS_PORT=6441
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

# The server takes a few seconds to join its own ring, and the tests need the default namespace.
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
            try:
                await client.workflow_service.register_namespace(
                    RegisterNamespaceRequest(
                        namespace="default",
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
uv run --no-sync pytest tests/contrib/streams \
  -E "127.0.0.1:$GRPC_PORT" -p no:cacheprovider --junitxml="$WORK_DIR/junit.xml" 2>&1 |
  tee "$WORK_DIR/pytest.log" || status=$?

# A live test that skips here proves nothing, so a skip for a missing notifier or missing Redis
# fails the lane.
gated='no stream notifier|STREAMS_REDIS_URL'
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
  test_live_a_recheck_finds_a_record_whose_notification_was_lost
gate tests/contrib/streams/test_notify_and_close.py \
  test_a_close_with_an_outcome_reaches_the_notifier
gate tests/contrib/streams/test_store_plugin.py \
  test_a_worker_flushes_notifications_after_it_stops
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

exit "$status"
