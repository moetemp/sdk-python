# Scenario demos for streams

Each module here runs one scenario of the "Nexus streaming scenarios" page, in the version the
release can run. A demo starts its own Workers, runs its Workflows, and prints what its backend
sees. The `s` modules run on Brook, the interim release, and the `n` modules need Bridge, which
serves streams over Nexus.

## Run them

A Brook demo runs from the repository root, with a dev server on `localhost:7233`:

```sh
python -m tests.contrib.streams.samples.scenarios.s1a_order_tracking
STREAMS_REDIS_URL=redis://localhost:6379 \
  python -m tests.contrib.streams.samples.scenarios.s3b_redis_turns
```

`TEMPORAL_ADDRESS` names another server. Without `STREAMS_REDIS_URL` a demo uses `MemoryStreams`.
`s3b` needs Redis.

A Bridge demo needs a server with the stream notifier and Nexus operation progress, and the
cross-namespace demos need the namespace `streams-b`. `scripts/streams_live_lane.sh` builds and
starts such a server, registers `streams-b`, and runs every demo test:

```sh
TEMPORAL_ADDRESS=127.0.0.1:7881 \
  python -m tests.contrib.streams.samples.scenarios.n1c_cross_namespace_relay
```

The tests run every demo on memory, and again on Redis when `STREAMS_REDIS_URL` is set:

```sh
STREAMS_REDIS_URL=redis://localhost:6379 pytest tests/contrib/streams/test_scenario_demos.py
STREAMS_REDIS_URL=redis://localhost:6379 pytest tests/contrib/streams/test_nexus_scenario_demos.py \
  -E 127.0.0.1:7881
```

## Brook demos

- **1a, `s1a_order_tracking`.** `OrderWorkflow` publishes `placed`. The restaurant and the courier
  append stages as outside producers. The Workflow writes `FINISH`, and the backend's read ends when
  the Workflow closes. Left out: the Worker Callback that pushes each record to the app. The backend
  long-polls.
- **2, `s2_dashcam_reference`.** The courier appends a `Segment` with a URL for each segment. The
  backend fetches each one from an object store, which a dict stands in for. Left out: live video
  and storage that Temporal manages.
- **3a, `s3a_token_deltas`.** The Workflow publishes the turn on `turns`, and the model Activity
  appends deltas on `deltas`. The backend reads both topics at once. The Activity sees `turn
  started` already, since the publish is visible when its activation completes. Left out: the
  native stream. Redis or memory stands in for it.
- **3b, `s3b_redis_turns`.** Two turns on one topic in Redis. Turn 1's Activity is retried, so
  `SUPERSEDED` comes before the retry's deltas. Turn 2 fails on every attempt, and the Workflow
  publishes `turn 2 failed` after its partial deltas. It leaves nothing out, since it is the release
  as built.
- **3c, `s3c_two_topics`.** Turns and deltas in two topics of one store, with no reference record.
  The last attempt fails, and its deltas stay after `SUPERSEDED`. Left out: the native stream and a
  retraction the server writes at an attempt timeout.
- **5a, `s5a_late_dashboard`.** The app follows live. After three records, a dashboard reads from
  `BEGINNING`, catches up and follows live. Both reads end when the Workflow closes. Left out: a
  push to a late reader.
- **5b, `s5b_child_dashboard`.** A parent starts the child `Delivery`. A dashboard reads
  `Delivery`'s stream from `BEGINNING`. Left out: the parent relaying the child's stream. That needs
  a Workflow reader.
- **6, `s6_location_topic`.** `Delivery` publishes `tracking started` on `events`. The backend then
  starts to read `location`, where the courier appends positions. Left out: a backend in another
  namespace, which needs Nexus direct calls. A cheaper storage tier for `location`.
- **7a, `s7a_failed_delivery`.** `Delivery` publishes two stages and fails with `CourierLost`. The
  read ends at the close. The backend describes the Workflow and gets the failure from its result.
  Left out: a close that carries the failure to stream readers.
- **7b, `s7b_retried_courier`.** Activity attempt 1 appends and stalls past its 2 s timeout. Attempt
  2 appends after `SUPERSEDED`. Attempt 1 then appends late, and the record arrives marked `stale`.
  Left out: fencing and a retraction at the timeout.
- **8, `s8_publish_replay`.** A publisher completes, an admin deletes its streams, and a `Replayer`
  with no store replays the History. The stream is still empty after the replay. Left out: a
  Workflow that reads a stream and replays its reads. A reset, whose records carry the new run id
  and retract nothing.

Bridge runs these too, and adds nothing to 1a, 3a, 3b, 3c and 5a.

## Bridge demos

- **1b, `n1b_parent_relay`.** The parent `Order` starts the child `Delivery`, then a stream
  operation on an endpoint in its own namespace whose `open_stream` names `Delivery`'s stream. It
  reads with `StreamReader` and republishes with `workflow_writer`, and the backend long-polls the
  parent's stream. The operation completes with the close result. Left out: System Nexus
  `ReadStream` and records in progress callbacks.
- **1c, `n1c_cross_namespace_relay`.** As 1b, with `Delivery` and the handler in namespace
  `streams-b` and the caller in `default`. Only B's Worker holds B's store. Left out: records in
  progress callbacks.
- **2, `n2_dashcam_relay`.** s2's `Segment` URLs, appended in B, reach a Workflow in `default`
  through the 1c path and resolve. Left out: live video, and a store claim across Nexus.
- **3d, 3e, `n3d_inner_loop`.** `AgentWorkflow` in `default` follows `TurnWorkflow`'s `message`
  topic in `streams-b`, one topic per operation. The test runs B on memory (3d) and on Redis (3e),
  and the agent hears the same. Left out: a backend that polls `delta` across namespaces.
- **4, `n4_slack_connector`.** A `Connector` Workflow in `default` follows the agent's `message`
  topic in `streams-b` and posts each message through an Activity. While the agent idles 10 s, the
  connector's History has no Workflow Task. Left out: a standalone Nexus operation as the caller,
  and a Worker Callback per delivery.
- **5b, `n5b_dashboard_and_relay`.** s5b's dashboard and 1b's relay read `Delivery`'s stream at the
  same time, and both get every record.
- **5c, `n5c_late_attach`.** Followers start the operation at once, after three records, and after
  the close. Each reads the backlog from `BEGINNING`, and all get every record and the close result.
  The last one completes at once. Left out: a standalone Nexus operation as the dashboard, and a
  pushed backlog.
- **6, `n6_location_operation`.** A `Tracker` in `default` follows `events` in `streams-b`, and
  after `tracking started` it follows `location` through a second operation. The test counts the
  reads in its History. Left out: a backend outside any Workflow that polls `location` in B.
- **7a, `n7a_failed_delivery`.** `Delivery` fails with `CourierLost`, closes with
  `close_workflow_stream(error=...)`, or is terminated. The caller reads every stage, then `next()`
  raises `NexusOperationError` with the failure as its cause.
- **7b, `n7b_reader_marks`.** s7b's courier Activity behind a stream operation. The caller's
  `reader.supersessions` has the takeover, and `reader.stale_records` has the late record.
- **8, `n8_replay_after_trim`.** A `Bookkeeper` continues as new with `reader.position` and reads on
  after it, across the owner's own Continue-as-New. The owner's streams are then deleted and its
  Worker stops, and a `Replayer` replays both of the bookkeeper's runs. Left out: a reset that
  retracts records.

## What each release demos

Brook has no Nexus and no Workflow reader, so 1b, 1c, 3d, 3e, 4 and 5c have no Brook demo. On
Bridge, every scenario has a demo.

| Scenario | Brook | Bridge |
|---|---|---|
| 1a | `s1a` | `s1a` |
| 1b | none, a Workflow must read the child's stream | `n1b` |
| 1c | none, the relay goes through Nexus | `n1c` |
| 2 | `s2` | `n2` |
| 3a, 3b, 3c | `s3a`, `s3b`, `s3c` | the same |
| 3d, 3e | none, the inner loop is behind Nexus | `n3d` |
| 4 | none, the connector reads across namespaces | `n4` |
| 5a | `s5a` | `s5a` |
| 5b | `s5b`, the dashboard half | `n5b` |
| 5c | none, a late caller attaches through Nexus | `n5c` |
| 6 | `s6`, one namespace | `n6` |
| 7a | `s7a`, the outcome from the Workflow | `n7a` |
| 7b | `s7b` | `n7b` |
| 8 | `s8`, the publish side | `n8` |

## What no demo shows

- A Worker Callback per record, or a push to an app's backend (1a, 4, 5c). Not built.
- A standalone Nexus operation as a caller (4, 5c). Only a Workflow's operation takes progress.
- The native stream, server fencing and server retraction (3a as written, 3c, 7b). Out of scope.
- Nexus direct calls, so an outside backend can read across namespaces (3d, 3e, 6). Not built.
- Live video and storage that Temporal manages (2). Out of scope. Whether a store claim resolves
  across Nexus is still open.
- Records in progress callbacks (1b, 1c). Progress is a notification, and the records come from
  reads that the caller's History records.
- A reset that retracts the old run's records (8). Each record carries its run id instead.
