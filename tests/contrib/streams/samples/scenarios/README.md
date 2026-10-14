# Scenario demos for streams

Each module here runs one scenario of the "Nexus streaming scenarios" page, in the version this
release can run. A demo starts its own Workers, runs its Workflows, and prints what its backend
sees. The `n` modules need Bridge, which serves streams over Nexus. The `u` modules need
worker-hosted followers too, which push a stream's progress to a Worker through a standalone Nexus
operation.

## Run them

From the repository root, with a dev server on `localhost:7233`:

```sh
python -m tests.contrib.streams.samples.scenarios.s2_dashcam_reference
STREAMS_REDIS_URL=redis://localhost:6379 \
  python -m tests.contrib.streams.samples.scenarios.s7b_retried_courier
```

`TEMPORAL_ADDRESS` names another server. Without `STREAMS_REDIS_URL` a demo uses `MemoryStreams`.

A Bridge demo needs a server with the stream notifier and Nexus operation progress, and the
cross-namespace demos need the namespace `streams-b`. `scripts/streams_live_lane.sh` builds and
starts such a server, registers `streams-b`, and runs every demo test:

```sh
TEMPORAL_ADDRESS=127.0.0.1:7961 \
  python -m tests.contrib.streams.samples.scenarios.n1c_cross_namespace_relay
```

The tests run every demo on memory, and again on Redis when `STREAMS_REDIS_URL` is set:

```sh
STREAMS_REDIS_URL=redis://localhost:6379 pytest tests/contrib/streams/test_scenario_demos.py
STREAMS_REDIS_URL=redis://localhost:6379 pytest tests/contrib/streams/test_nexus_scenario_demos.py \
  -E 127.0.0.1:7961
```

## The demos

- **1a, `s1a_order_tracking`.** The `Order` Workflow publishes `placed` with `workflow_writer`. The
  courier appends two stages through a client producer, and the Workflow publishes `delivered`.
  The app reads every stage in order. Left out: a push to the app.
- **2, `s2_dashcam_reference`.** The courier appends a `Segment` with a URL for each segment. The
  backend fetches each one from an object store, which a dict stands in for. Left out: live video
  and storage that Temporal manages.
- **3a to 3c, `s3_chat_turn`.** `Chat` publishes the turn's start and end on `turns` with
  `workflow_writer`. Its Activity appends tokens on `tokens`, and attempt 1 fails after two. The
  UI reads `tokens` through `track_supersession`, takes back the two tokens at `SUPERSEDED`, and
  shows only attempt 2's answer. Left out: a model, and server fencing and retraction.
- **5a, `s5a_late_dashboard`.** The courier appends two stages before anyone reads. A late
  dashboard reads from `BEGINNING` and gets all four stages. A live view reads from `END` and gets
  only the two that come after it started. Left out: the relay half of 5b.
- **6, `s6_location_topic`.** The app reads `events`. The `tracking started` record opens a
  second client read on `location` from `BEGINNING`, which also gets the position from before it.
- **7a, `s7a_failed_delivery`.** The courier Activity appends two stages and fails. The read ends
  at the Workflow's end, and the backend gets `CourierLost` from the Workflow's result. Left out: a
  close that carries the failure.
- **7b, `s7b_retried_courier`.** Activity attempt 1 appends and stalls past its 2 s timeout. Attempt
  2 appends after `SUPERSEDED`. Attempt 1 then appends late, and the record arrives marked `stale`.
  Left out: fencing and a retraction at the timeout.
- **8, `s8_publisher_replay`.** `Publisher` publishes with `workflow_writer`, and a `Replayer`
  replays its History with a store at a port where nothing listens. Each flush's result comes from
  its marker. Left out: a Workflow that reads a stream, which `j8_replay_opt_out` shows.

## Readers that sleep

These demos need a server with the stream notifier and the Workflow wake target. Run them with
the live lane, `scripts/streams_live_lane.sh`, or against such a server on `TEMPORAL_ADDRESS`.
They turn on `notify_on_append()` themselves.

- **`j1_sleeping_client`.** The owner publishes three bursts with gaps longer than the idle wait,
  then closes its stream. A client reader sleeps on the notifier in each gap and wakes on the next
  burst.
- **1b, `j1b_parent_relay`.** `Order` starts its `Delivery` child, reads the child's stages with
  `workflow_reader`, and publishes each one again on its own topic with `workflow_writer`. The app
  reads only the parent's stream. Left out: a child in another namespace (1c).
- **`j2_slack_connector`.** A connector Workflow follows a channel's stream in the same namespace
  and posts each message through an Activity. Between messages it sleeps with no task. Left out:
  Slack itself, and a channel in another namespace, which needs Nexus.
- **`j3_ci_tail`.** A build's steps each append one log line after a long step, and the build
  closes its stream with its status. A client tail sleeps between lines.
- **5b, `j5b_dashboard_and_relay`.** `Order` relays its child's stream as in `j1b_parent_relay`.
  After the first relayed stage, a dashboard opens late on the child's stream and reads it from
  `BEGINNING`. Both get every stage. Left out: a child in another namespace.
- **7a, `j7a_close_with_error`.** `Delivery` publishes two stages, then closes its stream with a
  `CourierLost` error and runs on. A client read and a `Dispatcher` Workflow's `workflow_reader`
  both get the two stages, then raise `CourierLost`. Left out: an owner that fails with no close.
- **8, `j8_replay_opt_out`.** Two `Audit` Workflows read one stream, one with the default
  `replay_from_store=True` and one with `False`. A Worker restart replays both. Then a `Replayer`
  with no connected store replays only the one that kept its batches in History.

This release publishes from Workflow code with `workflow_writer`, through Local Activities rather
than with the Workflow Task. The demos of 5a, 6 and 7a publish from a client or an Activity. The
dashboard half of 5b is 5a, and `j5b_dashboard_and_relay` runs both halves.

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
- **5b, `n5b_dashboard_and_relay`.** A dashboard and 1b's relay read `Delivery`'s stream at the
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
  raises `NexusOperationError` with the failure as its cause. A failed owner that never closed its
  stream gives the owner check's `StreamOwnerFailed`, not `CourierLost`.
- **7b, `n7b_reader_marks`.** s7b's courier Activity behind a stream operation. The caller's
  `reader.supersessions` has the takeover, and `reader.stale_records` has the late record.
- **8, `n8_replay_after_trim`.** A `Bookkeeper` continues as new with `reader.position` and reads on
  after it, across the owner's own Continue-as-New. The owner's streams are then deleted and its
  Worker stops, and a `Replayer` replays both of the bookkeeper's runs. Left out: a reset that
  retracts records.

Bridge adds demos of 1b, 1c, 3d, 3e, 4 and 5c, which need Nexus, and Nexus versions of 2, 5b, 6,
7a, 7b and 8.

## Worker-hosted follower demos

They need the Bridge server with `nexusoperation.enableStandalone` and
`nexusoperation.enabledCallbackKinds: [nexus, nexusHandler]`, as the live lane sets.

- **1a, `u1a_worker_push`.** `OrderWorkflow` publishes stages behind a stream operation. The app
  follows with `follow_stream`, and its Worker prints each batch as the server pushes it, then the
  close. The app code has no read loop. Left out: data in the callback. One callback per record. An
  endpoint is needed even in one namespace.
- **4, `u4_sano_connector`.** n4's agent in `streams-b`, with the connector in `default` as a
  standalone operation and a Worker that posts each message from `on_records`. The Worker holds no
  store of `streams-b`, so it reads over the Nexus HTTP ingress, at the gRPC port plus one or at
  `TEMPORAL_HTTP_ADDRESS`. Posts are at least once, so it drops repeats by cursor. Left out: a topic
  filter.
- **5c, `u5c_sano_dashboard`.** A dashboard follows after 50 records. It reads the backlog in pages
  of 10 and is slow, so a call answers `behind` and the server sends the progress again until it
  caught up. It then follows live. A second dashboard follows after the close and gets the backlog
  and the close at once. Left out: the backlog as data in the callbacks.

## What no demo shows

- A Worker Callback per record (1a, 4, 5c). With worker-hosted followers a Worker gets a push per
  progress and reads the records.
- Nexus direct calls, so an outside backend can read across namespaces (3d, 3e, 6). Not built.
- Records in progress callbacks (1b, 1c). Progress is a notification, and the records come from
  reads that the caller's History records.
- The native stream, server fencing and server retraction (3a as written, 3c, 7b). Out of scope.
- Live video and storage that Temporal manages (2). Out of scope. Whether a store claim resolves
  across Nexus is still open.
- A reset that retracts the old run's records (8). Each record carries its run id instead.
