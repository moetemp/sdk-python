# Scenario demos for streams

Each module here runs one scenario of the "Nexus streaming scenarios" page, in the version this
release can run. A demo starts its own Worker, runs its Workflows on a dev server, and prints what
its backend sees.

## Run them

From the repository root, with a dev server on `localhost:7233`:

```sh
python -m tests.contrib.streams.samples.scenarios.s1a_order_tracking
STREAMS_REDIS_URL=redis://localhost:6379 \
  python -m tests.contrib.streams.samples.scenarios.s3b_redis_turns
```

`TEMPORAL_ADDRESS` names another server. Without `STREAMS_REDIS_URL` a demo uses `MemoryStreams`.
`s3b` needs Redis.

The tests run every demo on the test server, on memory and again on Redis when
`STREAMS_REDIS_URL` is set:

```sh
STREAMS_REDIS_URL=redis://localhost:6379 pytest tests/contrib/streams/test_scenario_demos.py
```

## The demos

- **1a, `s1a_order_tracking`.** `OrderWorkflow` publishes `placed`. The restaurant and the courier
  append stages as outside producers. The Workflow writes `FINISH`, and the backend's read ends when
  the Workflow closes. Left out: the Worker Callback that pushes each record to the app. The backend
  long-polls.
- **2, `s2_dashcam_reference`.** The courier appends a `Segment` with a URL for each segment. The
  backend fetches each one from an object store, which a dict stands in for. Left out: live video
  and storage that Temporal manages.
- **3a, `s3a_token_deltas`.** The Workflow publishes the turn on `turns`, and the model Activity
  appends deltas on `deltas`. The backend reads both topics at once. The Activity sees `turn
  started` already, since the publish is visible when its Workflow Task is accepted. Left out: the
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

## Scenarios with no demo in this release

This release has no Nexus and no Workflow reader, so these scenarios can't run in any version.

| Scenario | Why |
|---|---|
| 1b | A parent Workflow must read the child's stream to relay it. |
| 1c | The relay goes through Nexus to a stream in another namespace. |
| 3d | The inner loop runs behind Nexus in another namespace. |
| 3e | As 3d, with the inner loop's deltas in Redis. |
| 4 | The connector reads a stream in another namespace. |
| 5c | A late caller attaches to the stream through Nexus. |

## What no demo shows

- A Worker Callback per record, or a push to an app's backend (1a, 4, 5c). Not built.
- The native stream, server fencing and server retraction (3a as written, 3c, 7b). Out of scope.
- Live video and storage that Temporal manages (2). Out of scope.
- A close that carries a failure (7a). The read ends at the close, and the outcome comes from the
  Workflow.
- A reset that retracts the old run's records (8). Each record carries its run id instead.
