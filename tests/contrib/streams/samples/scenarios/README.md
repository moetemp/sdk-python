# Scenario demos for streams

Each module here runs one scenario of the "Nexus streaming scenarios" page, in the version this
release can run. A demo starts its own Worker, runs its Workflows on a dev server, and prints what
its backend sees.

## Run them

From the repository root, with a dev server on `localhost:7233`:

```sh
python -m tests.contrib.streams.samples.scenarios.s2_dashcam_reference
STREAMS_REDIS_URL=redis://localhost:6379 \
  python -m tests.contrib.streams.samples.scenarios.s7b_retried_courier
```

`TEMPORAL_ADDRESS` names another server. Without `STREAMS_REDIS_URL` a demo uses `MemoryStreams`.

The tests run every demo on the test server, on memory and again on Redis when
`STREAMS_REDIS_URL` is set:

```sh
STREAMS_REDIS_URL=redis://localhost:6379 pytest tests/contrib/streams/test_scenario_demos.py
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
  replays its History with a store at a port where nothing listens. History holds no marker for
  the publishes, and the replay sends nothing to the store. Left out: a Workflow that reads a
  stream.

This release publishes from Workflow code with `workflow_writer`, and Core appends each
activation's records when the activation completes. The demos of 5a, 6 and 7a publish from a
client or an Activity. 5b
has no demo of its own: its dashboard half is 5a, and its relay half needs a Workflow reader.

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
- A close that carries a failure (7a). The read ends at the close, and `s7a_failed_delivery` gets
  the outcome from the Workflow.
- A reset that retracts the old run's records (8). Each record carries its run id instead.
