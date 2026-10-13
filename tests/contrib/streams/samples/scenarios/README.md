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

- **2, `s2_dashcam_reference`.** The courier appends a `Segment` with a URL for each segment. The
  backend fetches each one from an object store, which a dict stands in for. Left out: live video
  and storage that Temporal manages.
- **7b, `s7b_retried_courier`.** Activity attempt 1 appends and stalls past its 2 s timeout. Attempt
  2 appends after `SUPERSEDED`. Attempt 1 then appends late, and the record arrives marked `stale`.
  Left out: fencing and a retraction at the timeout.

The demos of 1a, 3a, 3b, 3c, 5a, 5b, 6, 7a and 8 publish from Workflow code with its Workflow
Task, which this release does not have.

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
