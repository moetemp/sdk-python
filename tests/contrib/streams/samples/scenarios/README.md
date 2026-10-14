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

## Readers that sleep

These demos need a server with the stream notifier and the Workflow wake target. Run them with
the live lane, `scripts/streams_live_lane.sh`, or against such a server on `TEMPORAL_ADDRESS`.
They turn on `notify_on_append()` themselves.

- **`j1_sleeping_client`.** The owner publishes three bursts with gaps longer than the idle wait,
  then closes its stream. A client reader sleeps on the notifier in each gap and wakes on the next
  burst.
- **`j2_slack_connector`.** A connector Workflow follows a channel's stream in the same namespace
  and posts each message through an Activity. Between messages it sleeps with no task. Left out:
  Slack itself, and a channel in another namespace, which needs Nexus.
- **`j3_ci_tail`.** A build's steps each append one log line after a long step, and the build
  closes its stream with its status. A client tail sleeps between lines.

The demos of 1a, 3a, 3b, 3c, 5a, 5b, 6, 7a and 8 publish from Workflow code with its Workflow
Task, which this release does not have.

## Scenarios with no demo in this release

This release has no Nexus. A Workflow reader in the same namespace covers part of 1b and 4, as
`j2_slack_connector` shows, but not across namespaces.

| Scenario | Why |
|---|---|
| 1b | The relay's caller sees the stream as a Nexus operation's progress. |
| 1c | The relay goes through Nexus to a stream in another namespace. |
| 3d | The inner loop runs behind Nexus in another namespace. |
| 3e | As 3d, with the inner loop's deltas in Redis. |
| 4 | The connector reads a stream in another namespace. |
| 5c | A late caller attaches to the stream through Nexus. |

## What no demo shows

- A Worker Callback per record, or a push to an app's backend (1a, 4, 5c). Not built.
- The native stream, server fencing and server retraction (3a as written, 3c, 7b). Out of scope.
- Live video and storage that Temporal manages (2). Out of scope.
- A close that carries a failure (7a) to a client reader. A client read ends at the close, and the
  outcome comes from the Workflow. A Workflow reader raises it, as the live lane checks.
- A reset that retracts the old run's records (8). Each record carries its run id instead.
