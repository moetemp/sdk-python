# Moving from Workflow Streams

> This package is experimental and may change in future versions.

`temporalio.contrib.workflow_streams` keeps a stream inside the Workflow:
publishes arrive as Signals, readers poll with Updates, and the records
live in the Workflow's state and History. `temporalio.contrib.streams`
keeps the records in your Redis instead. Publishing costs no Signals, and
records stay out of History. Readers do not need a running Worker, and any
number of readers can follow one stream.
This page maps one to the other.

## What maps

| Workflow Streams | Streams |
|---|---|
| `WorkflowStream()` in `@workflow.init` | Nothing to construct. Register `RedisStreams` once, on the client. |
| `stream.topic("events", type=E).publish(e)` in the Workflow | `stream_producer(topic("events", E)).append(e)`, which the Worker appends before it completes the activation |
| `WorkflowStreamClient.from_within_activity()` and `topic(...).publish(...)` in an Activity | `await stream_producer(...).append(...)` |
| `WorkflowStreamClient.create(client, workflow_id)` and `topic(...).publish(...)` | `stream_handle(client, workflow_id).producer(topic=..., producer_id=..., attempt=...).append(...)` |
| `client.subscribe(["events"], result_type=E)` | `stream_handle(client, workflow_id).read(topic=topic("events", E))` |
| Integer offsets, `from_offset=` | Opaque cursors: keep `record.cursor`, pass `after=cursor` |
| Publisher dedupe by publisher id and sequence | Producer id, attempt and sequence. A retry of the newest batch is written once. |
| `stream.continue_as_new(...)` and `prior_state` | Nothing. The stream follows the run chain. |
| `batch_interval`, `force_flush` | Each `append` is one atomic batch. |
| `get_offset()` | `latest()` returns the newest cursor |

Differences to plan for:

- **Cursors are opaque.** Do not compare them or do arithmetic on them. A
  cursor belongs to one stream, and another stream refuses it.
- **Producers declare an attempt.** A producer outside an Activity passes
  `producer_id` and `attempt`, and raises the attempt when it restarts its
  work. A retry inside an attempt deduplicates. A new attempt makes readers
  see `SUPERSEDED` first.
- **Retention is time-based.** Records expire `retention` after they were
  written (7 days by default). There is no `truncate(up_to_offset)`.
- **Payload codec.** Workflow Streams runs the codec once per Signal or
  Update, on the batch. Streams runs it on each record body before storing
  it. Readers decode with the client's data converter as usual.

## What does not map

- **Reading inside the Workflow.** A Workflow reads a stream with
  `workflow_reader`, one Local Activity per batch, and sleeps on the
  stream's notifier while the stream is idle. That needs a server with the
  stream notifier. On a server without it, keep that part on Workflow
  Streams, or send the Workflow what it needs, for example as a Signal.
- **A server-sent events bridge.** Streams ships none. Serve readers from
  your own endpoint with `stream_handle(...).read(...)`.
- **Records already published.** They stay where they are, in the
  Workflow's state and History. Nothing copies them to Redis. A reader that
  needs both must read the old stream to its end, then the new one.

## Moving a running application

1. Register `RedisStreams` on the client with `Client.connect`, and deploy
   Workers built from that client.
2. Move producers and readers of one topic together. A topic split across
   both libraries is two streams.
3. For Workflows already running, switch at a natural boundary, such as the
   next run after Continue-as-New, or let them finish on Workflow Streams.
   A publish with `stream_producer` adds no command. Gate the switch with
   Workflow versioning where the code it replaces sends commands. For
   example:

   ```python
   if workflow.patched("publish-to-streams"):
       stream_producer(TOKENS).append(token)
   else:
       stream.topic("tokens").publish(token)
   ```
4. Remove `WorkflowStream` from a Workflow only after no reader needs its
   records.
