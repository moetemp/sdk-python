# Temporal Streams

> This package is experimental and may change in future versions.

A stream is an ordered log that a Workflow owns. The Workflow publishes to
it from Workflow code, and its Activities and any client append to it.
Outside readers follow it from a cursor. Records live in the application's
Redis and never pass through Temporal or its History. Core runs the store
for every SDK, so Python reads what any other SDK writes. Publishes and
appends add nothing to History.

## Register

Register the store once, on the client, with `Client.connect`. Connecting
the client connects the store, and every Worker built from that client
publishes and appends through it. There's nothing extra to install.

```python
from temporalio.client import Client
from temporalio.contrib.streams.redis import RedisStreams

client = await Client.connect(
    "localhost:7233", plugins=[RedisStreams("redis://localhost:6379")]
)
```

Redis 7.0 or later is required. Use `maxmemory-policy noeviction`: an
evicting Redis drops whole stream keys under memory pressure, and Core logs
a warning when it sees another policy. For tests and local development,
`temporalio.contrib.streams.memory.MemoryStreams` keeps streams in the
process.

## Path B: an Activity streams to its Workflow

An Activity writes to the stream of the Workflow that scheduled it, as
itself: its Activity id with the scheduling run id, and its Temporal
attempt. The run id keeps two runs of a chain apart, since Activity ids
restart in each run. When a retry starts writing, readers first see a
`SUPERSEDED` record, so they can drop what this Activity's failed attempt
wrote.

```python
TOKENS = topic("tokens", str)

@activity.defn
async def generate(prompt: str) -> str:
    producer = activity_handle().producer(topic=TOKENS)
    ...
    await producer.append(*words)
    await producer.finish()

text = []
async for record in get_stream_handle(client, "answer-1").read(topic=TOKENS):
    if record.kind is RecordKind.SUPERSEDED:
        text.clear()
    elif record.kind is RecordKind.DATA:
        text.append(record.value)
```

`SUPERSEDED` is per producer attempt. It withdraws only what the producer
in `record.producer_id` wrote in its earlier attempts. It doesn't cancel
the stream, and the records of other producers stay valid. This topic has
one producer, so clearing everything is right. With several producers on a
topic, keep the text per `record.producer_id` and clear only that
producer's.

`SupersessionTracker` does that bookkeeping. It holds the records of a read
under their producer and returns the ones each `SUPERSEDED` withdraws, in
stream order. `track_supersession` wraps a read with it:

```python
async for record, withdrawn in track_supersession(handle.read(topic=TOKENS)):
    for old in withdrawn:
        undo(old)
    if record.kind is RecordKind.DATA and not record.stale:
        show(record)
```

A record marked `stale` comes back withdrawn as it arrives, since an older
attempt wrote it after the read delivered a newer one. The tracker keeps
every record it may still withdraw in memory, for the producers it saw
most recently. It keeps at most `max_producers` of them (1,000 by default),
and no more than fit in 16 KiB at each id's length plus 20 bytes. Past that
it drops the producer it saw least recently. The read's state in Core
forgets producers by the same rule, so a dropped producer gets no
`SUPERSEDED` either. Each Local Activity call is a producer, about 57 bytes
of id each, so on a stream that many calls write the tracker holds about 200
of them and its memory stays steady. For a Workflow's producer, `@<run id>`,
it holds only the records of the newest Workflow Task, since a rerun
withdraws only the records of its own task. Call `forget(producer_id)` once
a producer can't retry, to drop it sooner.

A future Workflow reader can't drop superseded records silently, because
replay must see the same records, so it will deliver `SUPERSEDED` too.

Every read follows the Workflow's run chain across Continue-as-New and ends
when the chain closes. A reader can't stop at one run. A Workflow id can
start a new chain after the old one closed, and a handle opens the newest
chain unless `chain_run_id=` names a run of another one. An Activity's
`record.producer_id` names the run that scheduled it.
To resume later, keep the last `record.cursor` and pass it back as
`read(after=cursor)`.

The sample runs in full in `tests/contrib/streams/samples/`.

## A Local Activity streams to its Workflow

A Local Activity calls `activity_handle()` the same way, and its records
are visible while the Workflow Task that runs it is still open. So an agent
loop that runs its model calls as Local Activities streams their tokens.

A Local Activity's attempt is not durable: after a Worker crash or a failed
Workflow Task, the same call runs again from attempt 1. So it writes with
an identity of its own:

- **Producer id.** `__temporal_local_<n>@<scheduling run id>`, where `n`
  counts the run's Local Activity calls in order. The `__temporal` prefix is
  reserved, so no user Activity id writes as the same producer. The store plugin's Worker interceptor
  stamps the number on each call in a header, so it stays the same across
  reruns, and across a retry that backs off longer than
  `local_retry_threshold`, which gets a new Activity id.
- **Attempt.** Before the first append, the producer asks the server for
  the run's History length `h` and the pending Workflow Task's attempt `t`.
  With the Local Activity's attempt `a`, it writes attempt
  `(h * 100_000 + t) * 10_000 + a`. Each rerun runs in a later Workflow Task
  attempt, so the attempt grows, and readers see `SUPERSEDED` before the
  rerun's records. A retry inside one attempt is written once, as for any
  producer.

Known limits:

- **One describe call.** Each Local Activity attempt makes one
  `DescribeWorkflowExecution` call before its first append, so its first
  record waits one round trip to the server. Later appends make no call.
  Core's owner check also describes the Workflow, but it caches the answer
  per owner run for a minute, so the run's Local Activities share it.
- **Describe permission.** The Worker's identity needs permission to
  describe the Workflow (`DescribeWorkflowExecution` on its namespace),
  both for the attempt and for Core's owner check. Without it, the first
  append fails.
- **A later Core change removes the call.** Workflow code doesn't see the
  Workflow Task attempt, so the server is asked for it. A later Core change
  can supply the Workflow Task attempt, which removes the call.
- **Attempt range.** A Local Activity past attempt 9,999, or in a Workflow
  Task past attempt 99,999, raises `StreamUnsupportedError` on its first
  append.

## A Workflow publishes to its own stream

Workflow code can't do I/O, so `workflow_writer` hands its records to Core
with the activation that published them. Core appends them to the store when
the activation completes, before it reports the Workflow Task to the server.

```python
STEPS = topic("steps", str)

@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        steps = workflow_writer(STEPS)
        steps.publish("planning")
        answer = await workflow.execute_local_activity(
            call_model, prompt, start_to_close_timeout=timedelta(minutes=5)
        )
        steps.publish("answered")
        return answer
```

- **Publish.** `publish(value)` is a plain call. It converts the value with
  the Workflow's payload converter and adds the record to the activation.
- **Visibility.** A record is visible when the activation that published it
  completes. That is before the Workflow Task completes, and before the
  server accepts it. In the example, readers see `planning` while the model
  call runs as a Local Activity in the same Workflow Task.
- **Order.** Records land in the order the run published them, across all
  writers and topics of the run.
- **No History.** Publishing adds no event and no marker to History, and
  costs no Action.
- **Identity.** The run writes as the producer `@<run id>`. Each Workflow
  Task has its own producer attempt, and the sequence restarts at 1 for
  each attempt.
- **Failed tasks.** If a Workflow Task fails or times out after some of its
  activations completed, their records stay in the store. The task's rerun
  publishes again under a higher attempt, and readers see `SUPERSEDED` for
  the failed task's records, then the rerun's. Records of tasks that
  completed are never withdrawn. `SupersessionTracker` applies this rule for
  a reader.
- **Store outage.** If the store fails the append, the Workflow Task fails
  with `StreamOutputNotAppended` and retries, as for any task failure. The
  run makes no progress until the store takes the records.
- **Closed topic.** If the store reports the topic closed, a retry could
  never land, so Core drops that topic's records with a warning and the task
  goes on. A Workflow can't see this through its writer.
- **Run end.** The records of the activation that completes, fails, cancels
  or continues the run as new land with that completion. `finish()` writes
  `FINISH` with the activation. `flush()` returns at once, since nothing
  waits in the Workflow.
- **Replay.** Replay sends no records, since the store has them. A
  `Replayer` needs the store plugin in `plugins=`, which adds the
  interceptor and connects nothing.
- **Queries and validators.** Opening a writer or publishing there raises
  `ReadOnlyContextError`.
- **Codec.** The values are converted in the Workflow. The payload codec
  runs on each record body when the Worker encodes the completion.
- **Size.** A record holds at most 1.5 MiB, and one activation's records at
  most 16 MiB. The publish that crosses either raises `StreamError` and
  publishes nothing.

Known limits:

- **One output stream per agent.** A Workflow publishes only to its own
  stream. Use one topic per kind of output, for example `steps` and
  `tokens`, and write to another Workflow's stream from an Activity.
- **The Worker needs the store.** Core on the Workflow's Worker does the
  append, so every Worker that runs the Workflow needs the store plugin on
  its client.
- **No check on replay.** Replay doesn't compare the records the run
  publishes with the store, so a Workflow change that publishes other
  records replays without an error.
- **Histories of the Local Activity writer.** An earlier version of
  `workflow_writer` sent records through Local Activities. Its Histories
  have a marker per flush, and they fail replay on this version with a
  nondeterminism error.

## A reader sleeps while the stream is idle

With `notify_on_append()` on the store and a server with the stream
notifier, an idle reader sleeps instead of holding a store read open. A
client's `StreamHandle.read()` waits on the notifier with a long poll.
Workflow code reads with `workflow_reader()`, one Local Activity per batch,
and wakes on a Workflow Task when a producer appends:

```python
@workflow.defn
class Follows:
    @workflow.run
    async def run(self, owner: str) -> None:
        async with workflow_reader(owner, topic=EVENTS) as reader:
            async for batch in reader:
                for record in batch.records:
                    ...
```

By default History keeps each read's range and digest, not its records, and
replay reads them from the store again. The store keeps what the reader read
for `floor_lease` (the retention by default) after its last read or replay,
so a run that goes longer than that should Continue-as-New.
`replay_from_store=False` records each batch in History, for a History that
replays with no store. See
[Positions or recorded](docs/notifications.md#positions-or-recorded) to choose.

With `notify_on_append(notification_log=True)`, producers make no call to
Temporal, and a `StreamDriver` you run forwards the notifications from the
store. See [Run a StreamDriver](docs/notifications.md#run-a-streamdriver).

A Workflow closes its own stream with `close_workflow_stream(topic,
result=..., error=...)`, and each sleeping reader sees the outcome. See
[Readers that sleep](docs/notifications.md).

## What to know

- **Retries deduplicate.** An append that fails with
  `StreamOutcomeUnknownError` can be repeated on the same producer. The store
  returns the original position and writes nothing twice.
- **Retention.** A stream keeps records for `retention` (7 days by default,
  set on `RedisStreams`) and dies that long after its last write. A cursor
  past dropped records raises `StreamExpiredError`.
- **Closing.** Once the Workflow's run chain ends, appends are refused with
  `StreamClosedError`, after a short closing window. Close earlier with
  `close_workflow_stream` in the Workflow or `close()` on a handle. A
  Workflow doesn't see a close from outside through its writer: Core drops
  its later records on that topic, and the run goes on. So close a topic
  from the Workflow that owns it, or have readers check how it closed.
- **Readers before the owner.** A read of a Workflow that Temporal doesn't
  know yet waits for `owner_start_grace` (60 seconds by default, set on
  `RedisStreams` or `MemoryStreams`), counted from the read's first owner
  check. So a reader can start before the Workflow does. After the grace,
  the read raises `StreamNotFoundError`, so a wrong Workflow id fails after
  the grace, not at once. Appends and `latest()` don't wait. Zero turns the
  grace off.
- **Deleting.** Deleting a Workflow does not delete its streams.
  `RedisStreams.delete_workflow_streams` removes them at once.
- **Not in this release.** Streams owned by an Activity or by no one raise
  `StreamUnsupportedError`.

## Further reading

- [Readers that sleep](docs/notifications.md)
- [Redis store: guarantees and setup](docs/redis-guarantees.md)
- [Stream wire contract](docs/wire-contract.md)
- [Moving from Workflow Streams](docs/migration.md)
