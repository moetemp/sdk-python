# Temporal Streams

> This package is experimental and may change in future versions.

A stream is an ordered log that a Workflow owns. The Workflow publishes to
it from Workflow code, and its Activities and any client append to it.
Outside readers follow it from a cursor. Records live in the application's
Redis and never pass through Temporal or its History. The stream runtime,
the `temporalio-streams` crate from the sdk-streams repository, runs the
store for every SDK, so Python reads what any other SDK writes. Publishes and
appends add nothing to History.

## Register

Register the store once, on the client, with `Client.connect`. Connecting
the client connects the store, and every Worker built from that client
publishes and appends through it. There's nothing extra to install.

A Worker refuses a store passed only to `Worker(plugins=[...])`, or one on a
client made without `Client.connect`, with `ValueError`. The stream runtime
needs the client's connection to check a stream's owner.

```python
from temporalio.client import Client
from temporalio.contrib.streams.redis import RedisStreams

client = await Client.connect(
    "localhost:7233", plugins=[RedisStreams("redis://localhost:6379")]
)
```

Redis 7.0 or later is required. Use `maxmemory-policy noeviction`: an
evicting Redis drops whole stream keys under memory pressure, and the stream
runtime logs a warning when it sees another policy. For tests and local
development, `temporalio.contrib.streams.memory.MemoryStreams` keeps streams
in the process.

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
    producer = stream_producer(TOKENS)
    ...
    await producer.append(*words)
    await producer.finish()

text = []
async for record in stream_handle(client, "answer-1").read(topic=TOKENS):
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
every record it may still withdraw in memory, for the producers it saw most
recently. It keeps at most `max_producers` of them (1,000 by default), and
no more than fit in 16 KiB at each id's length plus 20 bytes. Past that it
drops the producer it saw least recently. The read's state in the stream
runtime forgets producers by the same rule, so a dropped producer gets no
`SUPERSEDED` either. Each Local Activity call is a producer, about 57 bytes
of id each, so on a stream that many calls write the tracker holds about 200
of them and its memory stays steady. For a Workflow's producer, `@<run id>`,
it holds only the records of the newest Workflow Task, since a `SUPERSEDED`
withdraws only the records of one task: the rerun's own, or an earlier one
whose rerun wrote nothing to the topic. Of one producer it keeps the newest
`max_records_per_producer` records (10,000 by default), so a long attempt
can't grow it either. A `SUPERSEDED` past that withdraws only the records
still held. Call `forget(producer_id)` once a producer can't retry, to drop
it sooner.

A future Workflow reader can't drop superseded records silently, because
replay must see the same records, so it will deliver `SUPERSEDED` too.

Every read follows the Workflow's run chain across Continue-as-New and ends
when the chain closes. A reader can't stop at one run. A Workflow id can
start a new chain after the old one closed, and a handle opens the newest
chain unless `chain_run_id=` names a run of another one. An Activity's
`record.producer_id` names the run that scheduled it.
To resume later, keep the last `record.cursor` and pass it back as
`read(after=cursor)`. An idle read waits `idle_wait` (2 seconds by default)
in the store, comes back empty and reads again, so a longer
`read(idle_wait=...)` makes fewer store calls.

The sample runs in full in `tests/contrib/streams/samples/`.

## A Local Activity streams to its Workflow

A Local Activity calls `stream_producer` the same way, and its records
are visible while the Workflow Task that runs it is still open. So an agent
loop that runs its model calls as Local Activities streams their tokens.

A Local Activity's attempt is not durable: after a Worker crash or a failed
Workflow Task, the same call runs again from attempt 1. So it writes with
an identity of its own:

- **Producer id.** `__temporal_local_<n>@<scheduling run id>`, where `n`
  counts the run's Local Activity calls in order. The `__temporal` prefix is
  reserved, so no user Activity id writes as the same producer. The store
  plugin's Worker interceptor stamps the number on each call in a header, so
  it stays the same across reruns, and across a retry that backs off longer
  than `local_retry_threshold`, which gets a new Activity id.
- **Attempt.** Core puts the facts of the Workflow Task in which it
  dispatched the call on the Local Activity's start. It takes them from
  History. The task attempt is `prefix << 16 | retry`, the same value the
  Workflow's own records use. With the Local Activity's attempt `a`, the
  producer writes attempt `task attempt << 14 | min(a, 16383)`. A rerun is
  dispatched in a later Workflow Task than the first run, also when Core
  schedules it while it replays a task that completed as a heartbeat. So the
  attempt grows, and readers see `SUPERSEDED` before the rerun's records. A
  retry inside one attempt is written once, as for any producer.
- **Failed tasks.** The records carry the started event id of the call's
  Workflow Task attempt and the run's last completed tasks, as the
  Workflow's records do. If that task fails and its retry doesn't run the
  call again, the next record of the run on the topic shows that the task
  never completed. Readers then see `SUPERSEDED` for the call's records.

The attempt needs no server call, so the first record goes out at once. The
stream runtime's owner check describes the Workflow, but it caches the
answer per owner run, so the run's Local Activities share it.

Known limit:

- **Old failed tasks.** Each record lists only the run's last eight
  completed tasks. If more than seven tasks complete before the next record
  on the topic, the reader can't prove the call's task failed, and its
  records stand.

## Link records and followers

A stream can hold a reference to another Workflow's stream, as a `LINK`
record. A follower reads the stream and each stream its links open in one
order. So an agent writes a link per child agent in place of copying the
child's records, and a reader still sees one ordered view.

```python
# Workflow code (agent). Links go out with the activation, as appended values do.
events = stream_producer(EVENTS)
events.link(StreamRef.for_workflow(child_id), name="sub-3", after=child_cursor)  # OPEN
reply = await child.execute_update(...)
events.join(name="sub-3", until=reply.turn_end)  # JOIN, the agent waited here

# Client code. One ordered view of the agent and every stream it links.
async for item in stream_handle(client, agent_id, topic=EVENTS).follow():
    if item.link_state is not None:      # OPENED, DONE, UNAVAILABLE or SKIPPED
        ui.mark(item.source, item.link_state)
    else:
        ui.render(item.source, item.record)
    if item.position is not None:
        saved = item.position            # resumes every open link, not only the agent
```

- **Writing.** `StreamProducer.link(ref, *, name, after=None, value=None)`
  and `WorkflowStreamProducer.link(...)` write an `OPEN`. The follower reads
  the linked stream strictly after `after`, or from its oldest record.
  `join(*, name, until=None)` writes a `JOIN`. It says the producer waited
  for the link up to `until`, or to the linked stream's end. A link names a
  Workflow's stream only. Any other ref raises `ValueError`.
- **Mixed batches.** `append` also takes a `StreamLink` next to plain values,
  so `append(event, StreamLink(LinkEvent.OPEN, ref, name))` writes the event
  and its link as one batch, with one dedupe identity.
- **Following.** `StreamHandle.follow()` and `ActivityStreamHandle.follow()`
  read a stream and the streams it links, from client or Activity code.
  Workflow code can't follow.
- **Order.** Each stream keeps its own order. A linked stream's records
  never come before their `OPEN`. A record that comes after a `JOIN` waits
  until the linked stream reached the join point or its end. Other records
  come in the order they were read. No clock decides the order.
- **Link reports.** Items with `link_state` say what happened to a link.
  `OPENED` when a segment starts. `DONE` when it ends. `UNAVAILABLE` with
  the error when it can't be read, for example a Workflow that never
  started, after `owner_start_grace`. `SKIPPED` with a reason: `depth` past
  `max_depth` (4), `limit` past `max_open` (16), `cycle` for a link back to
  a stream on the path, `open` for a stream already open, and `endpoint`
  for a stream in another namespace or behind a Nexus endpoint. A linked
  stream never fails the follow.
- **Positions.** Each record keeps its own stream's cursor. The last item of
  each batch carries a `FollowPosition`, which resumes the root and every
  open segment with no loss and no repeat. To resume from an earlier item,
  resume from the previous batch end.
- **Retries.** `SUPERSEDED` does not take back an expanded link. The link
  record and the withdrawal both reach user code, and
  `SupersessionTracker` withdraws the link.
- **Plaintext.** A link's ref, name and cursors are stored as plaintext, as
  Workflow ids are in History. A link's value goes through the codec, so put
  secrets there.

Known limits:

- **Size.** The follow position grows with the open segments. With the
  default bounds it stays below 64 KiB.
- **Waits.** An idle follow holds one store wait per open source.
- **Old readers.** A reader of an earlier release reads a `LINK` as a
  record it can't decode.

## A Workflow publishes to its own stream

`stream_producer` is one function for Workflow code and Activity code. In
an Activity it returns `activity_handle().producer(topic=...)`, and
`append` is awaited. In a Workflow it returns the Workflow Producer, and
`append` returns at once. Workflow code can't do I/O, so the Workflow
Producer hands its records to the Worker with the activation that appended
them. The Worker appends them to
the store through the stream runtime when the activation ends, before it
gives the completion to Core, so before the Workflow Task goes to the server.

```python
STEPS = topic("steps", str)

@workflow.defn
class Agent:
    @workflow.run
    async def run(self, prompt: str) -> str:
        steps = stream_producer(STEPS)
        steps.append("planning")
        answer = await workflow.execute_local_activity(
            call_model, prompt, start_to_close_timeout=timedelta(minutes=5)
        )
        steps.append("answered")
        return answer
```

- **Append.** `append(value)` is a plain call. It converts the value with
  the Workflow's payload converter and adds the record to the activation.
- **Visibility.** A record is visible when the activation that appended it
  completes. That is before the Workflow Task completes, and before the
  server accepts it. In the example, readers see `planning` while the model
  call runs as a Local Activity in the same Workflow Task.
- **Order.** Records land in the order the run appended them, across all
  producers and topics of the run.
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
  never land, so the stream runtime drops that topic's records with a
  warning and the task goes on. A Workflow can't see this through its
  producer.
- **Run end.** The records of the activation that completes, fails, cancels
  or continues the run as new land with that completion. `finish()` writes
  `FINISH` with the activation. `flush()` returns at once, since nothing
  waits in the Workflow.
- **Replay.** Replay sends no records, since the store has them. A
  `Replayer` needs the store plugin in `plugins=`, which adds the
  interceptor and connects nothing.
- **Queries and validators.** Opening a producer or appending there raises
  `ReadOnlyContextError`.
- **Codec.** The values are converted in the Workflow. The payload codec
  runs on each record body when the Worker publishes the records.
- **Size.** A record holds at most 1.5 MiB. A larger append raises
  `StreamError` and appends nothing. One activation's records have no
  limit in the Workflow, since replay can't check one the same way.

Known limits:

- **One output stream per agent.** A Workflow publishes only to its own
  stream. Use one topic per kind of output, for example `steps` and
  `tokens`, and write to another Workflow's stream from an Activity.
- **The Worker needs the store.** The Workflow's Worker does the append, so
  every Worker that runs the Workflow needs the store plugin on its client.
- **No check on replay.** Replay doesn't compare the records the run
  publishes with the store, so a Workflow change that publishes other
  records replays without an error.

## What to know

- **Retries deduplicate.** An append that fails with
  `StreamOutcomeUnknownError` can be repeated on the same producer. The store
  returns the original position and writes nothing twice. On a store without
  producer state, such as `RedisStreams(plain=True)`, the copy lands and
  every reader drops it. `StreamStorePlugin.capabilities` says which.
- **Retention.** A stream keeps records for `retention` (7 days by default,
  set on `RedisStreams`) and dies that long after its last write. A cursor
  past dropped records raises `StreamExpiredError`.
- **Closing.** Once the Workflow's run chain ends, appends are refused with
  `StreamClosedError`, after a short closing window. On a store without
  producer state, a late append can still answer success, and no reader
  delivers it, since every read ends at the first `CLOSE` record.
- **Readers before the owner.** A read of a Workflow that Temporal doesn't
  know yet waits for `owner_start_grace` (60 seconds by default, set on
  `RedisStreams` or `MemoryStreams`), counted from the read's first owner
  check. So a reader can start before the Workflow does. After the grace,
  the read raises `StreamNotFoundError`, so a wrong Workflow id fails after
  the grace, not at once. Appends and `latest()` don't wait. Zero turns the
  grace off.
- **Deleting.** Deleting a Workflow does not delete its streams.
  `RedisStreams.delete_workflow_streams` removes them at once.
- **Not in this release.** A Workflow can't read a stream. Streams owned
  by an Activity or by no one raise `StreamUnsupportedError`.

## Further reading

- [Store capabilities and guarantees](docs/store-capabilities.md)
- [Redis store: guarantees and setup](docs/redis-guarantees.md)
- [Stream wire contract](docs/wire-contract.md)
- [Moving from Workflow Streams](docs/migration.md)
