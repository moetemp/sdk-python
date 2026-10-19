# Temporal Streams

> This package is experimental and may change in future versions.

A stream is an ordered log that a Workflow owns. The Workflow publishes to
it from Workflow code, and its Activities and any client append to it.
Outside readers follow it from a cursor. Records live in the application's
Redis and never pass through Temporal or its History. The
stream runtime, the `temporalio-streams` crate from the sdk-streams
repository, runs the store for every SDK, so Python reads what any other SDK
writes. Publishes and
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

The store tells the server's stream notifier after each append, which is
what wakes a reader that sleeps on the stream. An append answers once its
notification went. Keep it on in every process that appends. In a process
whose streams have no sleeping reader, `RedisStreams(...).notify_on_append(False)`
turns it off.

Redis 7.0 or later is required. Use `maxmemory-policy noeviction`: an
evicting Redis drops whole stream keys under memory pressure, and the stream
runtime logs a warning when it sees another policy. For tests and local
development, `temporalio.contrib.streams.memory.MemoryStreams` keeps streams
in the process.

## Path B: an Activity streams to its Workflow

An Activity writes to the stream of the Workflow that scheduled it, as
itself: its Activity id with the scheduling run id. Its producer attempt is
`started_ms << 11 | min(attempt, 2047)`, from the time the server started
the attempt and its Temporal attempt, so a retry and an attempt after a
reset write a higher one. The run id keeps two runs of a chain apart, since
Activity ids restart in each run. When a retry starts writing, readers
first see a `SUPERSEDED` record, so they can drop what this Activity's
failed attempt wrote.

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

Every read follows the Workflow's run chain across Continue-as-New and ends
when the chain closes. A reader can't stop at one run. A Workflow id can
start a new chain after the old one closed, and a handle opens the newest
chain unless `chain_run_id=` names a run of another one. An Activity's
`record.producer_id` names the run that scheduled it.
To resume later, keep the last `record.cursor` and pass it back as
`read(after=cursor)`. An idle read waits `idle_wait` (2 seconds by default)
in the store before it sleeps on the stream's notifier, so a longer
`read(idle_wait=...)` holds a store connection longer and sleeps less often.

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

## Streams owned by a standalone Activity

A standalone Activity is one that a client starts with `start_activity`,
with no Workflow. It owns streams of its own. In a standalone Activity,
`activity_handle()` is the Activity's own stream, and the producer and
close calls are the ones a Workflow Activity uses.

```python
@activity.defn
async def generate(prompt: str) -> str:
    stream = activity_handle()
    out = stream.producer(topic=TOKENS)
    async for token in model(prompt):
        await out.append(token)
    await stream.close(topic=TOKENS, result="done")
    return "ok"

handle = await client.start_activity(generate, "hi", id="gen-1", ...)
ref = StreamRef.for_activity(handle.id, run_id=handle.run_id, topic=TOKENS)
async for record in stream_handle(client, ref).read():
    ...
```

- **Owner.** The stream belongs to one Activity run. The ref names the
  Activity id and its run. A ref with no run names the newest run of the id
  when a read starts, and the read stays on that run. A new run of the same
  Activity id has new streams.
- **Readers.** A client reads with `stream_handle(client, ref)`, and
  Workflow code with `workflow_reader(ref)`, as for a Workflow's stream.
- **Producer.** The Activity writes as `<Activity id>@<run id>`. Retries,
  backoff, pause and reset keep the stream open.
- **Attempt.** Each attempt writes the producer attempt
  `started_ms << 11 | min(attempt, 2047)`, where `started_ms` is the Unix
  time in milliseconds at which the server started the attempt. A retry and
  an attempt after a reset start later, so readers see `SUPERSEDED` before
  their records. An attempt that timed out but still writes started
  earlier, so its late records are `stale`. Workflow Activities use the same
  rule.
- **Close.** The Activity closes its stream with `close(result=)` or
  `close(error=)`. The SDK closes nothing when the Activity returns, so make
  the close the last step. When the run ends with no close, the stream's
  notifier on the server closes it within its owner check interval (30
  seconds by default). A completed run gives a null result. A failed,
  canceled, terminated or timed-out run gives the run's own failure, which a
  Workflow reader raises after its last batch. A client reader that the
  notifier did not wake ends with no outcome.
- **Codec.** Records and closes go through the codec with an
  `ActivitySerializationContext` that holds only the namespace and the
  Activity id, since a reader can build no more from the ref.
- **Server.** The server must run standalone Activities
  (`activity.enableStandalone`) and the stream notifier.
- **Deleting.** `delete_activity_streams(namespace, activity_id)` removes
  the streams of every run of an Activity id.

A standalone Activity reads and follows other streams as a client does. To
go on after a retry, heartbeat the cursor of each record it handled, and
read after the cursor in the heartbeat details:

```python
@activity.defn
async def follow(workflow_id: str) -> int:
    details = activity.info().heartbeat_details
    after = Cursor(details[0]) if details else BEGINNING
    seen = 0
    async for record in stream_handle(activity.client(), workflow_id).read(
        after=after
    ):
        seen += 1
        activity.heartbeat(record.cursor.token)
    return seen
```

Known limits:

- **Clock skew.** The attempt uses the server's attempt start time. After a
  failover to a cluster whose clock is behind by more than the retry
  interval, a new attempt can look older, and its records are `stale`.
- **Workflow Activities.** A Workflow Activity writes to its Workflow's
  stream unless the Workflow started it to own one. See the next section.

## Streams owned by a Workflow Activity

An Activity that a Workflow schedules can own a stream too, for example a
tool call of an agent. The Workflow starts it with
`start_activity_with_stream`, which takes the arguments of
`workflow.start_activity` and a `topic`, and returns the handle and the
stream's ref. In that Activity, `activity_handle()` is the Activity's own
stream.

```python
@workflow.defn
class Agent:
    @workflow.run
    async def run(self, call: ToolCall) -> str:
        handle, ref = start_activity_with_stream(
            run_tool, call, activity_id=f"tool-{call.id}",
            start_to_close_timeout=timedelta(minutes=5), topic=TOKENS,
        )
        return await handle

@activity.defn
async def run_tool(call: ToolCall) -> str:
    out = stream_producer(TOKENS)
    async for token in tool(call):
        await out.append(token)
    return "ok"

ref = StreamRef.for_workflow_activity(
    agent_id, run_id=run_id, activity_id="tool-42", topic=TOKENS
)
async for record in stream_handle(client, ref).read():
    ...
```

- **Owner.** The stream belongs to one Activity in one Workflow run. The ref
  names the Workflow id, the run that scheduled the Activity and the
  Activity id. The run is required, since Activity ids such as `"1"` repeat
  in each run of a chain.
- **Picking the stream.** `start_activity_with_stream` marks the Activity
  with the header `__temporal_stream_owner`. With the mark,
  `activity_handle()` is the Activity's own stream. Without it, it is the
  Workflow's stream. `activity_handle(scope="activity")` and
  `activity_handle(scope="workflow")` pick one. A Local Activity can't own a
  stream, and `scope="activity"` raises `StreamUnsupportedError` there.
- **Producer.** The Activity writes as `<Activity id>@<run id>`, with the
  attempt rule of a standalone Activity. A retry and an Activity reset
  write to the same stream, and readers see `SUPERSEDED`.
- **Close.** The Activity need not close its stream. When the Workflow sees
  the Activity end, its Worker closes each topic the Activity wrote. A
  completed Activity gives a null result. A failed, timed-out or canceled
  Activity gives the failure that History holds, which a Workflow reader
  raises after its last batch. The Activity may close a topic itself first,
  with its own result.
- **Backstop.** If the Workflow's Worker never closes the stream, for
  example since it stopped right after the Activity ended, the stream's
  notifier closes it with a generic failure. It does this once the
  Activity is no longer pending, after `streamnotifier.ownerStartGrace`
  from the notifier's start and `streamnotifier.ownerCloseGrace` from the
  first check that found the Activity gone. A read that starts before the
  Activity is scheduled waits for `owner_start_grace`.
- **Runs.** Continue-as-New and a Workflow reset make a new run, so the
  rerun Activity writes a new stream, and a cursor of the old stream is
  refused on it. A second Activity with the same id in the same run writes
  to a closed stream, and its first append raises `StreamClosedError`.
- **Codec.** Records and closes go through the codec with an
  `ActivitySerializationContext` that holds the namespace, the Activity id
  and the Workflow id, and no other field.
- **Deleting.** `delete_workflow_streams` removes the streams of the
  Workflow's Activities too.

Known limits:

- **Cancel.** With cancellation types `TRY_CANCEL` and `ABANDON`, the
  Workflow sees the cancel at once, while the Activity may still run. The
  stream closes then with the canceled failure, and later appends raise
  `StreamClosedError`.
- **Owner check cost.** The owner check reads the run's pending
  Activities. A run with thousands of them makes each check larger.
- **Codec keys.** A codec that keys on the Activity type or task queue
  can't decode the close outcome.

## Link records and followers

A stream can hold a reference to another stream, as a `LINK` record. A
follower reads the stream and each stream its links open in one order. So
an agent writes a link per tool call or child agent in place of copying
their records, and a reader still sees one ordered view.

```python
# Workflow code (agent). Links go out with the activation, as published values do.
events = stream_producer(EVENTS)
handle, tool_ref = start_activity_with_stream(run_tool, call, topic=TOKENS, ...)
events.link(tool_ref, name=call.id)                       # OPEN
events.link(StreamRef.for_workflow(child_id), name="sub-3", after=child_cursor)
await handle
events.join(name=call.id)                                 # JOIN, the agent waited here

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
  `join(*, name, until=None)` writes a `JOIN`. It says the producer waited for the link
  up to `until`, or to the linked stream's end. A tool stream needs no `JOIN`
  for its segment to end, since its stream closes with the call.
- **Mixed batches.** `append` also takes a `StreamLink` next to plain values,
  so `append(event, StreamLink(LinkEvent.OPEN, ref, name))` writes the event
  and its link as one batch, with one dedupe identity.
- **Order.** Each stream keeps its own order. A linked stream's records
  never come before their `OPEN`. A record that comes after a `JOIN` waits
  until the linked stream reached the join point or its end. Other records
  come in the order they were read. No clock decides the order.
- **Link reports.** Items with `link_state` say what happened to a link.
  `OPENED` when a segment starts. `DONE` with the linked stream's close
  outcome when it ends. `UNAVAILABLE` with the error when it can't be read,
  for example a Workflow that never started, after `owner_start_grace`.
  `SKIPPED` with a reason: `depth` past `max_depth` (4), `limit` past
  `max_open` (16), `cycle` for a link back to a stream on the path, `open`
  for a stream already open, and `endpoint` for a stream behind another
  Nexus endpoint. A linked stream never fails the follow.
- **Positions.** Each record keeps its own stream's cursor. The last item of
  each batch carries a `FollowPosition`, which resumes the root and every
  open segment with no loss and no repeat. To resume from an earlier item,
  resume from the previous batch end.
- **Workflow followers.** `workflow_follower(ref, topic=EVENTS)` gives the
  same items in batches, one Local Activity per step, and sleeps on the
  wakes of every open source. Each open source is one subscription of the
  Workflow. By default each step's marker holds its ranges and a digest,
  and replay reads them again through the stream runtime.
- **Retries.** An attempt owns the `OPEN`s it writes. A `SUPERSEDED` ends
  each segment the withdrawn attempt opened, with `DONE` and reason
  `withdrawn`. The same `OPEN` from a later attempt of that producer, with
  the same name, stream and `after`, resumes the segment where it paused,
  with `OPENED` and reason `resumed`. So a retry that writes the same `OPEN`
  again shows each linked record once. Another `OPEN` of the name reads the
  stream from its `after`.
- **Plaintext.** A link's ref, name and cursors are stored as plaintext, as
  Workflow and Activity ids are in History. A link's value goes through the
  codec, so put secrets there.

Known limits:

- **Size.** The follow position grows with the open segments. With the
  default bounds it stays below 64 KiB.
- **Workflow followers.** A Workflow follower with more open sources than
  its free wake sources gets `SKIPPED` with reason `limit` for the rest.
- **Withdrawn joins.** A `JOIN` from a withdrawn attempt stays in effect,
  since its segment ended before the `SUPERSEDED`.

## A Workflow publishes to its own stream

`stream_producer` is one function for Workflow code and Activity code. In
an Activity it returns `activity_handle(scope).producer(topic=...)`, and
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

## A reader sleeps while the stream is idle

On a server with the stream notifier, an idle reader sleeps instead of holding a store read open. A
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

A Workflow closes its own stream with `close_workflow_stream(topic,
result=..., error=...)`, and each sleeping reader sees the outcome. See
[Readers that sleep](docs/notifications.md).

## What to know

- **Retries deduplicate.** An append that fails with
  `StreamOutcomeUnknownError` can be repeated on the same producer. The store
  returns the original position and writes nothing twice. On a store without
  producer state, such as `RedisStreams(plain=True)`, the copy lands and
  every reader drops it. `StreamStorePlugin.capabilities` says which.
- **Retention.** A stream keeps records for `retention` (7 days by default,
  set on `RedisStreams`) and dies that long after its
  last write. A cursor
  past dropped records raises `StreamExpiredError`.
- **Closing.** Once the Workflow's run chain ends, appends are refused with
  `StreamClosedError`, after a short closing window. Close earlier with
  `close_workflow_stream` in the Workflow or `close()` on a handle. A
  Workflow doesn't see a close from outside through its producer: the stream runtime drops
  its later records on that topic, and the run goes on. So close a topic
  from the Workflow that owns it, or have readers check how it closed. On a
  store without producer state, a late append can still answer success, and
  no reader delivers it, since every read ends at the first `CLOSE` record.
- **Readers before the owner.** A read of a Workflow that Temporal doesn't
  know yet waits for `owner_start_grace` (60 seconds by default, set on
  `RedisStreams` or `MemoryStreams`), counted from the read's first owner
  check. So a reader can start before the Workflow does. After the grace,
  the read raises `StreamNotFoundError`, so a wrong Workflow id fails after
  the grace, not at once. Appends and `latest()` don't wait. Zero turns the
  grace off.
- **Deleting.** Deleting a Workflow does not delete its streams.
  `delete_workflow_streams` on the store removes them at once, and
  `delete_activity_streams` removes the streams of a standalone Activity.
- **Not in this release.** Streams owned by no one raise
  `StreamUnsupportedError`.

## Further reading

- [Readers that sleep](docs/notifications.md)
- [Store capabilities and guarantees](docs/store-capabilities.md)
- [Redis store: guarantees and setup](docs/redis-guarantees.md)
- [Stream wire contract](docs/wire-contract.md)
- [Moving from Workflow Streams](docs/migration.md)
