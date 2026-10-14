# Temporal Streams

> This package is experimental and may change in future versions.

A stream is an ordered log that a Workflow owns. The Workflow publishes to
it through Local Activities, and its Activities and any client append to
it. Outside readers follow it from a cursor. Records live in the
application's Redis and never pass through Temporal or its History. Core
runs the store for every SDK, so Python reads what any other SDK writes.
Appends add nothing to History. A Workflow's flush adds one Local Activity
marker.

## Register

Register the store once, on the client, with `Client.connect`. Connecting
the client connects the store, and the Activities of every Worker built
from that client append through it. There's nothing extra to install.

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
- **Describe permission.** The Worker's identity needs permission to
  describe the Workflow.
- **Ford removes the call.** Workflow code doesn't see the Workflow Task
  attempt, so the server is asked for it. Ford's Core can supply the task
  attempt to the Local Activity, which removes the call.
- **Attempt range.** A Local Activity past attempt 9,999, or in a Workflow
  Task past attempt 99,999, raises `StreamUnsupportedError` on its first
  append.

## A Workflow publishes to its own stream

Workflow code can't do I/O, so `workflow_writer` buffers each value in the
Workflow and sends the buffer through a built-in Local Activity. The store
plugin registers that Activity on every Worker built from the client, so
there's nothing to register.

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
  the Workflow's payload converter, buffers it and returns.
- **Batching.** One background coroutine per topic sends what is buffered
  through one Local Activity, one flush at a time. Values published while a
  flush runs go in the next one. A Local Activity costs one Action for the
  Workflow Task that carries its marker, however many it carries, so a
  Workflow Task's publishes cost about one Action.
- **Order.** Flushes on a topic run one at a time, so records land in the
  order they were published, across all writers on the topic in the run.
- **Visibility.** A record is visible once its flush's append is accepted.
  That is while the Workflow Task is still running, for example while a
  model call runs as another Local Activity. `await steps.flush()` waits
  for everything published so far, and returns the cursor of the last
  record.
- **Run end.** The store plugin's Workflow interceptor flushes every buffer
  before the run returns, fails, is cancelled or continues as new, so what
  was published lands first. If that flush fails for good, the Workflow
  fails with its error in place of a return. `finish()` flushes and writes
  `FINISH`.
- **Identity.** Each flush is a Local Activity producer of its own (see
  above). After a failed Workflow Task, the flush runs again in a higher
  attempt, so readers see `SUPERSEDED` for that flush, then its records
  again.
- **Replay.** Each flush's result comes from its marker, so a replay does
  no store I/O. A `Replayer` needs the store plugin in `plugins=`, which
  adds the interceptor and connects nothing.
- **Queries and validators.** Opening a writer or publishing there raises
  `ReadOnlyContextError`.
- **Codec.** The values are converted once, in the Workflow, and passed raw.
  The payload codec runs once on each record body, when the Local Activity
  stores it. The values ride in the Local Activity's input, not in History.
- **Size.** A record holds at most 1.5 MiB. A flush carries at most 1,000
  records or 2 MiB, and a larger buffer splits into several flushes.

Known limits:

- **One describe call per flush.** As for any Local Activity producer, each
  flush makes one `DescribeWorkflowExecution` call before its append.
- **Local Activity rules.** The flush runs on the Workflow's Worker, so that
  Worker needs the store. A retry that backs off longer than
  `local_retry_threshold` waits on a Workflow timer. By default a flush
  retries until the store answers, and the run can't end until it lands.
- **Readers track 1,000 producers.** Each flush is a producer. A read keeps
  the newest attempt of its last 1,000 producers, so it misses
  `SUPERSEDED` only for a flush that reruns after 1,000 newer flushes.

## A Workflow publishes through an Activity

`workflow_activity_writer` runs a regular Activity for each call instead.
It adds the Activity's events to History and can't batch across calls, but
the Activity can run on another task queue, on Workers that hold the store.

```python
PROGRESS = topic("progress", dict)

@workflow.defn
class ProcessOrder:
    @workflow.run
    async def run(self, order_id: str) -> str:
        progress = workflow_activity_writer(PROGRESS)
        for step in ("reserved", "charged", "shipped"):
            await progress.publish({"order": order_id, "step": step})
        await progress.publish_batch([{"order": order_id, "step": "done"}])
        await progress.finish()
        return "done"
```

- **Order.** Each call waits for its Activity. Calls on one topic of a run
  go one at a time, in the order they were made, even across writers.
- **Visibility.** A record is visible once the Activity's append is
  accepted. That is after the Workflow Task that scheduled the Activity is
  accepted, so a failed task publishes nothing.
- **Identity.** The Activity writes as itself, like any Activity. A retry
  starts a new attempt, so readers see `SUPERSEDED` before the records the
  retry wrote. An append whose outcome is unknown is repeated in the same
  attempt, and the store writes it once.
- **Options.** `task_queue` picks where the Activity runs (the Workflow's
  task queue by default). `start_to_close_timeout` is 30 seconds by
  default. `schedule_to_close_timeout` and `retry_policy` default to
  Temporal's Activity defaults, so a publish retries until the store
  answers. A closed stream fails the publish at once.
- **Codec.** The values are converted once, in the Workflow, with its
  payload converter. The payload codec encodes them in the Activity's input,
  and again on each record body when the Activity stores it.
- **Size.** A record holds at most 1.5 MiB. A batch rides in the
  Activity's input, so the server's payload size limit bounds it.

## What to know

- **Retries deduplicate.** An append that fails with
  `StreamOutcomeUnknownError` can be repeated on the same producer. The store
  returns the original position and writes nothing twice.
- **Retention.** A stream keeps records for `retention` (7 days by default,
  set on `RedisStreams`) and dies that long after its last write. A cursor
  past dropped records raises `StreamExpiredError`.
- **Closing.** Once the Workflow's run chain ends, appends are refused with
  `StreamClosedError`, after a short closing window.
- **Deleting.** Deleting a Workflow does not delete its streams.
  `RedisStreams.delete_workflow_streams` removes them at once.
- **Not in this release.** Reading a stream inside a Workflow, and streams
  owned by an Activity or by no one, raise `StreamUnsupportedError`.

## Further reading

- [Redis store: guarantees and setup](docs/redis-guarantees.md)
- [Stream wire contract](docs/wire-contract.md)
- [Moving from Workflow Streams](docs/migration.md)
