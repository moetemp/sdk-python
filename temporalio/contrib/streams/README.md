# Temporal Streams

> This package is experimental and may change in future versions.

A stream is an ordered log that a Workflow owns. The Workflow publishes to
it, and so do its Activities and any client. Outside readers follow it from
a cursor. Records live in the application's Redis and never pass through
Temporal or its History. Core runs the store for every SDK, so Python reads
what any other SDK writes. A Workflow Task that publishes adds one marker
event to History, which carries a small manifest of what it published and
counts toward the History limits. Appends from Activities and clients add
nothing to History. A publish through `workflow_activity_writer` adds the
events of one Activity instead.

## Register

Register the store once, on the client, with `Client.connect`. Connecting
the client connects the store, and every Worker built from that client
publishes through it. There's nothing extra to install.

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

## Path A: a Workflow publishes, a client reads

Define a topic once with the type its records decode to, publish from the
Workflow, and read from a client. A Workflow's own publish becomes visible
when its Workflow Task is accepted, and never if the task fails.

```python
PROGRESS = topic("progress", dict)

@workflow.defn
class ProcessOrder:
    @workflow.run
    async def run(self, order_id: str) -> str:
        progress = workflow_writer(PROGRESS)
        for step in ("reserved", "charged", "shipped"):
            progress.publish({"order": order_id, "step": step})
            await workflow.sleep(timedelta(milliseconds=100))
        progress.finish()
        return "done"

async for record in get_stream_handle(client, "order-1").read(topic=PROGRESS):
    if record.kind is RecordKind.DATA:
        print(record.value["step"])
```

The read ends when the Workflow's run chain closes. It follows
Continue-as-New, and `record.run_id` says which run published each record.
To resume later, keep the last `record.cursor` and pass it back as
`read(after=cursor)`.

## Path B: an Activity streams to its Workflow

An Activity writes to the stream of the Workflow that scheduled it, as
itself: its Activity id with the scheduling run id, and its Temporal
attempt. The run id keeps two runs of a chain apart, since Activity ids
restart in each run. When a retry starts writing, readers first see a
`SUPERSEDED` record, so they can drop what the failed attempt wrote.

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

`SUPERSEDED` is per producer. This topic has one, so clearing everything is
right. With several producers on a topic, keep the text per
`record.producer_id` and clear only that producer's.

Both samples run in full in `tests/contrib/streams/samples/`.

## A Workflow publishes through an Activity

`workflow_writer` publishes with the Workflow Task. `workflow_activity_writer`
runs a built-in Activity for each call instead. The store plugin registers
that Activity on every Worker built from the client, so there's nothing to
register.

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
- **Limits per task.** A Workflow's record holds at most 1.5 MiB, and one
  activation publishes at most 16 MiB of records. One Workflow Task can
  publish to a few hundred topics before its manifest reaches the budget.
  The publish that would cross a limit raises `StreamError`. Catch it.
  Uncaught, it fails the Workflow Task, and every retry fails the same way.
- **Not in this release.** Reading a stream inside a Workflow, and streams
  owned by an Activity or by no one, raise `StreamUnsupportedError`. A
  Workflow can read through a stream-returning Nexus operation with
  `StreamReader` (see [Streams over Nexus](docs/nexus.md)).

## Further reading

- [Redis store: guarantees and setup](docs/redis-guarantees.md)
- [Stream wire contract](docs/wire-contract.md)
- [Moving from Workflow Streams](docs/migration.md)
- [Streams over Nexus](docs/nexus.md)
