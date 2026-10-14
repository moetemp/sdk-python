# Temporal Streams

> This package is experimental and may change in future versions.

A stream is an ordered log that a Workflow owns. The Workflow publishes to
it through a built-in Activity, and its Activities and any client append to
it. Outside readers follow it from a cursor. Records live in the
application's Redis and never pass through Temporal or its History. Core
runs the store for every SDK, so Python reads what any other SDK writes.
Appends add nothing to History. A Workflow's publish adds the events of one
Activity.

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

## A Workflow publishes through an Activity

Workflow code can't do I/O, so `workflow_activity_writer` runs a built-in
Activity for each call. The store plugin registers that Activity on every
Worker built from the client, so there's nothing to register.

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
