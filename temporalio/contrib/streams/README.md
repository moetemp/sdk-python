# Temporal Streams

> This package is experimental and may change in future versions.

A stream is an ordered log that a Workflow owns. Its Activities and any
client append to it. Outside readers follow it from a cursor. Records live
in the application's Redis and never pass through Temporal or its History.
Core runs the store for every SDK, so Python reads what any other SDK
writes. Appends add nothing to History.

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

The read ends when the Workflow's run chain closes. It follows
Continue-as-New, and an Activity's `record.producer_id` names the run that
scheduled it.
To resume later, keep the last `record.cursor` and pass it back as
`read(after=cursor)`.

The sample runs in full in `tests/contrib/streams/samples/`.

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
