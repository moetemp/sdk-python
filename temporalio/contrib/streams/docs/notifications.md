# Readers that sleep

A reader that waits for the next record can sleep while the stream is idle,
instead of holding a store read open. The server's stream notifier wakes it
when a producer appends, or when the stream closes. Two readers sleep this
way in one namespace:

- A client reader, `StreamHandle.read()`, waits on the notifier with a long
  poll.
- A Workflow reader, `workflow_reader()`, reads in batches through Local
  Activities and wakes on a Workflow Task.

Both need a server with the stream notifier, and producers that notify.

## Server setup

The notifier is a CHASM component. Turn it on with these dynamic config
settings:

```yaml
history.enableChasm:
  - value: true
history.enableCHASMCallbacks:
  - value: true
streamnotifier.enabled:
  - value: true
```

These settings change how readers wake. The defaults are right for most
deployments:

| Setting | Default | What it does |
|---------|---------|--------------|
| `streamnotifier.pollMaxWait` | 60 s | How long a client reader's poll waits. At the limit the reader reads again |
| `streamnotifier.recheckInitial` | 1 min | When the notifier first asks an idle Workflow reader to read again |
| `streamnotifier.recheckMax` | 1 h | The longest gap between two such asks. The gap doubles up to it |
| `streamwake.minInterval` | 1 s | The shortest time between two Workflow Tasks that only carry wakes |
| `history.maximumStreamWakeSources` | 32 | How many readers one Workflow execution can have |
| `streamnotifier.maxCallbacks` | 2,000 | How many Workflow readers one stream can have |
| `streamnotifier.ownerCheckInterval` | 30 s | How often the notifier checks a stream's owner |
| `streamnotifier.idleTimeoutNoCallbacks` | 1 h | How long a notifier with no reader stays |

## Turn notifications on

```python
store = RedisStreams("redis://localhost:6379").notify_on_append()
client = await Client.connect("localhost:7233", plugins=[store])
```

Turn it on in every process that appends or reads. A client reader sleeps
on the notifier only when its own store has it on. A Worker built from the
client waits for the notifications still out when it stops. A process that
holds only a client calls `await store.flush_notifications()` before it
exits.

A reader that can't arm, such as a hand-written reader over HTTP, waits for
a notification that never comes. For such readers, use
`notify_on_append(every_append=True)`. Then every append notifies.

## When Core notifies

A producer sends nothing to Temporal while no reader sleeps.

1. A reader reads with `arm` and waits up to its idle time, 2 seconds by
   default. A read that finds records returns them at once.
2. A read that waits its whole idle time and finds nothing arms the stream
   in the store, in the same step. It answers `wake_after`, the counter of
   its position.
3. The reader sleeps until the notifier's counter is above `wake_after`, the
   stream closes, or a time limit passes.
4. The next append finds the arm and notifies the notifier once. Core clears
   the arm after the notification lands. A failed notification keeps the
   arm, so the next append notifies again.
5. The notifier wakes every sleeping reader with one notification.

So a busy stream, with no gap longer than the idle time, costs no
notification. A busy stream sends its last counter again once a day, so its
notifier does not reach its idle timeout.

## A client reader

`StreamHandle.read()` keeps its API. With notifications on, an idle read
sleeps on the notifier:

```python
handle = get_stream_handle(client, workflow_id, topic=EVENTS)
async for record in handle.read():
    ...
```

The sleep holds one gRPC long poll, not a store connection. At the poll
limit the reader reads again with `arm`. That read finds records whose
notification was lost, or arms again. On a server without the notifier, the
reader waits in the store instead, as without notifications.

## A Workflow reader

```python
@workflow.defn
class Connector:
    @workflow.run
    async def run(self, channel: str, after: StreamPosition | None) -> None:
        async with workflow_reader(
            channel, topic=MESSAGES, after=after or BEGINNING
        ) as reader:
            async for batch in reader:
                for record in batch.records:
                    if record.kind is RecordKind.DATA:
                        await workflow.execute_activity(post, record.value, ...)
                if workflow.info().is_continue_as_new_suggested():
                    workflow.continue_as_new(args=[channel, batch.position])
```

- Each read is one Local Activity, which adds one marker event when its
  Workflow Task completes. Replay takes the batch from the marker.
- The first read attaches the run to the stream's notifier. While the stream
  is idle, the reader waits with no Local Activity and no timer. The server
  records each wake on the next Workflow Task's scheduled event, so a wake
  adds no event of its own.
- A batch holds every record kind, `SUPERSEDED` and `FINISH` too, since
  replay must give your code the same records. `SupersessionTracker` names
  the records each `SUPERSEDED` withdraws.
- `batch.position` carries the cursor and Core's read state. Pass it as
  `after=` in the next run, for example after Continue-as-New.
- `async with` detaches a reader that stops before the stream ends.
- A reader that finds records on every read ends its Workflow Task after 100
  commands or 2 seconds, so your Activities and timers do not wait for the
  reads. Change it with `cutoff=TaskCutoff(commands=..., time=...)`.
- A read that fails for good, for example on an owner that does not exist,
  fails with an `ApplicationError` whose type names the stream error.

## Closing a stream

| Who closes | How |
|------------|-----|
| The owner Workflow | `await close_workflow_stream(topic, result=..., error=...)` |
| An Activity | `await activity_handle().close(result, error=...)` |
| A client | `await get_stream_handle(client, workflow_id).close(result, error=...)` |
| Nobody | The owner's run chain ends. The notifier's owner check closes the stream |

`close_workflow_stream` lands what the topic's writer buffered and writes
`FINISH` first. Then one Local Activity closes the store and the notifier.
Every read ends after the last record. A sleeping reader wakes. The store
keeps the first close's outcome and hands it to every reader that reaches
the end, so a client read raises the close's error after its last record,
and a Workflow reader raises it after its last batch. A close with a result
ends the read with no error.

Close explicitly. Without a close, the notifier's owner check closes the
stream up to `ownerCheckInterval` plus `ownerCloseGrace` after the owner
ended, and a client reader ends at its next read.

## A lost notification

A producer can stop after its append and before its notification lands. The
arm stays in the store, so the next append by any producer notifies. For
the last append before a quiet period, a time-based check finds the record:

- A client reader reads again at the poll limit (60 seconds).
- The notifier asks an idle Workflow reader to read again after 1 minute,
  doubling to 1 hour. That costs about 30 Workflow Tasks a day per idle
  reader.

## Live tests

`scripts/streams_live_lane.sh` starts a server with the notifier and the
settings above, with short poll and recheck limits, and runs the stream
tests against it. It fails when a live test skips or when a gated test did
not pass.

```bash
SERVER_BIN=/path/to/temporal-server PORT_BASE=7941 \
  STREAMS_REDIS_URL=redis://127.0.0.1:6441/0 bash scripts/streams_live_lane.sh
```
