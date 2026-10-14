# Streams over Nexus

> This package is experimental and may change in future versions.

A Nexus operation can return a stream instead of a single result. The
handler starts the operation, hands the caller a reference to a stream, and
the stream's producer keeps appending records to it. The caller Workflow
reads the records as they arrive. The operation completes when the stream
closes.

The records never pass through the caller's History. What reaches the
caller is progress: a small note that says the stream moved, with a position
and a counter. The caller then reads the records through the stream service
on the operation's endpoint.

## When to use it

Use a stream-returning operation when a caller in another namespace, or in
another team's service, needs a feed of results while the work runs. For
example a model's tokens, the steps of a long job, or messages for a chat
channel.

Inside one namespace a client, an Activity or a Workflow does not need
Nexus. A client or an Activity reads a stream it knows the reference of
directly, as in the [package README](../README.md), and a Workflow reads it
with `workflow_reader()` (see [Readers that sleep](notifications.md)).

## Server setup

Start with the notifier settings in [Readers that sleep](notifications.md):
`history.enableChasm`, `history.enableCHASMCallbacks` and
`streamnotifier.enabled`. Streams over Nexus also needs these dynamic config
values, for the namespaces involved:

| Setting | Why |
|---|---|
| `nexusoperation.enableChasmWorkflowOperations` and `nexusoperation.chasmWorkflowOperationsRolloutPercent: 100` | The caller's operation must use the CHASM path. Only that path accepts progress. |
| `nexusoperation.enableProgress` | The caller accepts progress deliveries. |
| `nexusoperation.callback.endpoint.template` | The callback URL the caller gives the handler. Point it at the frontend's HTTP address, for example `http://localhost:7243/namespaces/{{.NamespaceName}}/nexus/callback`. |
| `callback.allowedAddresses` | The notifier only calls addresses on this list. Add the frontend's HTTP address, for example `- Pattern: "localhost:7243"` with `AllowInsecure: true` when it serves plain HTTP. |

Without progress, the operation still runs. The caller then gets the
completion only, and its reader sees every record at the end.

Turn on `nexusoperation.enableProgress` and `streamnotifier.enabled` only once
every server in the cluster runs a version that has them. Before a downgrade,
turn them off and let open stream notifiers finish, because an older server
cannot run the tasks they leave behind.

## The handler side

The handler names the stream and returns a `StreamOperationHandler`. Its
`open_stream` function usually starts the Workflow that produces the stream
and returns the stream's reference. The handler never appends; the producer
does.

```python
import nexusrpc
import nexusrpc.handler
import temporalio.nexus
from temporalio import workflow
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.contrib.streams import (
    StreamRef, close_workflow_stream, topic, workflow_writer,
)
from temporalio.contrib.streams.nexus import StreamOperationHandler

MESSAGES = topic("messages", Message)

@nexusrpc.service
class ConversationService:
    respond: nexusrpc.Operation[str, str]

@workflow.defn
class Conversation:
    @workflow.run
    async def run(self, request: str) -> None:
        writer = workflow_writer(MESSAGES)
        for reply in make_replies(request):
            writer.publish(reply)
        await close_workflow_stream(MESSAGES, result="done")

async def open_conversation(ctx, request: str) -> StreamRef:
    workflow_id = f"conversation-{ctx.request_id}"
    await temporalio.nexus.client().start_workflow(
        Conversation.run, request, id=workflow_id,
        task_queue=temporalio.nexus.info().task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )
    return StreamRef.for_workflow(workflow_id, topic=MESSAGES.name)

@nexusrpc.handler.service_handler(service=ConversationService)
class ConversationHandler:
    @nexusrpc.handler.operation_handler
    def respond(self) -> nexusrpc.handler.OperationHandler[str, str]:
        return StreamOperationHandler(open_conversation)
```

`USE_EXISTING` lets a retried start find the Workflow the first attempt
started. The server keys a stream's notifier by the owner's run chain, which
it looks up from the owner Workflow, so the owner must exist when the start
returns and whenever the producer notifies. Starting it in `open_stream`, as
above, covers both.

The store rides the handler Worker's client, with notifications on. The
Worker serves the stream service next to the handler, so the caller reads on
the same endpoint:

```python
from temporalio.client import Client
from temporalio.contrib.streams.redis import RedisStreams   # or MemoryStreams()
from temporalio.contrib.streams.nexus import TemporalStreamsHandler

store = RedisStreams("redis://localhost:6379").notify_on_append()
client = await Client.connect("localhost:7233", plugins=[store])
worker = Worker(
    client,
    task_queue="conversations",
    workflows=[Conversation],
    nexus_service_handlers=[ConversationHandler(), TemporalStreamsHandler()],
)
```

Core serves the stream service. `TemporalStreamsHandler` hands each call to
Core and keeps only what Core can't do: your `authorize` callable and the
namespace's payload codec. It holds nothing between calls, so it needs no
shutdown, and any Worker on the endpoint's task queue can serve a reader's
next read. The reader carries Core's read state in each read instead. So
the reader and the stream service come from the same release. A stream
service from an earlier release refuses the reader's `state` as an unknown
member. A reader from an earlier release on a newer service still works, but
it sends no state, so the service primes from the cursor on every read and
knows only the cursor record's producer.

`notify_on_append()` turns on progress. Without it, the caller sees no
progress, but the close still completes every operation with its result.
Core closes a stream's notifier after every store close, whether or not
progress is on. Core notifies only while a reader sleeps on the stream, so a
busy stream costs no notification. [Readers that sleep](notifications.md)
says when Core notifies and how a process flushes its notifications before
it exits.

Closing the stream completes every operation that handed it out:

- From the owning Workflow, `await close_workflow_stream(topic,
  result=...)`. The topic's writer lands what it buffered first. Then one
  Local Activity closes the stream in the store and then completes the
  operations, so a reader never misses a record published before the close.
- From outside a Workflow, for example an Activity or the process that sees
  the work end, `await store.close_stream(client, ref, result)`. It closes
  the stream in the store first, so a later append raises
  `StreamClosedError`, and then closes its notifier. It raises when either
  refuses the close. A later flush of the owner Workflow's writer is refused
  too, and fails the Workflow. So close a Workflow-owned stream from the
  Workflow.

A reader ends when the stream service says the stream is done: the stream
is closed in the store and the reader has every record.

`result` becomes the operation's result, for example a summary. To end the
work with a failure instead, pass `error=`, as
`await close_workflow_stream(topic, error=ApplicationError("the model gave up"))` or
`await store.close_stream(client, ref, error=...)`. Every operation then fails
with that error as its cause, or is canceled when the error is a
`CancelledError`. The error goes through the data converter's failure
converter and codec, as a Workflow failure does. A close takes a result or an
error, never both, and passing both raises `ValueError`. Either way the
store closes first, so a reader hands over every record and then raises the
failure as a `NexusOperationError`. A cancel from the caller detaches its
callback and completes that caller's operation as canceled. The stream keeps
going for any other caller. A start without a callback is refused.

`temporalio.workflow` also has generated low-level bindings for the
notifier: `notify_stream`, `attach_stream_callback` and
`detach_stream_callback`. Use `close_workflow_stream` rather than
`notify_stream`. Attach and detach have no use from Workflow code: the
handler attaches, and a cancel detaches.

What happens underneath:

1. The handler's start attaches the caller's callback to the stream's
   notifier on the server, and returns the stream reference in the
   operation token.
2. Core notifies the notifier after an append, a Workflow's flushes
   included, when a reader armed the stream. Notifications are folded: at
   most one call is in flight, and the newest one wins.
3. The notifier delivers the latest notification to the caller as progress.
4. The close completes the caller's operation with the close result.

### When Core notifies

[Readers that sleep](notifications.md) lists the steps: a read that waits
and finds nothing arms the stream, and the next append notifies once. For
streams over Nexus:

- A Workflow `StreamReader` waits `idle_wait` (2 seconds by default) before
  a read arms the stream. A Worker follower waits 1 second.
- The notifier sends the progress to every attached caller, so one append
  wakes all sleeping readers.
- A gap shorter than the idle wait costs only the read that returns the next
  record. A longer gap costs one read that arms, one notification and one
  progress task per caller.
- The attach arms an empty stream, so the first append reaches a caller that
  has no record to read yet. On a stream with records, the attach announces
  the newest one. Closes do not depend on an arm, so a close always reaches
  every caller.
- A reader from this release on a stream service from an earlier release
  fails its read with `BAD_REQUEST`, since the earlier service refuses the
  `arm` member.

## The caller side

The caller Workflow starts the operation as usual and reads it with a
`StreamReader`:

```python
from temporalio.contrib.streams.nexus import StreamReader

handle = await nexus_client.start_operation(ConversationService.respond, request)
reader = StreamReader(handle, item_type=Message, endpoint=ENDPOINT)
while (batch := await reader.next()) is not None:
    for message in batch:
        ...
summary = await handle
```

A client generated from a service that declares the operation's stream
result has a `read_<op>_stream(handle)` method that builds the reader for
you, with the endpoint defaulting to the client's:

```python
reader = nexus_client.read_respond_stream(handle)
```

What `next()` does:

- It reads the records after its cursor through the stream service on
  `endpoint`, and returns their bodies decoded to `item_type`. The first
  call reads at once. Later calls read on while answers carry records, and
  wait for newer progress only after an answer with no record. Each read
  before the completion waits up to `idle_wait` (a `StreamReader` keyword,
  2 seconds by default) and arms the stream when it finds nothing. Zero arms
  on the first empty read. Keep it well below the 10 second timeout of a
  Nexus request. It never returns an empty list.
- It returns `None` only when the stream service says the stream is done,
  and the last records are handed over. After the operation completes, the
  reader keeps reading until then, waiting up to 2 seconds per read. Every
  call after that returns `None` too.
- It hands over data records only. When a producer's earlier attempt was
  replaced by a newer one, the reader records that in
  `reader.supersessions`, in order, instead of returning it from `next()`.
  Each entry is a `ReadSupersession` with `index` (how many items had been
  handed over when it came), `cursor` and `supersession`. The stream service
  marks it. Each read carries the state the last answer gave, so the marks
  hold across reads and across the service's Workers. A reader built with
  `after=` an earlier reader's `position` keeps them exact too. One built
  with only a cursor tells the service only the cursor record's producer, so
  another producer's stale record can pass as current data:

  ```python
  if reader.supersessions:
      workflow.logger.warning("a producer restarted: %s", reader.supersessions[-1])
  ```

- A record that an earlier attempt of a producer wrote after a newer attempt
  started is not the current answer, so `next()` does not return it. The
  reader records it in `reader.stale_records`, in order. Each entry is a
  `ReadStaleRecord` with `index`, `cursor`, `producer_id`, `attempt` and the
  decoded `value`. This happens when an Activity attempt times out but keeps
  running.
- A record whose body does not decode into `item_type` raises
  `temporalio.contrib.streams.StreamRecordError` (a `StreamError` with
  `.cursor`), after the records before it are handed over. The next call
  goes on past it, so the Workflow can catch it and keep reading.
- A failed read raises `temporalio.exceptions.NexusOperationError`. So does
  a failed operation, after the records it left are handed over.
- If the operation's token names no stream, it raises `ValueError`.

Bodies reach the Workflow decoded. The stream service decodes them with its
Worker's data converter, the read result crosses once encoded with that
Worker's payload codec, and the caller's codec decodes it. So the handler's
Worker and the caller's Worker must use the same payload codec.

The codec runs in the handler Worker's process, as a step Core asks for in
the middle of a call. So Core holds plain bodies in memory during a call:
an append's bodies before the codec encodes them, and a read's bodies after
the codec decodes them. Core never stores or logs them, and sends them only
in the read's answer. The store keeps only encoded bodies.

Each read is a synchronous Nexus operation of the Workflow, so History
records it and a replay reads the same records. When a read finds no new
record, the reader waits on the operation's progress, which is also on the
handle:

```python
progress = await handle.progress()          # waits for the first progress
newer = await handle.progress(after_counter=progress.counter)
latest = handle.latest_progress             # or None
```

`progress()` returns `None` when the operation completes before newer
progress arrives. Progress comes from History, so a replay returns the same
values at the same points.

### History cost and long streams

Reading a stream adds to the caller's History in two ways.

- Each read is a synchronous Nexus operation, about three events, and its
  answer holds up to 1 MiB of records. A gap in the stream shorter than
  `idle_wait` costs one read. A longer gap costs one empty read that arms
  the stream, then one progress task.
- Each progress delivery costs at most one Workflow Task, three events,
  and up to about 3 KiB on its scheduled event. The server schedules a task
  for progress no sooner than `nexusoperation.progressMinInterval` (1 second
  by default, per namespace) after the caller Workflow's last task that
  carried progress. Progress that comes sooner waits and folds. A task
  scheduled for any other reason still carries it, and 0 turns the interval
  off. The interval is per caller Workflow, so all its operations share it,
  and a busy caller costs about one progress task per second plus its
  reads.

The server terminates a Workflow whose History passes 51,200 events or
50 MiB by default. To read a long stream, Continue-as-New before that, for
example when `workflow.info().is_continue_as_new_suggested()` is true. Pass
`reader.position` to the new run, start the operation again there, and
resume from it:

```python
reader = StreamReader(handle, item_type=Message, endpoint=ENDPOINT, after=position)
```

A `ReadPosition` holds the cursor and the stream service's read state, at
most about 87 KiB, so it passes as a Workflow argument. With it, the new
run's superseded and stale marks stay exact for every producer. A bare
cursor works too, but it tells the service only the cursor record's
producer.

Two cases stop a reader that would otherwise read for ever.

- After the operation completes, the reader reads until the stream service
  says the stream is done. If its reads find nothing for `drain_limit`
  (60 seconds by default, a `StreamReader` keyword), it raises
  `temporalio.contrib.streams.nexus.StreamIncompleteError`, whose `.cursor`
  says where it stopped. That happens when the owner's close never reached
  the store, or when the Workflow id moved on to a new run chain whose
  topic is still open.
- A record too large for one read answer raises `StreamRecordError` at its
  cursor, and the next call goes on past it. The stream service refuses an
  append of such a record, so this only happens for a record written to the
  store directly.

## Following a stream from a Worker

A Worker can follow a stream with no caller Workflow. `follow_stream` starts
the stream-returning operation as a standalone Nexus operation, with a
callback to the Worker's task queue. The server then calls the Worker each
time the stream moves (`OnProgress`) and once when the operation completes
(`OnComplete`). `StreamFollowerService` takes both calls on the Worker. It
reads the new records and hands them to your `StreamFollower` in batches,
then hands over the close.

```python
from temporalio.contrib.streams.nexus import (
    FollowBatch,
    FollowOutcome,
    StreamFollowerService,
    follow_stream,
)


class OrderFollower:
    async def on_records(self, batch: FollowBatch[OrderStage]) -> None:
        for record in batch.records:
            await show(record.cursor, record.value)

    async def on_close(self, outcome: FollowOutcome) -> None:
        await finish(outcome.result if outcome.succeeded else outcome.error)


worker = Worker(
    client,
    task_queue="app",
    nexus_service_handlers=[
        StreamFollowerService({"orders": OrderFollower()}, client=client)
    ],
)

handle = await follow_stream(
    client,
    endpoint="orders",
    service=OrderService,
    operation=OrderService.order_stream,
    arg=order_id,
    follower="orders",
    task_queue="app",
)
```

The type argument of the `FollowBatch` annotation on `on_records` is what
each record body decodes into. One `StreamFollowerService` per Worker routes
calls by the follower key, since a Worker registers a service name once.

**How it reads.** With `client=` a client that carries a stream store, the
service reads through Core's stream service on that store, as an outside
reader does (`StoreTransport`). For a stream in another namespace or another
store, pass `transport=TemporalStreamsHttpClient(url)`, which reads through
the stream service on a Nexus endpoint's HTTP ingress. With neither, the
service raises at construction. The stream service and the follower must use
the same payload codec.

**Where the position lives.** Each answer to `OnProgress` carries the read
position, and the server keeps it on the callback and sends it back with the
next call and with `OnComplete`. So the Worker keeps nothing between calls,
and any Worker on the task queue can take the next one. A call reads page by
page and calls `on_records` once per page. Each read waits up to 1 second
for a record and asks to arm the stream. The call stops at the first answer
that armed the stream, so the next append wakes the follower. A busy stream
never arms, so the call also stops when half the request timeout passed.
Then it answers `behind`, and the server sends the same progress again after
`callback.nexusHandler.progressBehindInterval` (1 second by default), so a
long backlog is read in pages without a busy loop. `OnComplete` reads the rest of the stream,
then calls `on_close`.

**Delivery is at least once.** A batch repeats after a lost answer or a
timeout, and a completion can overtake a progress call in flight, so a batch
can come after `on_close`. Each record carries its cursor: drop the ones you
already handled. Supersessions, stale records and records that could not be
read come in the batch, as the Workflow reader reports them.

**A lost notification waits.** A producer that dies between its append and
its notify sends no progress, and the server sends a follower no recheck.
So the follower gets that record with the next append's progress, with the
close, or when the owner check closes the stream. A Workflow `StreamReader`
has a recheck timer, and a follower doesn't. If a record must not wait,
append again after a crash, or close the stream when the work ends.

**Server setup.** On top of the settings above, the server must take
NexusHandler callbacks on a standalone operation:

| Setting | Why |
|---|---|
| `nexusoperation.enableStandalone` | Standalone Nexus operations. |
| `nexusoperation.enabledCallbackKinds: [nexus, nexusHandler]` | A standalone start may carry a callback to a Worker's task queue (`nexusHandler`), and other callers' Nexus callbacks (`nexus`). |

The store must have `notify_on_append()` on, so the stream's moves reach the
follower as progress. A callback attached later with
`attach_completion_callbacks` gets the completion only.

`NexusClient.start_operation(..., completion_callbacks=[NexusHandlerCallback(...)])`
is the general form `follow_stream` uses. A Worker takes those calls with a
handler of `temporalio.nexus.system.notification_service.NotificationService`,
for any standalone operation, stream or not. The service and its models are
generated from api's `nexus/notification-service.wit`. The Worker decodes the
callback's `source_context` and the operation's result with its data
converter, so a handler gets the values, not payloads.

## Limits and timeouts

- A notification's `position` is at most 1 KiB of UTF-8, and its metadata
  at most 2 KiB. The server refuses larger ones.
- A stream's notifier holds at most `streamnotifier.maxCallbacks` callers
  (2,000 by default), and callbacks of at most
  `streamnotifier.maxCallbackBytes` in total (4 MiB by default). A full
  notifier first drops its oldest callbacks that already completed. If that
  does not make room, it refuses the attach.
- A read answer holds about 1 MiB of records past the first record
  (`TemporalStreamsHandler(..., max_answer_bytes=...)`). The first record can
  be up to 1.5 MiB. The reader goes on with the next read.
- If a stream gets no attach and no notification for
  `streamnotifier.idleTimeout` (7 days by default), the notifier fails the
  callers waiting on it. The stream stays open, and later attaches and
  notifications work as before. Core sends a busy stream's last counter
  again once a day, so a stream that gets appends keeps its notifier.
- While callers are attached, the notifier checks the owner Workflow every
  `streamnotifier.ownerCheckInterval` (30 seconds by default). If the owner's
  run chain closed without closing the stream, the notifier closes it by the
  owner's close status, as the table below says.
- A closed stream's notifier completes late attaches right away for
  `streamnotifier.closedRetention` (7 days by default, the store's
  retention), then refuses them. Keep it at least as long as the store
  keeps a stream's records.
- Once a Workflow id is reused by a new run chain, a late start for the
  earlier chain's stream (a stream reference that names one of its runs)
  fails: the server refuses its attach with `InvalidArgument` instead of
  completing it with that stream's close result. The server attaches only
  to the chain that holds the id now. A reference that names no run
  attaches to the new chain.
- A producer that dies between its append and its notify sends no
  progress. A Workflow `StreamReader` that waits for progress reads again
  once a minute passes with none, then after 2, 4 and so on up to 1 hour
  while the stream stays quiet, on Workflow timers. So such a record
  reaches the reader at the latest one recheck later. Each recheck adds a
  timer and a read to the caller's History.
- If a caller answers a progress delivery with 404, its operation is
  already closed, and the notifier drops that caller. Any other refusal
  turns progress off for that caller, and it still gets the completion.
- A reader reads the run chain the owner's Workflow id has at each read,
  not the chain its operation started on. If the chain ends and the same id
  starts a new one while a reader still drains, the reader can hand over
  the new chain's records. Give each stream owner its own Workflow id, or
  reuse an id only after its readers finish.
- `close_workflow_stream` closes the store first, then the notifier, from
  one Local Activity. Core tries a failed notifier close again with
  backoff, from 1 second up to 1 minute. A close the server refuses for
  good, such as one for a stream it doesn't know, isn't retried. The Local
  Activity's retries cover the rest.
- No Worker closes a stream when its owner's run chain ends. A stream that
  the owner never closed is closed by the notifier's owner check, by the
  owner's close status, as the table below says. Close explicitly, so the
  callers get your result and not an outcome the check makes up.

### An owner that ends without closing its stream

The owner's close status decides how its open streams' operations complete,
as for an operation that runs a Workflow. The notifier's owner check uses
it.

| Owner's close status | The operations | The reader raises |
|---|---|---|
| Completed | Complete with an empty result | Nothing. It returns `None`. |
| Failed | Fail with an `ApplicationError` of type `StreamOwnerFailed` | `NexusOperationError`, with that `ApplicationError` as the cause |
| Canceled | Are canceled | `NexusOperationError`, with a `CancelledError` cause |
| Terminated | Fail as terminated | `NexusOperationError`, with a `TerminatedError` cause |
| Timed out | Fail as timed out | `NexusOperationError`, with a `TimeoutError` cause |
| Not found, or the id moved to another chain | Fail with "the owner Workflow ended without closing the stream" | `NexusOperationError`, with an `ApplicationError` cause |

The check closes a terminated or timed-out owner's streams after up to
`streamnotifier.ownerCheckInterval`. It leaves a completed, failed or
canceled owner alone until `streamnotifier.ownerCloseGrace` (1 minute by
default) after it closed, so a close that the owner sent on its way out
comes first. The owner check reads no History, so a failed owner that it
closes gives an `ApplicationError` of type `StreamOwnerFailed` rather than
the Workflow's own failure. For the callers to see the real failure, close
the stream with it before the Workflow fails:

```python
except Exception as err:
    await close_workflow_stream(MESSAGES, error=err)
    raise
```

A stream server from before this release
ignores a close's failure and completes the operations with an empty
result.

## Guarantees

- **Highest counter wins.** Each progress has a counter. The caller keeps
  the highest one and drops any lower or repeated one, so deliveries may
  arrive out of order.
- **A burst folds.** Progress that arrives while the caller is between
  Workflow Tasks waits for the next task. The server also holds progress
  until `nexusoperation.progressMinInterval` (1 second by default) has
  passed since the caller Workflow's last task that carried progress. So a
  burst of any size costs the caller at most two tasks: the one it rides,
  and one follow-up when newer progress folded in after that task was
  already scheduled. The Workflow then sees the highest counter of the
  burst. Intermediate counters may be skipped, which is why the reader
  reads records and does not count progress.
- **No History event per delivery.** Progress rides the caller's next
  Workflow Task scheduled event. History is still the record, so replay
  sees the same progress.
- **Completion never waits for progress.** The operation completes when the
  stream closes, whether or not a progress delivery is in flight. Progress
  for a closed operation is dropped.
- **Progress before the start is dropped.** The reader reads from its
  cursor when the start arrives, so nothing is lost.
- **A completion names the operation even when it beats the start.** The
  handler gives the notifier its operation token and start time when it
  attaches, and the notifier sends them with every delivery. A stream that
  is already closed when the caller starts still hands over its reference.
- **Old receivers degrade.** A caller on a server without progress refuses
  it, and the notifier stops sending progress to that caller. The
  completion still arrives.

## Do not pass store claims across Nexus

The stream reference names a stream: its owner and its topic. It carries no
credential for the store. Do not put a Redis URL, a key prefix or any store
credential in the operation's input, result or token, and do not build a
caller that reads the handler's store directly. The caller reads through
the stream service on the handler's endpoint, so it needs no access to the
handler's store.

The stream service writes and reads with its Worker's client and store
credentials, whatever caller sent the call. So by default every caller that
can reach the endpoint can read and append to every stream of the handler's
namespace, as any producer. Limit who reaches the endpoint with its allowed
caller namespaces, and pass `authorize` to `TemporalStreamsHandler` to check
each call. It gets a `StreamAccess` (`operation`, `stream`, the Nexus
`context` with the caller's headers, and for an append `producer_id` and
`attempt`) and answers whether to serve it. A refused call fails as
unauthorized.

Parked reads hold Nexus task slots on the handler's Worker for up to a few
seconds each. With many readers, give that Worker more Nexus pollers
(`nexus_task_poller_behavior`), and serve appends and reads on separate
task queues or endpoints, so an append doesn't wait behind parked reads.

## Sample

[`tests/contrib/streams/samples/nexus_slack_router.py`](../../../../tests/contrib/streams/samples/nexus_slack_router.py)
runs a handler that streams a conversation's messages and a caller Workflow
that reads them and files each one under its channel. Its test,
`tests/contrib/streams/test_nexus_samples.py`, runs it on a server with the
settings above and skips on a server without the stream notifier.

## Live tests

The stock dev server has no stream notifier and takes no NexusHandler
callbacks on a standalone operation, so the sample test and the reader,
progress, stream operation and follower lifecycle tests skip there.
`scripts/streams_live_lane.sh` builds a server that has the notifier, starts
it with the settings above and a scratch Redis, and runs those tests against
it, along with Core's live notifier test. It fails unless each test on its
gated list passes. A release of streams over Nexus needs a green run of it,
until CI has a lane with such a server.
