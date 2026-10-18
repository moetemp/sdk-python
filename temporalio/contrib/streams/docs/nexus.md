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
    StreamRef, close_workflow_stream, topic, stream_producer,
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
        producer = stream_producer(MESSAGES)
        for reply in make_replies(request):
            producer.append(reply)
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

store = RedisStreams("redis://localhost:6379")
client = await Client.connect("localhost:7233", plugins=[store])
worker = Worker(
    client,
    task_queue="conversations",
    workflows=[Conversation],
    nexus_service_handlers=[ConversationHandler(), TemporalStreamsHandler()],
)
```

The stream runtime serves the stream service. `TemporalStreamsHandler` hands
each call to the stream runtime and keeps only what the stream runtime can't
do: your `authorize` callable and the namespace's payload codec. It holds
nothing between calls, so it needs no shutdown, and any Worker on the
endpoint's task queue can serve a reader's next read. The reader carries the
stream runtime's read state in each read instead. So the reader and the
stream service come from the same release. A stream service from an earlier
release refuses the reader's `state` as an unknown member. A reader from an
earlier release on a newer service still works, but it sends no state, so
the service primes from the cursor on every read and knows only the cursor
record's producer.

`notify_on_append`, on by default, gives the caller progress. With
`notify_on_append(False)` the caller sees no progress, but the close still
completes every operation with its result. The
stream runtime closes a stream's notifier after every store close, whether
or not progress is on. The stream runtime notifies only while a reader
sleeps on the stream, so a busy stream costs no notification. [Readers that
sleep](notifications.md) says when the stream runtime notifies and how a
process flushes its notifications before it exits.

Closing the stream completes every operation that handed it out:

- From the owning Workflow, `await close_workflow_stream(topic,
  result=...)`. The topic's Workflow Producer lands what it buffered first. Then one
  Local Activity closes the stream in the store and then completes the
  operations, so a reader never misses a record published before the close.
- From outside a Workflow, for example an Activity or the process that sees
  the work end, `await store.close_stream(client, ref, result)`. It closes
  the stream in the store first, so a later append raises
  `StreamClosedError`, and then closes its notifier. It raises when either
  refuses the close. A later flush of the owner Workflow's producer is refused
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

What happens underneath:

1. The handler's start attaches the caller's callback to the stream's
   notifier on the server as an idle callback, which hears no progress
   until it waits, and returns the stream reference in the operation token.
2. The stream runtime notifies the notifier after every append, a Workflow's
   publishes included. Notifications are folded: at most one call is in
   flight, and the newest one wins.
3. A reader reads first. Each read names its operation (`wake_token`). A
   read that finds nothing asks the notifier to wake that operation's
   callback once the stream moves past the read, then reads once more, and
   answers what that finds. So a record that lands while the wait is set up
   is not lost.
4. The notifier delivers one notification to each waiting callback as
   progress, and the callback is idle again until the reader waits again.
5. The close completes the caller's operation with the close result.

### When the stream runtime notifies

[Readers that sleep](notifications.md) lists the steps: every append
notifies, and the server delivers at most one notification per stream in
each debounce window, plus the newest one at the window's end. For streams
over Nexus:

- A Workflow `StreamReader` waits `idle_wait` (2 seconds by default) before
  it sleeps on progress. A Worker follower waits 1 second.
- The notifier sends the progress to every waiting caller, so one append
  wakes all sleeping readers.
- A reader that finds records on each read never waits, so it gets no
  progress at all. A gap shorter than the idle wait costs only the read that
  returns the next record. A reader that waits gets one delivery per idle
  stretch, however many appends come in it, within the debounce window.
- The attach starts the notifier and announces nothing, since the reader
  reads first. A close always reaches every caller, waiting or not.

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
  2 seconds by default) before the reader sleeps on progress. Zero sleeps on
  the first empty read. Keep it well below the 10 second timeout of a
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
- A failed read raises `temporalio.exceptions.NexusOperationError`, or an
  `ActivityError` for a [direct reader](#direct-reads). A failed operation
  raises `NexusOperationError`, after the records it left are handed over.
- If the operation's token names no stream, it raises `ValueError`.

Bodies reach the Workflow decoded. The stream service decodes them with its
Worker's data converter, the read result crosses once encoded with that
Worker's payload codec, and the caller's codec decodes it. So the handler's
Worker and the caller's Worker must use the same payload codec.

The codec runs in the handler Worker's process, as a step the stream runtime
asks for in the middle of a call. So the stream runtime holds plain bodies
in memory during a call: an append's bodies before the codec encodes them,
and a read's bodies after the codec decodes them. The stream runtime never
stores or logs them, and sends them only in the read's answer. The store
keeps only encoded bodies.

By default each read is a synchronous Nexus operation of the Workflow, so
History records it and a replay reads the same records. When a read finds no new
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

- By default each read is a synchronous Nexus operation, about three
  events, and its answer holds up to 1 MiB of records. With
  `replay_from_store=True` a read is a direct read, one marker of about
  1 KiB. A gap in the stream shorter than `idle_wait` costs one read. A
  longer gap costs one empty read, then one progress task.
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
most about 22 KiB, so it passes as a Workflow argument. With it, the new
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

### Direct reads

With `replay_from_store=True` a reader keeps the records out of its
History. It's the opt-in. Each read runs in a Local Activity, not as a Nexus operation of the
Workflow. History keeps one marker per read, with the range of records that
crossed and the stream runtime's digest of them, not the records. The marker
is about 1 KiB. Most of it is the stream service's read state, which grows
with the producers the reader saw. A read counts once it returned. When the
read before was the same run's, the marker leaves out the read's input
state, since the previous marker holds it as its answer state. The first
read of a run, also of a reset run, keeps it. A process that replays a later
marker without the ones before it fails the Workflow Task, and the retry
replays the History from the start. All the Local Activities of one Workflow
Task count as one Action. The Worker turns the marker back into the batch
before Workflow code sees it. On the live Workflow Task it takes the batch
the Local Activity cached. On a replay it reads the range again through the
stream service's `read_range`, which checks the records against the digest.

It needs a way to reach the endpoint outside the Workflow. Register
`StreamDirectReads` on the caller Worker's client, and on any `Replayer`,
with a transport for each endpoint. Without one, every read fails at once
with `StreamUnsupportedError`, which names this option. Build the reader
with `replay_from_store=True`:

```python
reader = StreamReader(
    handle, item_type=Message, endpoint=ENDPOINT, replay_from_store=True
)
```

A transport:

```python
from temporalio.contrib.streams.nexus import HttpTransport, StreamDirectReads

reads = StreamDirectReads(
    {
        ENDPOINT: HttpTransport(
            f"{http}/nexus/endpoints/{endpoint_id}/services/"
            "temporal.sdk.streams.v1.TemporalStreams",
            headers={"Authorization": f"Bearer {token}"},
            payload_codec=codec,
        )
    }
)
client = await Client.connect(address, data_converter=converter, plugins=[reads])
```

Two transports ship. `HttpTransport` calls the endpoint over the server's
Nexus HTTP ingress. It is a stopgap for self-hosted servers, until a
transport that Cloud serves exists.

- It works on a self-hosted server only. Temporal Cloud has no HTTP ingress
  for endpoints.
- The server checks its credential (`headers` or `ssl_context`) against the
  endpoint's target namespace, the handler's, and asks for write access
  there. So the caller's Worker holds a write credential on the handler's
  namespace. The handler's `authorize` is the check per stream. It sees
  `operation="read_range"` and the `floor_holder` the call names.
- Listing endpoints needs cluster admin, so give it the endpoint id.
- Give it the codec of the handler's namespace as `payload_codec`, the same
  one the caller's data converter has. The input then crosses encoded, and
  the transport decodes the answer.

`ClientTransport` calls the endpoint with the caller's own client, through
the `ExecuteSyncNexusOperation` RPC:

```python
from temporalio.contrib.streams.nexus import ClientTransport, StreamDirectReads

client = await Client.connect(address, data_converter=converter)
reads = StreamDirectReads({ENDPOINT: ClientTransport(client, ENDPOINT)})
worker_client = await Client.connect(
    address, data_converter=converter, plugins=[reads]
)
```

- It takes the caller's own credential, and the server checks it against the
  caller's namespace. The endpoint is found by name as the caller's
  namespace sees it, so the endpoint's allowed callers apply.
- The server tells the handler the caller's namespace, which `authorize`
  gets as `StreamAccess.caller_namespace`. Only this RPC sets it. Over the
  HTTP ingress and from a Workflow's Nexus call it is `None`.
- It needs a server with the RPC turned on
  (`frontend.enableExecuteSyncNexusOperation`, off by default). Temporal Cloud
  doesn't serve it yet. So on Temporal Cloud no transport works today, and a
  reader there keeps the default, `replay_from_store=False`.
- It encodes the input and decodes the answer with the client's payload
  codec, under the wrappers that turn reads back into batches. So the
  handler's namespace must use the same codec.
- A `Replayer` has no client, so the transport brings its own. Any client of
  the caller's namespace does.

The same transport serves a Worker that follows a stream behind an endpoint:
`StreamFollowerService(followers, client=client,
transport=ClientTransport(client, ENDPOINT))`.

A direct read names a floor holder, `<caller namespace>/<run id>/<reader
number>`. The handler's store keeps the records after the read's start for
that holder, even past its retention. Each read and each replay read renews
the hold, and it lapses the store's floor lease after the last one
(`RedisStreams(floor_lease=...)`, the retention by default). The reader never
releases its hold while its run is open, since a query or a reset can still
replay the run.

A replay that misses the cache needs the endpoint and the records:

| A replay finds | Then |
|---|---|
| Other records, cursor or count | The Workflow Task fails as nondeterminism |
| The records gone, after the floor lease lapsed | The Workflow Task fails with `StreamReplayRangeLostError` and is retried |
| The store down, the endpoint gone, 401, 403, 429, 5xx, `NotFound`, `NamespaceNotActive` or a timeout | The Workflow Task fails and is retried |
| No `StreamDirectReads`, or no transport for the endpoint | `StreamUnsupportedError`, which names `replay_from_store`. The Workflow Task fails and is retried |

A live read whose credential the endpoint refuses, 401 or 403, retries its
Local Activity with backoff and logs an error, so a key that expired fails no
caller Workflow. A refusal the handler marks final, such as its authorizer's,
still fails the read for good. A live read on a Worker with no
`StreamDirectReads`, or with no transport for the endpoint, fails its Local
Activity at once, with no retry. The reader raises that again from `next()` as
`StreamUnsupportedError`, a `TemporalError`, so the Workflow Task fails and
is retried once the Worker is fixed. Workflow code that catches every
`Exception` around `next()` swallows it, so catch narrower errors there, or
raise `StreamUnsupportedError` again.

So keep the endpoint and its Worker up, and keep the floor lease longer
than a run can wait before a replay. To stop direct reads, deploy readers
with `replay_from_store=False`. A run that already has direct-read markers
still needs the transport to replay them. The mode decides the commands a
read sends, so a History recorded in one mode replays only with code that
builds the reader in the same mode.

A record that crossed as an error replays as the same error, even when the
codec decodes it now, since live Workflow code never saw it.


A Worker can follow a stream with no caller Workflow. `follow_stream` starts
the stream-returning operation as a standalone Nexus operation, with a
callback to the Worker's task queue. The server then calls the Worker each
time the stream moves (`OnProgress`) and once when the operation completes
(`OnComplete`). `StreamFollowerService` takes both calls on the Worker. It
reads the new records and hands them to your `StreamFollower` in batches,
then hands over the close.

```python
from temporalio.contrib.streams.nexus import (
    FollowerBatch,
    FollowOutcome,
    StreamFollowerService,
    follow_stream,
)


class OrderFollower:
    async def on_records(self, batch: FollowerBatch[OrderStage]) -> None:
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

The type argument of the `FollowerBatch` annotation on `on_records` is what
each record body decodes into. One `StreamFollowerService` per Worker routes
calls by the follower key, since a Worker registers a service name once.

**How it reads.** With `client=` a client that carries a stream store, the
service reads through the stream runtime's stream service on that store, as
an outside reader does (`StoreTransport`). For a stream in another namespace
or another store, pass `transport=TemporalStreamsHttpClient(url)`, which
reads through the stream service on a Nexus endpoint's HTTP ingress. With
neither, the service raises at construction. The stream service and the
follower must use the same payload codec.

**The first wake.** A follower reads only inside a progress call, so its
idle callback must be woken once before it can read at all. `follow_stream`
sends the start header `temporal-stream-first-wake-after: 0`, and the
handler asks the stream runtime for that first wake at the attach. The first
record of the stream then reaches the follower, or at once when the stream
already holds one. After that, each progress call reads until it finds
nothing, and that empty read asks for the next wake. A follower that reads a
burst in one call gets one delivery for it, not one per append.

**Where the position lives.** Each answer to `OnProgress` carries the read
position, and the server keeps it on the callback and sends it back with the
next call and with `OnComplete`. So the Worker keeps nothing between calls,
and any Worker on the task queue can take the next one. A call reads page by
page and calls `on_records` once per page. Each read waits up to 1 second
for a record. The call stops at the first answer with no record, since the
next append wakes the follower. On a busy stream the call also stops when
half the request timeout passed.
Then it answers `behind`, and the server sends the same progress again after
`callback.nexusHandler.progressBehindInterval` (1 second by default), so a
long backlog is read in pages without a busy loop. `OnComplete` reads the rest of the stream,
then calls `on_close`. A canceled or failed operation can leave the stream
open, so its `OnComplete` reads only up to the first empty read.

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

The store must keep `notify_on_append` on, the default, so the stream's moves
reach the follower as progress. A callback attached later with
`attach_completion_callbacks` gets the completion only.

`NexusClient.start_operation(..., completion_callbacks=[NexusHandlerCallback(...)])`
is the general form `follow_stream` uses. A Worker takes those calls with a
handler of `temporalio.nexus.system.notification_service.NotificationService`,
for any standalone operation, stream or not. The service and its models are
generated from api's `nexus/notification-service.wit`. The Worker decodes the
callback's `source_context` and the operation's result with its data
converter, so a handler gets the values, not payloads.
