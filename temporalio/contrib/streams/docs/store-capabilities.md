# Store capabilities and guarantees

> This package is experimental and may change in future versions.

Every store keeps an ordered log for each topic of each run chain. A store
can also have capabilities on top of that log. The stream runtime reads them
when a client connects the store, and takes the full path or a fallback for
each one. `StreamStorePlugin.capabilities` shows what the stream runtime
found, as a `StoreCapabilities`.

## What every store guarantees

These hold on every store, whatever its capabilities.

- A batch lands whole or not at all, in order, with no record of another
  batch between its records.
- Readers get the records of a topic in log order.
- A reader drops a copy of a record it delivered. A copy has the producer
  and attempt of a delivered record, and a sequence at or below the highest
  one the read delivered for that attempt. A record with sequence 0 is never
  a copy.
- A read ends at the first `CLOSE` record of its topic or of its run chain.
  It doesn't deliver the `CLOSE`, and it drops what comes after it.
- `SUPERSEDED` works the same on every store. The stream runtime makes it
  from the records the read saw, and no store holds it.
- A read never skips records in silence. If retention dropped records after
  the cursor, the read fails with `StreamExpiredError`.
- A reader writes nothing to the store. Only producers and the owner's Worker
  close a stream.

The read keeps its dedupe state with its supersession state, for at most
1,000 producers in 16 KiB. Past that, it forgets the producer it saw least
recently. A read that resumes from a cursor knows only the producer of the
cursor's record. So it can deliver the copy of a batch whose first copy came
before the cursor. It still delivers that batch once.

`SupersessionTracker` and `track_supersession` apply the same rules to
records that come from somewhere else, such as a copy of a read that the
application kept.

## The capabilities

| Capability | Field | With it | Without it |
|---|---|---|---|
| Producer state | `producer_state` | A retry of the newest batch writes nothing and answers the first positions. A changed batch, or one with an old sequence, fails with `StreamProducerError`. An append after a close fails with `StreamClosedError`. | A retry can land a second copy, which readers drop. The producer can get the second copy's positions. A changed or stale batch lands with no error, and readers keep the first copy. An append that races a close can answer success, and no reader delivers it. A Workflow then gets no `workflow_stream_output_dropped` metric for it. |
| Conditional append | `conditional_tail`, `conditional_fence` | The append checks a condition the store holds. A fence stops every append after a close. A tail check gives exact positions after a lost answer. | As without producer state. |
| Kept last position | `kept_last` | The store knows a log's last position after retention dropped all of it, for 30 days. So a read tells an expired cursor from an empty stream. | The stream runtime decides expiry from the cursor's own record. Retention drops only the oldest records, so if that record is still there, nothing after it is lost. If it is gone, the read fails with `StreamExpiredError`, also when nothing after it was lost. |
| Retention holds | `holds` | A `workflow_reader` with `replay_from_store=True` holds the records it read against retention, and the archive holds what it hasn't copied yet. | A positions-only read works only on a store with a `replay_horizon`. Otherwise its first read fails with `StreamUnsupportedError`, and the reader needs `replay_from_store=False`. |
| Change feed | `change_feed` | The notification log keeps a notification for each append, so a producer that stops after its append still wakes its readers. | Producers notify after each append, and an append answers only after its notification went. A producer that stops before then gets no answer, and its retry notifies. A producer that never retries leaves the reader to its next recheck. |
| Cheap logs | `cheap_logs` | One log per topic of each run chain. | The stream runtime would share a few logs between streams. No store in this release needs it. |
| Delete by owner | `delete_owner` | `delete_workflow_streams` removes a Workflow's streams. | `delete_workflow_streams` raises `StreamUnsupportedError`. Records go at retention. |
| Topic listing | `topic_listing` | A closer finds every topic of a run chain. | Producers write a `TOPIC` record to the chain's control log, so a closer finds the topics there. |
| Long poll | `long_poll` | A read waits in the store for the next record. | The stream runtime polls with backoff, so records can arrive a little later. |

`dense_positions` says that the next position is always the last one plus
one. `replay_horizon` is how long the store keeps every record for sure, if
the store says. `max_record_bytes` and `max_batch_bytes` are the store's
limits. A record or a batch above a limit fails the append with
`ValueError`, which names the limit.

## When the owner ends

When a run chain completes, fails or is canceled through a Workflow Task,
the owner's Worker closes every topic of the chain once the run's History
shows that the task closed the run. With producer state, it sets the close
flags. Without it, it writes `CLOSE` records. Either way, every reader ends
at the same record.

When the owner ends with no Workflow Task, as on terminate or timeout, no
Worker closes the chain. Each reader ends its own read when it sees that the
owner ended. A producer that sees it at its owner check closes its topic.
It checks at least once a minute, so for up to a minute it can still append.
So two readers can end at different records. This holds on every store.

Continue-as-New does not close a stream.

## The stores

Y means the store has it, and N means the stream runtime takes the fallback.

| Store | Producer state | Conditional | Kept last | Holds | Change feed | Cheap logs | Delete | Listing | Long poll |
|---|---|---|---|---|---|---|---|---|---|
| `MemoryStreams()` | Y | N | Y | Y | N | Y | Y | Y | Y |
| `RedisStreams(...)` | Y | N | Y | Y | N | Y | Y | Y | Y |
| `RedisStreams(..., plain=True)` | N | N | N | N | N | Y | Y | Y | Y |

Memory and Redis have a change feed once
`notify_on_append(notification_log=True)` turns the notification log on.
`RedisStreams(plain=True)` refuses the notification log and an archive when
the client connects it.

`RedisStreams(plain=True)` runs no Lua scripts. Use it on a Redis that
refuses scripts. It also runs the fallbacks on a real network store.

`MemoryStreams(capabilities=StoreCapabilities(...))` turns off each
capability that the mask sets to `False`, so a test can run a fallback in
one process. A mask can't add a capability that the store doesn't have. A
nonzero limit in the mask replaces the store's own.
