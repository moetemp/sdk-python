# Stream wire contract

> This package is experimental and may change in future versions.

This page is the contract another SDK, or a tool, follows to read and write
the same streams in Redis as Core's store, which every Core-based SDK uses.
Everything here is part of the stored format: change it and existing
streams stop working. The canonical scripts are in `crates/streams/lua/` of
the Core repository: `append.lua`, with the trim and refresh in `keep.lua`.

## Redis

Redis 7.0 or later. Every write that must be atomic runs as one Lua script.

## Key layout

All keys of one Workflow's streams share a Redis Cluster hash tag, the
Workflow's run chain:

```
<prefix>:{<namespace>:<workflow id>:<first run id>}<suffix>
```

- `<prefix>` is the store's `key_prefix`, default `temporal-streams`.
- `<first run id>` is the first run of the Workflow's run chain
  (`first_run_id` from describing the Workflow, or
  `first_execution_run_id` inside it). Keying by the chain lets a reader
  follow Continue-as-New with no handoff.
- Each of `<prefix>`, `<namespace>`, `<workflow id>`, `<first run id>` and
  `<topic>` is percent-encoded as UTF-8, keeping only `A-Z a-z 0-9 - . _ ~`
  (Python's `urllib.parse.quote(part, safe="")`). So no part can contain
  `:`, `{` or `}`, and no two streams share a key.

| Suffix | Type | Holds |
|---|---|---|
| `:t:<topic>` | stream | The log of one topic |
| `:t:<topic>:meta` | hash | Dedupe state, the tombstone of that log, and `closed` = `1` once the topic is closed |
| `:chain` | hash | `closed` = `1` once the run chain has ended |

## Log entries

Each entry of a log has one field, `r`, whose value is the serialized
`temporal.sdk.streams.v1.StreamRecord` protobuf
(`crates/protos/protos/local/temporal/sdk/streams/v1/message.proto` in
Core). The package
`temporal.sdk.streams` is reserved for this envelope, so no other proto may
use it:

| Field | Number | Meaning |
|---|---|---|
| `body` | 1 | The published value as a `temporal.api.common.v1.Payload`, after the payload codec |
| `metadata` | 2 | Map of string to `Payload`, see below |
| `topic` | 3 | The topic name |
| `kind` | 4 | `STREAM_RECORD_KIND_DATA` (1) or `STREAM_RECORD_KIND_FINISH` (2). 0 reads as DATA. 3 is reserved, since readers synthesize `SUPERSEDED` with it |
| `producer_id` | 5 | Who wrote it. An Activity writes as `<activity id>@<scheduling run id>`, and a Local Activity as `local-<call number>@<scheduling run id>` |
| `attempt` | 6 | The producer's attempt. A Local Activity's is `(History length * 100000 + Workflow Task attempt) * 10000 + Local Activity attempt`, below 2^53 |
| `sequence` | 7 | The record's position in its attempt, starting at 1 |

Metadata keys, each a `Payload` with `encoding` = `binary/plain`:

- `temporal.io/content-hash`: the lowercase hex SHA-256 of the body's
  deterministic protobuf serialization, taken before the payload codec.
  Only the writer that made it compares it, since protobuf's deterministic
  serialization is stable within one implementation, not across languages.

The entry id that Redis assigns is the record's position. Readers never
see a `SUPERSEDED` record in the log: a reader synthesizes one when a
producer's newer attempt first appears.

## Appending a batch

One script, with keys `log`, `meta` and `chain`, does the following in
order:

1. Read the producer's high-water field from `meta`. The field name is
   `hw:<length of producer id in UTF-8 bytes>:<producer id>:<attempt>`. The
   value is `<first sequence>|<record count>|<first entry id>|<last entry
   id>|<digest>`, for the newest batch.
2. If the batch's first sequence equals the held one: with the same digest,
   return the held first and last entry ids and write nothing; with another
   digest, refuse (divergent). If the batch's first sequence is below the
   held one plus its record count, refuse (stale).
3. If `chain` or `meta` has `closed`, refuse (closed).
4. If the log key is missing but `meta` has `last`, set `meta.trimmed` to
   `last`, because the log expired whole.
5. `XADD` each record with id `*` and field `r`.
6. Set the high-water field for this batch.
7. Trim and refresh, as below.

The digest is the lowercase hex SHA-256 over the batch's records before the
codec: for each record, its deterministic serialization's length as 8 bytes
big-endian, then the bytes. Because it is taken before the codec, a codec
that encrypts with a fresh nonce still deduplicates a retry. Only the
writer that made it compares it, as with the content hash.

Trim and refresh:

- `XREVRANGE log (<now - retention>-0 - COUNT 1` finds the newest record
  about to be trimmed. If there is one, `XTRIM log MINID <now - retention>-0`
  runs and `meta.trimmed` is set to that id. `now` is from `TIME`, in
  milliseconds.
- `PEXPIRE log <retention>`.
- `HINCRBY meta added <records>`, `HSET meta last <last entry id>`,
  `PEXPIRE meta <retention + 30 days>`.
- `HSCAN meta <meta.hwscan or 0> MATCH hw:* COUNT 50`, then `HDEL` each
  high-water field whose last entry id is older than `<now - retention>`,
  and `HSET meta hwscan <next cursor>`. A retry of a batch that old is
  accepted again, and its records were trimmed anyway.

Every writer of a chain must use the same retention. A writer with a
shorter one trims records that other writers' readers expect to keep, so
the shortest retention wins.

## Cursors

A cursor token is `redis:<stream hash>:<entry id>`.

- `<stream hash>` is the first 8 lowercase hex characters of the SHA-256
  over namespace, owner kind (`workflow`), Workflow id and topic, each as
  UTF-8 bytes prefixed by its length as 8 bytes big-endian. The run id is
  not part of it, so a cursor stays valid across Continue-as-New. So it
  also can't tell two chains of one Workflow id apart.
- The empty token is `BEGINNING` and `$end` is `END`.

A reader refuses a token from another store or with another stream
hash, and resumes strictly after the entry id. It reports the cursor as
expired when `meta.trimmed` is newer than it, or, if the log is gone, when
`meta.last` is newer than it.

## Closing

Any party that sees the Workflow's run chain end (complete, fail, cancel,
terminate or time out, but not Continue-as-New) sets `HSET chain closed 1`
and `PEXPIRE chain <retention + 30 days>`.

A close of one topic before its chain ends, such as the owner closing a
stream, sets `HSET meta closed 1` and `PEXPIRE meta <retention + 30 days>`
on that topic's `meta`. The append script checks both flags, so no append
lands after a close returned.

A producer of the earlier interim release runs its own append script in
Python, and that script checks only `chain`. It knows nothing of a topic
close. In a fleet that mixes it with Core-based writers, it goes on writing
to a topic that Core closed, until the chain ends. Upgrade every writer of a
stream before you rely on a topic close.
