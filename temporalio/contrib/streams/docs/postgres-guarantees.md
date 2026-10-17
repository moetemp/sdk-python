# PostgreSQL store: guarantees and setup

> This package is experimental and may change in future versions.

Stream records live in your PostgreSQL database. Temporal never sees them,
so what a stream guarantees is what your database guarantees. The stream runtime runs the
store for every SDK, with the SQL in `crates/streams/src/postgres/` of the
sdk-streams repository. This page says what the store promises, how to set it up,
and when to pick it over Redis.

```python
from temporalio.client import Client
from temporalio.contrib.streams.postgres import PostgresStreams

client = await Client.connect(
    "localhost:7233",
    plugins=[PostgresStreams("postgresql://app:secret@db:5432/app")],
)
```

PostgreSQL 14 or later is required. The stream runtime refuses an older server when the
client connects the store. Amazon RDS for PostgreSQL and Aurora PostgreSQL
14 and later work. The stream runtime's tests run against PostgreSQL 14 and 17.

## Postgres or Redis

Both stores keep the same contract. Readers, cursors, replays and the
notification log behave the same, so code does not change when you move
from one to the other. A stream does not move, so pick before you go to
production.

Pick Postgres when

- you already run PostgreSQL and do not want another system to operate,
- you keep streams for weeks or months. Postgres keeps records on disk, so
  a long retention costs disk, not memory. Redis keeps every record in
  memory, and needs an archive for long retention,
- you want a durable commit with no extra settings. An acknowledged append
  is in the write-ahead log, and a synchronous standby keeps it through a
  failover.

Pick Redis when

- one stream takes thousands of appends a second. One topic's appends run
  one at a time in Postgres, so a topic takes about 200 to 1,000 appends a
  second on a managed database. A batch of records counts as one append.
  Redis takes tens of thousands,
- readers that do not use the server's notifier need a tail in about a
  millisecond. See [Waiting reads](#waiting-reads).

## What an acknowledged append means

An append is one transaction. It returns after the commit, so a batch is
written in full or not at all, in order. A retry of the newest batch on the
same producer returns the original position and writes nothing.

How long an acknowledged record lives through a failure depends on the
database.

- With `synchronous_commit = on`, the PostgreSQL default, a commit waits
  for the WAL flush. The record lives through a crash and a power loss.
- With `synchronous_commit = off`, a crash can lose the last few hundred
  milliseconds of appends. Do not set it for the store's role.
- A failover to an asynchronous replica can lose appends the primary
  acknowledged. Use a synchronous standby, such as RDS Multi-AZ, or Aurora,
  which writes to a quorum of storage nodes.

After a failover, calls fail until the pool reaches the new primary. An
append whose result is not known raises `StreamOutcomeUnknownError`, and a
retry on the same producer answers it with no duplicate.

## Waiting reads

A read that waits for new records holds no database connection. Three things
wake it.

1. An append in the same process wakes its readers at once.
2. Each process polls for all of its waiting reads with one query every
   `poll_interval` (100 ms). So a reader in another process sees a new
   record within about that time. The cost grows with the number of
   processes, not with the number of readers.
3. With `notify_reads=True`, each append also sends `NOTIFY`, and readers in
   other processes wake in a few milliseconds. This costs append throughput
   when many producers write at once, so it is off by default.

Readers that sleep on the server's stream notifier, through
`notify_on_append()` or a `StreamDriver`, wake the same way on both stores.
For most readers that is the main path, and the poll is a fallback. Set
`notify_reads` only for readers that do not use the notifier and need a
tail below `poll_interval`.

## Retention and cleanup

A janitor deletes records older than `retention` (7 days), once a minute by
default. Every process that connects the store runs one, and an advisory
lock lets one work at a time. It keeps what a Workflow reader's hold or the
archive still needs.

- A topic whose records are all gone keeps one row for `retention` plus 30
  days past its last write. That lets a reader tell a cursor whose records expired
  (`StreamExpiredError`) from a stream that never existed.
- Deleting a Workflow does not delete its streams. To remove them at once,
  call `PostgresStreams.delete_workflow_streams(namespace, workflow_id)` or
  `delete_activity_streams(namespace, activity_id)`. Run it only after the
  chain closed and its producers stopped. A late append writes a fresh
  stream.
- Size the disk for your write rate times `retention`. A 256-byte record
  takes about 360 bytes with its index entry.

The janitor's deletes leave dead rows for autovacuum. The default
autovacuum settings keep up at moderate write rates. Watch
`pg_stat_user_tables.n_dead_tup` on `records` at high rates.

## Migrations

By default the store makes its tables and functions in `schema`
(`temporal_streams`) when it connects, and upgrades them on a later
release. An advisory lock makes many Workers that start at once apply each
migration once. The store refuses to start when an applied migration
differs from the one the stream runtime holds, or when the database is newer than the
release can read.

For a database where applications may not change the schema, pass
`skip_migrations=True`. The store then only checks that every migration it
knows is applied. A DBA applies the SQL files in
`crates/streams/src/postgres/migrations/` of the sdk-streams repository, in order,
after `SET search_path = <schema>, pg_temp`, and adds a row to
`schema_migrations` for each. Running the store once with a role that may
create the schema does the same. Then grant the application's role the
rights below. It needs no `CREATE`.

```sql
CREATE ROLE streams_app LOGIN PASSWORD 'secret';
GRANT USAGE ON SCHEMA temporal_streams TO streams_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA temporal_streams TO streams_app;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA temporal_streams TO streams_app;
```

Grant again after a migration adds a table or a function. The stream runtime's conformance
suite runs once as a role with exactly these grants.

One schema per isolation domain, such as a namespace or a team, with its own
role and grants, keeps domains apart, as a Redis key prefix and ACL do.

## PgBouncer and other poolers

The store keeps a pool of `max_connections` (16) per process. Waiting reads
hold none.

- PgBouncer in session mode works with no setting.
- PgBouncer in transaction mode works from PgBouncer 1.21, with
  `max_prepared_statements` set, since the store prepares its statements.
  `LISTEN` and the migrations' session lock do not work through
  transaction mode. So set `listen_url` to a direct or session-mode URL of
  the same database. The store runs its migrations there, and with a
  notification log or `notify_reads` it keeps one listening connection per
  process there. That connection opens when the store connects, so a
  `listen_url` the process can't reach fails the connect.
- RDS Proxy pins a connection that listens. Point `listen_url` at the
  instance or cluster endpoint.

Both URLs must name the primary. A read replica or an Aurora reader
endpoint refuses writes and `LISTEN`.

## TLS

The store reads `sslmode` and `sslrootcert` from the URL, as libpq does.
The default is `prefer`, which falls back to no TLS. For a database on
another host, use `sslmode=verify-full`. The stream runtime logs a warning for a remote
host with another mode. RDS and Aurora certificates chain to the Amazon RDS
certificate authorities, which system trust stores do not hold. Download
the RDS bundle and pass it in the URL.

```
postgres://app:<password>@<writer>:5432/<db>?sslmode=verify-full&sslrootcert=/etc/rds.pem
```

IAM database authentication is not in this release. The password comes
from the URL.

## The archive

Postgres keeps records on disk, so it does not need an archive to keep
streams for months. An archive is optional, and is worth it for retention
in years, for more than about 1 TB of records, or to keep backups small.

```python
from datetime import timedelta

from temporalio.contrib.streams.archive import StreamArchive
from temporalio.contrib.streams.postgres import PostgresStreams

store = PostgresStreams(
    "postgresql://app:secret@db:5432/app",
    retention=timedelta(days=7),
    archive=StreamArchive("s3://my-bucket/streams"),
).notify_on_append(notification_log=True)
```

A running `StreamDriver` copies each stream's records to the archive, and
reads below what Postgres holds read the archive with the same cursors.
Postgres keeps each record the archive does not hold yet, for at most the
archive's `stall_limit` (7 days). See
[Long retention](redis-guarantees.md#long-retention) for how the archive
works. It is the same on both stores.

## What crosses the wire

- **Record bodies** go through your data converter's payload codec before
  they are stored, so an encrypting codec keeps them encrypted in the
  database.
- **Workflow ids, topic names and producer ids** are stored in clear text.
  Do not put secrets in them.
- **Content hashes are not encrypted.** Each record carries the SHA-256 of
  its body as the converter produced it, before the codec. Anyone who can
  read the tables can tell when two records carry the same value. If that
  matters for your data, add something unique, such as a random field, to
  each value.
- Temporal does not authorize or meter record traffic. Records go straight
  between your applications and your database, so access is the database's
  job.
