"""Streams in the application's PostgreSQL database."""

from __future__ import annotations

from datetime import timedelta

from temporalio.bridge.proto.streams import PostgresStoreConfig, StreamStoreConfig
from temporalio.contrib.streams._plugin import (
    StreamStorePlugin,
    with_owner_start_grace,
)
from temporalio.contrib.streams.archive import StreamArchive

__all__ = ["PostgresStreams"]

_SCHEMES = ("postgres://", "postgresql://")


class PostgresStreams(StreamStorePlugin):
    """A store in a PostgreSQL database that the stream runtime connects to.

    One instance per database. Pass it to the client as a plugin, and open
    handles from the client with
    :func:`temporalio.contrib.streams.stream_handle`. PostgreSQL 14 or
    later is required. The store keeps records on disk, so a long retention
    costs disk, not memory. By default it makes and upgrades its tables in
    ``schema`` when it connects.
    """

    def __init__(
        self,
        url: str,
        *,
        schema: str = "temporal_streams",
        retention: timedelta = timedelta(days=7),
        max_connections: int | None = None,
        response_timeout: timedelta | None = None,
        floor_lease: timedelta | None = None,
        floor_release_grace: timedelta | None = None,
        owner_start_grace: timedelta | None = None,
        notify_doorbells: int | None = None,
        listen_url: str | None = None,
        poll_interval: timedelta | None = None,
        notify_reads: bool = False,
        skip_migrations: bool = False,
        janitor_interval: timedelta | None = None,
        archive: StreamArchive | None = None,
    ) -> None:
        """Create the store's configuration.

        Args:
            url: A ``postgres://`` or ``postgresql://`` URL. Credentials come
                from the URL. TLS follows its ``sslmode`` and ``sslrootcert``
                as libpq reads them, and ``sslmode=prefer`` when unset.
            schema: The schema that holds the store's tables, in lowercase
                letters, digits and underscores, at most 55 characters. A
                schema per isolation domain, with its own grants, keeps
                domains apart.
            retention: How long a stream keeps a record. A janitor in each
                process deletes older records, one process at a time.
            max_connections: How many connections the pool keeps. Waiting
                reads hold none. The stream runtime's 16 when unset.
            response_timeout: How long a call waits, as the session's
                ``statement_timeout``. The stream runtime's 5 seconds when unset.
            floor_lease: How long a Workflow reader that replays from the
                store, as ``workflow_reader`` does by default, holds the
                records it read against retention after its last read.
                ``retention`` when unset.
            floor_release_grace: How long such a hold lasts after a reader
                releases it. One day when unset. A Python Workflow reader
                never releases, since its run may replay its reads until it
                ends, so its hold lasts ``floor_lease``.
            owner_start_grace: How long a read of a Workflow that Temporal
                doesn't know yet answers empty before it fails with
                :class:`temporalio.contrib.streams.StreamNotFoundError`, so
                a reader can start before its owner. 60 seconds when unset.
                Zero fails at once.
            notify_doorbells: With a notification log, how many channels the
                log rings when a partition takes a first entry. A driver
                listens on each one. The stream runtime's 16 when unset, at most 64.
            listen_url: A URL for the one connection that listens for
                notifications, when ``url`` goes through a pooler in
                transaction mode, such as PgBouncer. Name the database
                directly or through a pooler in session mode. ``url`` when
                unset.
            poll_interval: How often a process looks for new records for its
                waiting reads, with one query for all of them. The stream runtime's 100 ms
                when unset.
            notify_reads: Each append also sends ``NOTIFY``, so a waiting
                read in another process wakes in milliseconds instead of at
                the next poll. It costs append throughput when many
                producers write at once.
            skip_migrations: The store only checks the schema version and
                never changes the schema. A DBA applies the migrations, and
                the role in ``url`` needs no ``CREATE``.
            janitor_interval: How often the janitor deletes expired records.
                The stream runtime's one minute when unset.
            archive: Object storage that keeps records after ``retention``
                deletes them. A running
                :class:`temporalio.contrib.streams.StreamDriver` fills it,
                and the store needs
                ``notify_on_append(notification_log=True)``. The stream runtime refuses
                to connect the store without the log.

        Raises:
            ValueError: ``url`` or ``listen_url`` is not a PostgreSQL URL,
                ``schema`` is empty, ``retention`` is shorter than a
                millisecond, ``notify_doorbells`` is not from 1 to 64, a
                duration is not positive, or ``owner_start_grace`` is
                negative.
        """
        if not url.startswith(_SCHEMES):
            raise ValueError(f"PostgresStreams needs a postgres:// URL, got {url!r}")
        if listen_url is not None and not listen_url.startswith(_SCHEMES):
            raise ValueError(
                f"listen_url must be a postgres:// URL, got {listen_url!r}"
            )
        if not schema:
            raise ValueError("schema must not be empty")
        if retention < timedelta(milliseconds=1):
            raise ValueError(
                f"retention must be at least a millisecond, got {retention}"
            )
        if notify_doorbells is not None and not 1 <= notify_doorbells <= 64:
            raise ValueError(
                f"notify_doorbells must be from 1 to 64, got {notify_doorbells}"
            )
        durations = {
            "response_timeout": response_timeout,
            "floor_lease": floor_lease,
            "floor_release_grace": floor_release_grace,
            "poll_interval": poll_interval,
            "janitor_interval": janitor_interval,
        }
        for name, value in durations.items():
            if value is not None and value <= timedelta(0):
                raise ValueError(f"{name} must be positive, got {value}")
        config = PostgresStoreConfig(
            url=url,
            schema=schema,
            max_connections=max_connections or 0,
            notify_doorbells=notify_doorbells or 0,
            listen_url=listen_url or "",
            notify_reads=notify_reads,
            skip_migrations=skip_migrations,
        )
        config.retention.FromTimedelta(retention)
        for name, value in durations.items():
            if value is not None:
                getattr(config, name).FromTimedelta(value)
        store = StreamStoreConfig(postgres=config)
        if archive is not None:
            store.archive.CopyFrom(archive._config)
        super().__init__(
            "temporalio.contrib.streams.PostgresStreams",
            with_owner_start_grace(store, owner_start_grace),
        )
