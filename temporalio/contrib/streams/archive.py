"""Object storage that keeps a store's records after the hot store drops them."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from temporalio.bridge.proto.streams import ArchiveConfig

__all__ = ["StreamArchive"]


class StreamArchive:
    """An archive tier for a store, in S3 or in a local directory.

    Pass it as ``archive=`` to
    :class:`temporalio.contrib.streams.redis.RedisStreams` or
    :class:`temporalio.contrib.streams.postgres.PostgresStreams`. A
    :class:`temporalio.contrib.streams.StreamDriver` copies each stream's
    records to the archive in segments, with the positions the hot store
    gave them. The hot store keeps a record until the archive holds it. A
    read below what the hot store still holds reads the archive, with the
    same records, cursors and positions, so readers and Workflow replays
    need no change. Nothing archives while no driver runs.

    The archive needs the store's notification log, so turn it on with
    ``notify_on_append(notification_log=True)``. The stream runtime deletes archived
    records only for a stream delete. Set a lifecycle rule on the bucket for
    how long to keep them. A read below what the lifecycle kept fails with
    :class:`temporalio.contrib.streams.StreamExpiredError`.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        location: str | os.PathLike[str],
        *,
        segment_bytes: int | None = None,
        segment_idle: timedelta | None = None,
        stall_limit: timedelta | None = None,
    ) -> None:
        """Name the archive.

        Args:
            location: An ``s3://bucket/prefix`` URL, a ``file://`` URL, or a
                local directory. S3 credentials, region and endpoint come
                from the environment of the process that connects the
                store: ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
                ``AWS_REGION``, and ``AWS_ENDPOINT_URL`` with
                ``AWS_ALLOW_HTTP=true`` for an S3-compatible server. Bodies
                are archived as the payload codec encoded them. Set bucket
                encryption for the rest.
            segment_bytes: A segment, one object, ends at the first record
                that takes it to this many bytes. The stream runtime's 4 MiB when unset.
            segment_idle: How long a stream goes with no write before the
                driver archives its tail. A stream that keeps taking writes
                has its tail archived this often too. The stream runtime's 5 minutes when
                unset.
            stall_limit: How long the hot store keeps records the archive
                does not hold yet, after the archive last moved. When no
                driver runs for longer, retention trims them again and a
                read of them fails as expired. The stream runtime's 7 days when unset.

        Raises:
            ValueError: ``location`` is empty or names another scheme, or a
                size or duration is not positive.
        """
        url = _url(location)
        if segment_bytes is not None and segment_bytes < 1:
            raise ValueError(f"segment_bytes must be at least 1, got {segment_bytes}")
        for name, value in (
            ("segment_idle", segment_idle),
            ("stall_limit", stall_limit),
        ):
            if value is not None and value <= timedelta(0):
                raise ValueError(f"{name} must be positive, got {value}")
        self._config = ArchiveConfig(url=url, segment_bytes=segment_bytes or 0)
        if segment_idle is not None:
            self._config.segment_idle.FromTimedelta(segment_idle)
        if stall_limit is not None:
            self._config.stall_limit.FromTimedelta(stall_limit)

    @property
    def url(self) -> str:
        """The archive's URL, with a local directory as a ``file://`` URL."""
        return self._config.url


def _url(location: str | os.PathLike[str]) -> str:
    if isinstance(location, os.PathLike) or "://" not in location:
        if not str(location):
            raise ValueError("StreamArchive needs a location")
        return Path(location).expanduser().resolve().as_uri()
    scheme = location.split("://", 1)[0]
    if scheme not in ("s3", "file"):
        raise ValueError(f"StreamArchive takes s3:// or file:// URLs, got {location}")
    return location
