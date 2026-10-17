"""The customer-run process that turns a store's notification log into notifications.

With ``notify_on_append(notification_log=True)``, an append writes a log
entry in the same store step and the producer sends nothing to Temporal. A
:class:`StreamDriver` reads that log through the stream runtime and calls the server's
``NotifyStreams`` for the streams that moved, so readers that sleep on a
stream wake. The stream runtime does the work: it reads the doorbells, sweeps the busy
slots, takes entries as a consumer group, folds each drain to the newest
counter per stream, and acks the entries only once the call landed.
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

from temporalio.bridge.proto.streams import RunStreamDriverRequest
from temporalio.contrib.streams._plugin import call, store_for_client

if TYPE_CHECKING:
    from temporalio.client import Client
    from temporalio.contrib.streams._plugin import StreamStorePlugin

__all__ = ["StreamDriver"]


class StreamDriver:
    """Forwards a store's notification log to the server's stream notifiers.

    Run one or more per store, each with its own ``consumer``. Several
    drivers share the log's entries, and one takes over what a dead one left
    unacked. Delivery is at least once. A repeat carries an old counter and
    wakes nobody.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        client: Client,
        store: StreamStorePlugin | None = None,
        *,
        consumer: str | None = None,
        sweep_interval: timedelta | None = None,
        claim_idle: timedelta | None = None,
        batch: int | None = None,
    ) -> None:
        """Set the driver up. :meth:`run` starts it.

        Args:
            client: The client whose namespace the driver notifies. It needs
                the right to call ``NotifyStreams`` there.
            store: The store whose log the driver reads. Without one, the
                store registered on ``client``. A store given here that no
                client connected yet is connected through ``client``.
            consumer: The driver's name in the log's consumer group. Without
                one, the host name, the process id and a random suffix.
            sweep_interval: How often the driver looks at every slot that
                holds entries, which bounds how long a lost doorbell delays
                a notification. The stream runtime's 30 seconds when unset.
            claim_idle: How long another driver's unacked entries wait
                before this one takes them. The stream runtime's 60 seconds when unset.
            batch: The most entries one read takes. The stream runtime's 100 when unset.

        Raises:
            ValueError: The store doesn't keep a notification log, or
                ``batch`` is below one.
        """
        self._client = client
        self._store = store if store is not None else store_for_client(client)
        if not self._store._config.notification_log:
            raise ValueError(
                "the stream store keeps no notification log; turn it on with "
                "notify_on_append(notification_log=True)"
            )
        if batch is not None and batch < 1:
            raise ValueError(f"batch must be at least 1, got {batch}")
        self._request = RunStreamDriverRequest(
            consumer=consumer
            or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}",
            batch=batch or 0,
        )
        if sweep_interval is not None:
            self._request.sweep_interval.FromTimedelta(sweep_interval)
        if claim_idle is not None:
            self._request.claim_idle.FromTimedelta(claim_idle)

    @property
    def consumer(self) -> str:
        """The driver's name in the log's consumer group."""
        return self._request.consumer

    async def run(self) -> None:
        """Run until the task that awaits it is cancelled.

        Raises:
            temporalio.contrib.streams.StreamRefusedError: The store keeps no
                notification log.
            temporalio.contrib.streams.StreamUnsupportedError: The server
                has no stream notifiers.
            temporalio.contrib.streams.StreamStorageError: The store failed
                for good.
        """
        service = await self._store._service_for(self._client)
        await call(service.run_stream_driver(self._request))
