"""The native Stream plugin, whose Streams the Temporal server keeps.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
import temporalio.exceptions
import temporalio.nexus.system
import temporalio.runtime
import temporalio.worker
import temporalio.workflow
from temporalio.contrib.streams import _workflow_reader
from temporalio.contrib.streams._backend import _ExtensionStreamBackend
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import _ExtensionStreamPlugin, _WorkflowOutbound

NATIVE_EXTENSION_NAME = "temporalio-streams-native"
_SERVICE = "temporal.api.streamservice.v1.StreamService"

_WAIT_COMPLETED = temporalio.workflow.NexusOperationCancellationType.WAIT_COMPLETED


class _NativeBackend(_ExtensionStreamBackend):
    def for_client(self, client: temporalio.client.Client) -> _NativeBackend:
        # A process with no Worker gives the extension its client's connection. A Worker
        # gives it the Worker's at its start.
        self._extension.attach(client)
        return self


class _NativeWorkflowOutbound(_WorkflowOutbound):
    # The server parks a read at the end of the Stream and completes it on an append or a
    # close, so a reader needs no timer.
    _temporal_stream_parks_reads = True
    _temporal_stream_read_wait = timedelta(seconds=60)

    async def _temporal_stream_read_page(
        self, request: stream_pb.ReadRecordsRequest
    ) -> stream_pb.ReadRecordsResponse:
        # The page is a System Nexus operation, so its answer is in History.
        handle = await self.next.start_system_nexus_operation(
            temporalio.worker.StartNexusOperationInput(
                endpoint=temporalio.nexus.system.TEMPORAL_SYSTEM_ENDPOINT,
                service=_SERVICE,
                operation="ReadRecords",
                input=request,
                output_type=stream_pb.ReadRecordsResponse,
                schedule_to_close_timeout=None,
                schedule_to_start_timeout=None,
                start_to_close_timeout=None,
                cancellation_type=_WAIT_COMPLETED,
                headers=None,
                summary=None,
            )
        )
        try:
            return await handle
        except temporalio.exceptions.NexusOperationError as err:
            raise _workflow_reader.error_of(err) from None


class NativeStreamPlugin(_ExtensionStreamPlugin):
    """Carries Stream calls to the Temporal server, which keeps the Streams.

    Workflow writes run as Local Activities, as on Redis, and the extension writes them to the
    server's ``temporal.api.streamservice.v1.StreamService`` on the Worker's connection.
    Activity and client calls go to the same service through the extension. It
    registers the backend name ``native``. Put it on the client next to ``RedisStreamPlugin``
    to keep old Streams on Redis and create new ones on the server.

    .. warning::
        This API is experimental.
    """

    _backend_class = _NativeBackend
    _outbound_class = _NativeWorkflowOutbound
    _server_closes_with_run = True

    def __init__(
        self,
        *,
        retention: timedelta | None = None,
        runtime: temporalio.runtime.Runtime | None = None,
        name: str = "native",
        default: bool = False,
    ) -> None:
        """Create the plugin. It makes no call.

        Args:
            retention: The default Retention of a new Stream. Defaults to the server's.
            runtime: The runtime of the client. Defaults to the default runtime.
            name: The backend name that Stream handles carry.
            default: Make this the default backend when the client or Worker has more than
                one Stream plugin. With none marked, the first one is the default.
        """
        config = ext_pb.NativeStreamConfig()
        if retention is not None:
            config.retention.FromTimedelta(retention)
        super().__init__(
            "temporalio.NativeStreamPlugin",
            NATIVE_EXTENSION_NAME,
            config,
            runtime,
            name,
            default,
        )
