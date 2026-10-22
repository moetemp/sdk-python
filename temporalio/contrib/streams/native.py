"""The native Stream plugin, whose Streams the Temporal server keeps.

.. warning::
    This module is experimental.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.client
import temporalio.runtime
from temporalio.contrib.streams._backend import _ExtensionStreamBackend
from temporalio.contrib.streams.proto import v1 as ext_pb
from temporalio.contrib.streams.redis import _ExtensionStreamPlugin

NATIVE_EXTENSION_NAME = "temporalio-streams-native"


class _NativeBackend(_ExtensionStreamBackend):
    def for_client(self, client: temporalio.client.Client) -> _NativeBackend:
        # A process with no Worker gives the extension its client's connection. A Worker
        # gives it the Worker's at its start.
        self._extension.attach(client)
        return self


class NativeStreamPlugin(_ExtensionStreamPlugin):
    """Carries Stream calls to the Temporal server, which keeps the Streams.

    Workflow writes go through the Core extension's buffer, as on Redis, and the extension
    writes them to the server's ``temporal.api.streamservice.v1.StreamService`` on the Worker's
    connection. Activity and client calls go to the same service through the extension. It
    registers the backend name ``native``. Put it on the client next to ``RedisStreamPlugin``
    to keep old Streams on Redis and create new ones on the server.

    .. warning::
        This API is experimental.
    """

    _backend_class = _NativeBackend

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
        config = ext_pb.RedisStreamConfig()
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
