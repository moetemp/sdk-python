"""The stream service: streams over a Nexus endpoint.

.. warning::
    This package is experimental and may change in future versions.

A process that cannot reach a stream's store, or should not, appends to and
reads from it through a Nexus endpoint. The endpoint runs the
``temporal.sdk.streams.v1.TemporalStreams`` service over the store the
endpoint's Worker carries, so callers never learn which store that is.

Core serves the service, so it is defined once, in Core's
``crates/streams/nexus/temporal_streams.nexusrpc.yaml``. Everything in
:mod:`temporalio.contrib.streams.nexus._generated` is generated from the copy
in the Core this SDK vendors, by ``scripts/gen_nexus_streams_api.py``:

* :class:`TemporalStreams`, the service definition a handler implements and a
  Workflow's Nexus client calls.
* The models its operations take and return. :class:`StreamRef` is
  :class:`temporalio.contrib.streams.StreamRef` itself, so a reference a
  Workflow hands out is the value the service takes.
* :class:`TemporalStreamsHttpClient`, a caller for a process outside any
  Worker, posting to the endpoint over the Nexus HTTP ingress. It depends on
  nothing beyond the standard library.
* ``TemporalStreamsForwarder``, which hands each operation's input to Core as
  the contract's JSON transfer form.

:class:`TemporalStreamsHandler` is the one hand-written piece. It runs its
authorizer and the namespace's payload codec, the parts only the SDK has, and
Core runs every rule of the contract on the store of its Worker's client.
"""

from temporalio.contrib.streams import StreamRef
from temporalio.contrib.streams.nexus._generated import (
    AppendInput,
    AppendOutput,
    ReadInput,
    ReadOutput,
    RecordWire,
    StreamCursor,
    TemporalStreams,
)
from temporalio.contrib.streams.nexus._generated.client import (
    HTTPStatusError,
    TemporalStreamsHttpClient,
)
from temporalio.contrib.streams.nexus._handler import (
    StreamAccess,
    TemporalStreamsHandler,
)

__all__ = [
    "AppendInput",
    "AppendOutput",
    "HTTPStatusError",
    "ReadInput",
    "ReadOutput",
    "RecordWire",
    "StreamAccess",
    "StreamCursor",
    "StreamRef",
    "TemporalStreams",
    "TemporalStreamsHandler",
    "TemporalStreamsHttpClient",
]
