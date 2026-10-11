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

A Nexus operation can also hand its caller a stream.
:class:`StreamOperationHandler` is such an operation: its start attaches the
caller to the stream's notifier on the server and puts the
:class:`StreamRef` in the operation token (:func:`stream_ref_from_token`).
With :meth:`temporalio.contrib.streams.StreamStorePlugin.notify_on_append`
on, Core tells the notifier when the stream moves, which the caller sees as
operation progress.
:meth:`temporalio.contrib.streams.StreamStorePlugin.close_stream` completes
the operation, or :func:`close_workflow_stream` from the owning Workflow.

A caller Workflow reads such a stream with :class:`StreamReader`: it waits on
the operation's progress and reads through this service as Nexus operations
of the Workflow, so a replay hands over the same batches.
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
from temporalio.contrib.streams.nexus._operation import (
    StreamOperationHandler,
    stream_ref_from_token,
)
from temporalio.contrib.streams.nexus._reader import (
    ReadPosition,
    ReadStaleRecord,
    ReadSupersession,
    StreamIncompleteError,
    StreamReader,
)
from temporalio.contrib.streams.nexus._workflow import close_workflow_stream

__all__ = [
    "AppendInput",
    "AppendOutput",
    "HTTPStatusError",
    "ReadInput",
    "ReadOutput",
    "ReadStaleRecord",
    "ReadPosition",
    "ReadSupersession",
    "RecordWire",
    "StreamAccess",
    "StreamCursor",
    "StreamIncompleteError",
    "StreamOperationHandler",
    "StreamReader",
    "StreamRef",
    "TemporalStreams",
    "TemporalStreamsHandler",
    "TemporalStreamsHttpClient",
    "close_workflow_stream",
    "stream_ref_from_token",
]
