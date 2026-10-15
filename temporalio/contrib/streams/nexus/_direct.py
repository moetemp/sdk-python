"""Direct stream reads: a caller Workflow's reads keep their records out of History.

A :class:`StreamReader` built with ``replay_from_store=True`` reads in a Local
Activity through a :class:`StreamDirectTransport`, not as a Nexus operation of
the Workflow. The read names a floor holder, so the owner's store holds the
records it read, and the Activity's result, and so its marker, holds only the
range that crossed and Core's digest of its records. Before Workflow code sees
the result, the Worker turns it back into the batch. It takes the batch the
Activity cached in this process, or else reads the range again through the
stream service's ``read_range``, which checks it against the digest. So
Workflow code gets the same records in both modes, and no I/O runs in Workflow
code.

The step runs as part of the client's payload codec, on decode, as for the
positions-only Workflow reader of :mod:`temporalio.contrib.streams`. The
:class:`StreamDirectReads` plugin wraps the codec of the client and of a
Replayer, and every other payload passes through to the codec it wraps.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import uuid
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import timedelta
from ssl import SSLContext
from typing import TYPE_CHECKING, Any, Protocol

import temporalio.activity
import temporalio.workflow
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import NexusHandlerErrorRetryBehavior
from temporalio.api.failure.v1 import Failure
from temporalio.api.workflowservice.v1 import ExecuteSyncNexusOperationRequest
from temporalio.common import RawValue
from temporalio.contrib.streams._errors import (
    StreamClosedError,
    StreamCursorError,
    StreamError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamRefusedError,
    StreamReplayMismatchError,
    StreamReplayRangeLostError,
    StreamStorageError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._replay_reads import (
    ReadChain,
    ReadStates,
    ReplayReadCodec,
)
from temporalio.contrib.streams.nexus._follower import StreamReadTransport
from temporalio.contrib.streams.nexus._generated import (
    FollowInput,
    FollowOutput,
    FollowRangeInput,
    ReadInput,
    ReadOutput,
    ReadOutputReplay,
    ReadRangeInput,
    RecordWire,
)
from temporalio.contrib.streams.nexus._generated.client import (
    HTTPStatusError,
    TemporalStreamsHttpClient,
)
from temporalio.converter import (
    DataConverter,
    PayloadCodec,
    PayloadConverter,
    SerializationContext,
    WithSerializationContext,
)
from temporalio.exceptions import ApplicationError
from temporalio.plugin import SimplePlugin
from temporalio.service import RPCError, RPCStatusCode

if TYPE_CHECKING:
    from temporalio.client import Client
    from temporalio.worker import WorkerConfig

__all__ = [
    "ClientTransport",
    "HttpTransport",
    "StreamDirectReads",
    "StreamDirectTransport",
    "StreamServiceCallError",
]

DIRECT_READ_ACTIVITY = "__temporal_streams_nexus_direct_read"
"""The Local Activity a direct reader reads in."""

DIRECT_FOLLOW_ACTIVITY = "__temporal_streams_nexus_direct_follow"
"""The Local Activity a direct reader that follows links follows in."""

FOLLOW_POSITIONS_TYPE = b"temporal.io/stream-nexus-follow-positions"
"""The ``messageType`` of a direct follow's result, before the codec step."""

FOLLOW_BATCH_TYPE = b"temporal.io/stream-nexus-follow-batch"
"""The ``messageType`` of a direct follow's result once the codec step turned it back."""

POSITIONS_TYPE = b"temporal.io/stream-nexus-read-positions"
"""The ``messageType`` of a direct read's result, before the codec step."""

BATCH_TYPE = b"temporal.io/stream-nexus-read-batch"
"""The ``messageType`` of a direct read's result once the codec step turned it back."""

_ENDPOINT_KEY = "temporal.io/stream-nexus-endpoint"
# Batches the read Activity read in this process, so the live Workflow Task
# turns its result back without a second read.
_CACHE_SIZE = 256
# The default payload converter writes the contract's JSON transfer form.
_CONTRACT = PayloadConverter.default
# Conditions that a repeat of the same read gets again, so the Activity stops.
_FINAL_ERRORS: tuple[type[StreamError], ...] = (
    StreamClosedError,
    StreamCursorError,
    StreamExpiredError,
    StreamNotFoundError,
    StreamRefusedError,
    StreamUnsupportedError,
)
_NAMED_ERRORS: dict[str, type[StreamError]] = {
    error.__name__: error
    for error in (
        *_FINAL_ERRORS,
        StreamReplayMismatchError,
        StreamReplayRangeLostError,
        StreamStorageError,
    )
}
_SERVICE = "temporal.sdk.streams.v1.TemporalStreams"
# Nexus handler errors that a repeat of the same call gets again.
_FINAL_HANDLER_ERRORS = {
    "BAD_REQUEST",
    "UNAUTHENTICATED",
    "UNAUTHORIZED",
    "NOT_FOUND",
    "NOT_IMPLEMENTED",
}
# Answers to the RPC that a repeat of the same call gets again.
_FINAL_STATUSES = {
    RPCStatusCode.INVALID_ARGUMENT,
    RPCStatusCode.FAILED_PRECONDITION,
    RPCStatusCode.NOT_FOUND,
    RPCStatusCode.PERMISSION_DENIED,
    RPCStatusCode.UNAUTHENTICATED,
    RPCStatusCode.UNIMPLEMENTED,
}
_OPT_OUT = (
    "pass replay_from_store=False to the StreamReader to record each batch in "
    "History instead"
)


class StreamDirectTransport(StreamReadTransport, Protocol):
    """How a direct reader reaches the stream service of an endpoint.

    .. warning::
        This API is experimental and may change in future versions.

    A transport raises the stream error class the service named, such as
    :class:`temporalio.contrib.streams.StreamReplayMismatchError`, and any
    other error for a failure on the way.
    """

    async def read_range(self, request: ReadRangeInput) -> ReadOutput:
        """Answer again the records of an earlier read that named a floor holder."""
        ...

    async def follow(self, request: FollowInput) -> FollowOutput:
        """Answer with the items after ``request``'s position on a stream and its links."""
        ...

    async def follow_range(self, request: FollowRangeInput) -> FollowOutput:
        """Answer again the items of an earlier follow that named a floor holder."""
        ...


class HttpTransport:
    """Reads through a Nexus endpoint's HTTP ingress.

    .. warning::
        This API is experimental and may change in future versions.

    It works on a self-hosted server only, since Temporal Cloud has no HTTP
    ingress for endpoints. The server checks the credential in ``headers``
    or ``ssl_context`` against the endpoint's target namespace, the owner's,
    and asks for write access there. So the caller's Worker holds a write
    credential on the owner namespace. The handler's ``authorize`` is the
    check per stream.
    """

    def __init__(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        ssl_context: SSLContext | None = None,
        timeout: float = 30.0,
        payload_codec: PayloadCodec | None = None,
    ) -> None:
        """Read through the stream service at ``url``.

        Args:
            url: The service's address on the HTTP ingress. That's
                ``{http}/nexus/endpoints/{endpoint id}/services/`` followed
                by ``temporal.sdk.streams.v1.TemporalStreams``. Listing
                endpoint ids needs cluster admin, so the app configures it.
            headers: Sent with each call, such as ``Authorization``.
            ssl_context: The TLS for an ``https`` ``url``, such as a client
                certificate for mTLS.
            timeout: How long one call may take, in seconds.
            payload_codec: The codec of the namespace that runs the handler.
                Usually the one the caller's data converter has. Not the
                client's wrapped codec, which would call itself.
        """
        self._client = TemporalStreamsHttpClient(
            url,
            headers=dict(headers or {}),
            timeout=timeout,
            ssl_context=ssl_context,
            payload_codec=payload_codec,
        )

    async def read(self, request: ReadInput) -> ReadOutput:
        """Answer with the records after ``request``'s cursor."""
        try:
            return await self._client.read(request)
        except HTTPStatusError as error:
            raise _named(error) from error

    async def read_range(self, request: ReadRangeInput) -> ReadOutput:
        """Answer again the records of an earlier read that named a floor holder."""
        try:
            return await self._client.read_range(request)
        except HTTPStatusError as error:
            raise _named(error) from error

    async def follow(self, request: FollowInput) -> FollowOutput:
        """Answer with the items after ``request``'s position on a stream and its links."""
        try:
            return await self._client.follow(request)
        except HTTPStatusError as error:
            raise _named(error) from error

    async def follow_range(self, request: FollowRangeInput) -> FollowOutput:
        """Answer again the items of an earlier follow that named a floor holder."""
        try:
            return await self._client.follow_range(request)
        except HTTPStatusError as error:
            raise _named(error) from error


class ClientTransport:
    """Reads through ``ExecuteSyncNexusOperation`` with a Temporal client.

    .. warning::
        This API is experimental and may change in future versions.

    The call carries the client's own credential and namespace. The server
    checks them against the caller's namespace, finds the endpoint as that
    namespace sees it, and tells the handler the caller's namespace, which
    ``authorize`` gets as :attr:`StreamAccess.caller_namespace`. It needs a
    server with ``ExecuteSyncNexusOperation`` turned on
    (``frontend.enableExecuteSyncNexusOperation``).

    The input and the answer cross through the client's payload codec, which
    must be the codec of the handler's namespace too. It uses the codec under
    the wrappers of :class:`StreamDirectReads` and of the stream store, since
    the replay step runs inside those wrappers.
    """

    def __init__(
        self,
        client: Client,
        endpoint: str,
        *,
        timeout: timedelta = timedelta(seconds=10),
    ) -> None:
        """Read through ``endpoint`` as a caller in ``client``'s namespace.

        Args:
            client: The caller's client. A Replayer has none, so pass one
                here for the transport a Replayer's plugin uses.
            endpoint: The Nexus endpoint's name.
            timeout: How long one call may take. The server cuts the
                handler's deadline to its Nexus request timeout, 10 seconds
                by default.
        """
        self._client = client
        self._endpoint = endpoint
        self._timeout = timeout

    async def read(self, request: ReadInput) -> ReadOutput:
        """Answer with the records after ``request``'s cursor."""
        return _from_json(await self._call("read", request), ReadOutput)

    async def read_range(self, request: ReadRangeInput) -> ReadOutput:
        """Answer again the records of an earlier read that named a floor holder."""
        return _from_json(await self._call("read_range", request), ReadOutput)

    async def follow(self, request: FollowInput) -> FollowOutput:
        """Answer with the items after ``request``'s position on a stream and its links."""
        return _from_json(await self._call("follow", request), FollowOutput)

    async def follow_range(self, request: FollowRangeInput) -> FollowOutput:
        """Answer again the items of an earlier follow that named a floor holder."""
        return _from_json(await self._call("follow_range", request), FollowOutput)

    async def _call(self, operation: str, request: Any) -> bytes:
        codec = innermost_codec(self._client.data_converter.payload_codec)
        payload = Payload(
            metadata={"encoding": b"json/plain"},
            data=json.dumps(_transfer(request)).encode(),
        )
        if codec is not None:
            [payload] = await codec.encode([payload])
        response = await self._client.workflow_service.execute_sync_nexus_operation(
            ExecuteSyncNexusOperationRequest(
                namespace=self._client.namespace,
                identity=self._client.identity,
                request_id=str(uuid.uuid4()),
                endpoint=self._endpoint,
                service=_SERVICE,
                operation=operation,
                input=payload,
            ),
            timeout=self._timeout,
        )
        if response.HasField("failure"):
            raise _failed(response.failure, f"{self._endpoint}/{operation}")
        result = response.result
        if codec is not None:
            [result] = await codec.decode([result])
        encoding = result.metadata.get("encoding", b"")
        if not encoding.startswith(b"json/"):
            raise ValueError(
                f"{self._endpoint}/{operation} answered with a {encoding!r} payload, "
                "so the client's codec isn't the handler namespace's"
            )
        return result.data


class StreamServiceCallError(RuntimeError):
    """A call to the stream service failed with no stream error class named.

    .. warning::
        This API is experimental and may change in future versions.
    """

    def __init__(self, message: str, *, failure_type: str | None, retryable: bool):
        """Keep the handler's failure type and whether a repeat could answer otherwise."""
        super().__init__(message)
        self.failure_type = failure_type
        """The handler error or application failure type, if the failure had one."""
        self.retryable = retryable
        """Whether the same call could answer otherwise."""


class StreamDirectReads(SimplePlugin):
    """Lets a caller Workflow's :class:`StreamReader` read around its History.

    .. warning::
        This API is experimental and may change in future versions.

    Register it on the client, as ``Client.connect(..., plugins=[reads])``,
    and on a Replayer. It maps each endpoint name to the transport that
    reaches it, registers the Local Activity that reads, and wraps the
    codec of the client and of a Replayer, so a direct read's result turns
    back into its batch before Workflow code sees it. A replay that misses
    the batch cache reads the range again through the same transport, so
    the endpoint must stay reachable while a run can replay.

    It needs no stream store, since the stream lives in the owner's.
    """

    def __init__(self, transports: Mapping[str, StreamDirectTransport]) -> None:
        """Read each endpoint in ``transports`` through its transport."""
        super().__init__(
            "temporalio.contrib.streams.nexus.StreamDirectReads",
            data_converter=self._wrap,
        )
        self._transports = dict(transports)
        self._batches = _BatchCache()
        self._follows: OrderedDict[bytes, bytes] = OrderedDict()
        self._read_states = ReadStates()

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Add the Local Activity a direct reader reads in.

        Raises:
            ValueError: The plugin is not registered on the Worker's client.
        """
        config = super().configure_worker(config)
        client = config["client"]  # type:ignore[reportTypedDictNotRequiredAccess]
        if self not in client.config()["plugins"]:
            raise ValueError(
                "StreamDirectReads must be registered on the Worker's client, as "
                "Client.connect(..., plugins=[reads]), so the client's codec "
                "turns direct reads back into batches"
            )
        activities = list(config.get("activities") or [])
        # The same plugin on the client and the Worker configures it twice.
        for builtin in (direct_read_activity, direct_follow_activity):
            if builtin not in activities:
                activities.append(builtin)
        config["activities"] = activities
        return config

    def transport(self, endpoint: str) -> StreamDirectTransport:
        """The transport that reaches ``endpoint``.

        Raises:
            StreamUnsupportedError: No transport is configured for
                ``endpoint``. The message names the opt-out.
        """
        transport = self._transports.get(endpoint)
        if transport is None:
            raise StreamUnsupportedError(
                f"a stream read with replay_from_store=True reads directly from "
                f"endpoint {endpoint!r}, and StreamDirectReads has no transport for "
                f"it; add one, or {_OPT_OUT}"
            )
        return transport

    def _wrap(self, converter: DataConverter | None) -> DataConverter:
        converter = converter or DataConverter.default
        if _direct_codec_of(converter.payload_codec) is not None:
            return converter
        return dataclasses.replace(
            converter, payload_codec=_DirectReadCodec(self, converter.payload_codec)
        )


@temporalio.activity.defn(name=DIRECT_READ_ACTIVITY)
async def direct_read_activity(
    endpoint: str, reader_id: str, input: ReadInput, read_seq: int = 0
) -> RawValue:
    """Read one batch for a direct reader, holding its records for the run's replay.

    A :class:`StreamReader` with ``replay_from_store=True`` runs it as a Local
    Activity. :class:`StreamDirectReads` registers it on every Worker built
    from its client. ``read_seq`` counts the reader's reads, this one
    included. Past the first, the marker leaves out the read's input state,
    which the previous marker holds as its answer state.
    """
    try:
        return await _read(endpoint, reader_id, input, read_seq)
    except _FINAL_ERRORS as error:
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error
    except (HTTPStatusError, StreamServiceCallError) as error:
        if error.retryable:
            raise
        raise ApplicationError(
            str(error),
            type=error.failure_type or type(error).__name__,
            non_retryable=True,
        ) from error
    except RPCError as error:
        if error.status not in _FINAL_STATUSES:
            raise
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error


async def _read(
    endpoint: str, reader_id: str, input: ReadInput, read_seq: int
) -> RawValue:
    info = temporalio.activity.info()
    plugin = _plugin_of(temporalio.activity.client())
    holder = f"{info.workflow_namespace}/{info.workflow_run_id}/{reader_id}"
    answer = await plugin.transport(endpoint).read(
        dataclasses.replace(input, floor_holder=holder)
    )
    if answer.replay is None:
        raise StreamUnsupportedError(
            f"the stream service on endpoint {endpoint!r} answered no replay range, "
            f"so it predates direct reads; upgrade its Worker, or {_OPT_OUT}"
        )
    range = ReadRangeInput(
        stream=input.stream,
        chain_first_run_id=answer.replay.chain_first_run_id,
        after_token=answer.replay.after_token,
        to_token=answer.next_token,
        stored_records=answer.replay.stored_records,
        state=input.state,
        records_digest=answer.replay.records_digest,
        floor_holder=holder,
    )
    # The Worker turns the result back into this batch before Workflow code
    # sees it, from here while the run stays in this process.
    plugin._batches.put(range, answer)
    chain = None
    if read_seq > 0:
        chain = ReadChain(f"{info.workflow_id}/{reader_id}", read_seq)
        if read_seq > 1:
            range = dataclasses.replace(range, state=None)
    return RawValue(positions_payload(endpoint, range, answer, chain))


@temporalio.activity.defn(name=DIRECT_FOLLOW_ACTIVITY)
async def direct_follow_activity(
    endpoint: str, reader_id: str, input: FollowInput
) -> RawValue:
    """Follow one step for a direct reader, holding its records for the run's replay.

    A :class:`StreamReader` with ``follow_links=True`` and
    ``replay_from_store=True`` runs it as a Local Activity.
    :class:`StreamDirectReads` registers it on every Worker built from its
    client.
    """
    try:
        return await _follow(endpoint, reader_id, input)
    except _FINAL_ERRORS as error:
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error
    except (HTTPStatusError, StreamServiceCallError) as error:
        if error.retryable:
            raise
        raise ApplicationError(
            str(error),
            type=error.failure_type or type(error).__name__,
            non_retryable=True,
        ) from error
    except RPCError as error:
        if error.status not in _FINAL_STATUSES:
            raise
        raise ApplicationError(
            str(error), type=type(error).__name__, non_retryable=True
        ) from error


async def _follow(endpoint: str, reader_id: str, input: FollowInput) -> RawValue:
    info = temporalio.activity.info()
    plugin = _plugin_of(temporalio.activity.client())
    holder = f"{info.workflow_namespace}/{info.workflow_run_id}/{reader_id}"
    answer = await plugin.transport(endpoint).follow(
        dataclasses.replace(input, floor_holder=holder)
    )
    if answer.replay is None:
        raise StreamUnsupportedError(
            f"the stream service on endpoint {endpoint!r} answered no follow replay, "
            f"so it can't replay a follow; upgrade its Worker, or {_OPT_OUT}"
        )
    range = FollowRangeInput(
        stream=input.stream,
        state=input.state,
        ranges=answer.replay.ranges,
        follow_digest=answer.replay.follow_digest,
        max_depth=input.max_depth,
        max_open=input.max_open,
        floor_holder=holder,
    )
    # The Worker turns the result back into this batch before Workflow code
    # sees it, from here while the run stays in this process.
    _put_follow(plugin, range, answer)
    without_records = dataclasses.replace(answer, records=[])
    document = {"range": _transfer(range), "answer": _transfer(without_records)}
    return RawValue(
        Payload(
            metadata={
                "encoding": b"json/plain",
                "messageType": FOLLOW_POSITIONS_TYPE,
                _ENDPOINT_KEY: endpoint.encode(),
            },
            data=json.dumps(document, separators=(",", ":"), sort_keys=True).encode(),
        )
    )


def _follow_key(range: FollowRangeInput) -> bytes:
    return b"\0".join(
        [(range.floor_holder or "").encode(), range.state or b"", range.follow_digest]
    )


def _put_follow(
    plugin: StreamDirectReads, range: FollowRangeInput, answer: FollowOutput
) -> None:
    key = _follow_key(range)
    plugin._follows[key] = _CONTRACT.to_payload(answer).data
    plugin._follows.move_to_end(key)
    while len(plugin._follows) > _CACHE_SIZE:
        plugin._follows.popitem(last=False)


def follow_batch_payload(answer: FollowOutput) -> Payload:
    """The result a direct follow turns back into, with its items."""
    payload = _CONTRACT.to_payload(answer)
    payload.metadata["messageType"] = FOLLOW_BATCH_TYPE
    return payload


def follow_answer_of(payload: Payload) -> FollowOutput:
    """The answer a direct follow's turned-back result holds.

    Raises:
        StreamUnsupportedError: No :class:`StreamDirectReads` on this Worker
            or Replayer turned the result back.
    """
    if payload.metadata.get("messageType") == FOLLOW_POSITIONS_TYPE:
        raise StreamUnsupportedError(
            "a stream follow with replay_from_store=True recorded only its ranges, "
            "and only the StreamDirectReads plugin on the Worker's client or the "
            f"Replayer turns them back into items; add the plugin, or {_OPT_OUT}"
        )
    return _CONTRACT.from_payload(payload, FollowOutput)


def positions_payload(
    endpoint: str,
    range: ReadRangeInput,
    answer: ReadOutput,
    chain: ReadChain | None = None,
) -> Payload:
    """The result a direct read records: its range, the rest of its answer, its errors.

    The range already holds the answer's end and replay range, so only the
    answer's state and ``done`` go beside it. The cursor and text
    of each record that crossed as an error ride along, since a codec
    failure can be transient and a replay must not hand over a record that
    live code never saw. ``chain`` places the read among its reader's reads,
    so a replay can take a left-out input state from the read before.
    """
    document: dict[str, Any] = {"range": _transfer(range), "done": answer.done}
    if chain is not None:
        document["reader"] = chain.reader
        document["seq"] = chain.seq
    if answer.state:
        document["state"] = base64.b64encode(answer.state).decode()
    errors = {record.token: record.error for record in answer.records if record.error}
    if errors:
        document["errors"] = errors
    return Payload(
        metadata={
            "encoding": b"json/plain",
            "messageType": POSITIONS_TYPE,
            _ENDPOINT_KEY: endpoint.encode(),
        },
        data=json.dumps(document, separators=(",", ":"), sort_keys=True).encode(),
    )


def _answer_of(range: ReadRangeInput, document: Mapping[str, Any]) -> ReadOutput:
    """The answer a direct read's marker holds, with no record."""
    state = document.get("state")
    return ReadOutput(
        records=[],
        next_token=range.to_token,
        done=document["done"],
        state=base64.b64decode(state) if state is not None else None,
        replay=ReadOutputReplay(
            chain_first_run_id=range.chain_first_run_id,
            after_token=range.after_token,
            stored_records=range.stored_records,
            records_digest=range.records_digest,
        ),
    )


def batch_payload(answer: ReadOutput) -> Payload:
    """The result a direct read turns back into, with its records."""
    payload = _CONTRACT.to_payload(answer)
    payload.metadata["messageType"] = BATCH_TYPE
    return payload


def is_positions(payload: Payload) -> bool:
    """Whether ``payload`` is a direct read's result that no codec turned back."""
    return payload.metadata.get("messageType") == POSITIONS_TYPE


def answer_of(payload: Payload) -> ReadOutput:
    """The answer a direct read's turned-back result holds.

    Raises:
        StreamUnsupportedError: No :class:`StreamDirectReads` on this Worker
            or Replayer turned the result back.
    """
    if is_positions(payload):
        raise StreamUnsupportedError(
            "a stream read with replay_from_store=True recorded only its range, and "
            "only the StreamDirectReads plugin on the Worker's client or the "
            f"Replayer turns it back into records; add the plugin, or {_OPT_OUT}"
        )
    return _CONTRACT.from_payload(payload, ReadOutput)


class _BatchCache:
    """The newest batches direct reads read in this process."""

    def __init__(self, size: int = _CACHE_SIZE) -> None:
        self._size = size
        self._batches: OrderedDict[tuple[str, str, str, bytes], bytes] = OrderedDict()

    @staticmethod
    def _key(range: ReadRangeInput) -> tuple[str, str, str, bytes]:
        return (
            range.floor_holder or "",
            range.after_token,
            range.to_token,
            range.records_digest,
        )

    def put(self, range: ReadRangeInput, answer: ReadOutput) -> None:
        key = self._key(range)
        self._batches[key] = _CONTRACT.to_payload(answer).data
        self._batches.move_to_end(key)
        while len(self._batches) > self._size:
            self._batches.popitem(last=False)

    def take(self, range: ReadRangeInput) -> ReadOutput | None:
        # The batch stays cached for a later replay in this process.
        data = self._batches.get(self._key(range))
        if data is None:
            return None
        return _from_json(data, ReadOutput)

    def clear(self) -> None:
        self._batches.clear()


class _DirectReadCodec(PayloadCodec, WithSerializationContext):
    """The client's codec, with direct reads turned back into batches on decode."""

    def __init__(self, plugin: StreamDirectReads, inner: PayloadCodec | None) -> None:
        self._plugin = plugin
        self._inner = inner

    @property
    def inner(self) -> PayloadCodec | None:
        """The codec this one wraps."""
        return self._inner

    def with_context(self, context: SerializationContext) -> _DirectReadCodec:
        """Pass the context to the wrapped codec."""
        inner = self._inner
        if isinstance(inner, WithSerializationContext):
            inner = inner.with_context(context)
        return _DirectReadCodec(self._plugin, inner)

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Encode with the wrapped codec."""
        if self._inner is None:
            return list(payloads)
        return await self._inner.encode(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Decode with the wrapped codec, then turn direct reads back into batches.

        Raises:
            temporalio.workflow.NondeterminismError: The store holds other
                records in the range than the read found.
            StreamReplayRangeLostError: The store no longer holds the range.
                The Workflow Task fails and is retried.
            StreamUnsupportedError: No transport reaches the read's endpoint.
                The Workflow Task fails and is retried.
        """
        decoded = (
            list(payloads)
            if self._inner is None
            else await self._inner.decode(payloads)
        )
        out: list[Payload] = []
        for payload in decoded:
            if is_positions(payload):
                payload = await self._batch(payload)
            elif payload.metadata.get("messageType") == FOLLOW_POSITIONS_TYPE:
                payload = await self._follow_batch(payload)
            out.append(payload)
        return out

    async def _follow_batch(self, payload: Payload) -> Payload:
        document = json.loads(payload.data)
        range = _from_transfer(document["range"], FollowRangeInput)
        answer = _from_transfer(document["answer"], FollowOutput)
        cached = self._plugin._follows.get(_follow_key(range))
        if cached is not None:
            return follow_batch_payload(_from_json(cached, FollowOutput))
        endpoint = payload.metadata.get(_ENDPOINT_KEY, b"").decode()
        try:
            again = await self._plugin.transport(endpoint).follow_range(range)
        except StreamReplayMismatchError as error:
            raise temporalio.workflow.NondeterminismError(
                f"a direct stream follow from endpoint {endpoint!r} replayed other "
                f"items: {error}"
            ) from error
        # The live answer's state stays, since a replay's differs in clock fields.
        return follow_batch_payload(dataclasses.replace(answer, records=again.records))

    async def _batch(self, payload: Payload) -> Payload:
        document = json.loads(payload.data)
        range = _from_transfer(document["range"], ReadRangeInput)
        answer = _answer_of(range, document)
        errors: dict[str, str] = document.get("errors") or {}
        chain = (
            ReadChain(document["reader"], document["seq"])
            if "reader" in document
            else None
        )
        previous = (
            self._plugin._read_states.before(chain) if chain is not None else None
        )
        if chain is not None:
            # Kept for the next read's replay, whose marker leaves its input
            # state out.
            self._plugin._read_states.put(chain, answer.state or b"")
        if range.stored_records == 0:
            # A read with no stored record has no record at all, since a
            # SUPERSEDED record comes only before a stored one.
            return batch_payload(answer)
        cached = self._plugin._batches.take(range)
        if cached is not None:
            return batch_payload(cached)
        if chain is not None and chain.seq > 1:
            if previous is None:
                raise StreamError(
                    "a direct stream read replays from the answer state of the "
                    "reader's previous read, which this process has not decoded "
                    "yet; the Workflow Task fails and its retry replays the History "
                    "from the start"
                )
            range = dataclasses.replace(range, state=previous or None)
        endpoint = payload.metadata.get(_ENDPOINT_KEY, b"").decode()
        try:
            again = await self._plugin.transport(endpoint).read_range(range)
        except StreamReplayMismatchError as error:
            raise temporalio.workflow.NondeterminismError(
                f"a direct stream read from endpoint {endpoint!r} replayed other "
                f"records: {error}"
            ) from error
        records = _with_errors(again.records, errors)
        return batch_payload(dataclasses.replace(answer, records=records))


def _with_errors(
    records: list[RecordWire], errors: Mapping[str, str]
) -> list[RecordWire]:
    """``records`` with the live read's error marks put back at their cursors."""
    marked: list[RecordWire] = []
    for record in records:
        if record.token in errors:
            marked.append(
                RecordWire(
                    token=record.token,
                    record=b"",
                    error=errors[record.token],
                    stale=record.stale,
                )
            )
        elif record.error:
            # The live read handed this record over, so Workflow code saw it.
            # A replay that can't is a failure of the way, not of the Workflow.
            raise StreamStorageError(
                f"a direct stream read replayed an error at {record.token} where the "
                f"read handed over a record: {record.error}"
            )
        else:
            marked.append(record)
    return marked


def _named(error: HTTPStatusError) -> Exception:
    """The stream error the handler named, or ``error`` when it named none."""
    named = _NAMED_ERRORS.get(error.failure_type or "")
    if named is None:
        return error
    return named(error.failure_message or error.detail)


def _failed(failure: Failure, call: str) -> Exception:
    """The error a transport raises for the failure ``ExecuteSyncNexusOperation`` answered."""
    handler_type: str | None = None
    retryable: bool | None = None
    found: Failure | None = failure
    while found is not None:
        if found.HasField("application_failure_info"):
            named = _NAMED_ERRORS.get(found.application_failure_info.type)
            if named is not None:
                return named(found.message)
            if retryable is None and found.application_failure_info.non_retryable:
                retryable = False
            handler_type = handler_type or found.application_failure_info.type or None
        if found.HasField("nexus_handler_failure_info"):
            info = found.nexus_handler_failure_info
            handler_type = info.type
            behavior = info.retry_behavior
            if (
                behavior
                == NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
            ):
                retryable = True
            elif (
                behavior
                == NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
            ):
                retryable = False
            else:
                retryable = info.type not in _FINAL_HANDLER_ERRORS
        if found.HasField("canceled_failure_info"):
            retryable = False
        found = found.cause if found.HasField("cause") else None
    return StreamServiceCallError(
        f"{call} failed: {failure.message}",
        failure_type=handler_type,
        retryable=True if retryable is None else retryable,
    )


def _plugin_of(client: Client) -> StreamDirectReads:
    for plugin in client.config()["plugins"]:
        if isinstance(plugin, StreamDirectReads):
            return plugin
    raise StreamUnsupportedError(
        "a stream read with replay_from_store=True needs StreamDirectReads on the "
        f"Worker's client; add it, or {_OPT_OUT}"
    )


def _direct_codec_of(codec: PayloadCodec | None) -> _DirectReadCodec | None:
    while codec is not None:
        if isinstance(codec, _DirectReadCodec):
            return codec
        if not isinstance(codec, ReplayReadCodec):
            return None
        codec = codec.inner
    return None


def innermost_codec(codec: PayloadCodec | None) -> PayloadCodec | None:
    """``codec`` without the wrappers that turn stream reads back into batches."""
    while isinstance(codec, (_DirectReadCodec, ReplayReadCodec)):
        codec = codec.inner
    return codec


def _transfer(value: Any) -> Any:
    return json.loads(_CONTRACT.to_payload(value).data)


def _from_transfer(transfer: Any, as_type: type[Any]) -> Any:
    return _from_json(json.dumps(transfer).encode(), as_type)


def _from_json(data: bytes, as_type: type[Any]) -> Any:
    payload = Payload(metadata={"encoding": b"json/plain"}, data=data)
    return _CONTRACT.from_payload(payload, as_type)
