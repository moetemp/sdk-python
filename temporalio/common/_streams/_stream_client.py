"""Streams in client code."""

from __future__ import annotations

import contextvars
import itertools
import os
import uuid
import weakref
from collections.abc import AsyncGenerator, Mapping
from datetime import timedelta
from types import TracebackType
from typing import Any, TypeVar

import temporalio.api.stream.v1 as stream_v1
import temporalio.client
from temporalio.api.enums.v1 import StreamErrorReason, StreamProducerKind, StreamStatus
from temporalio.common._streams import _backend, _errors, _ids
from temporalio.common._streams._access import Access, ReadSpec, read_spec, subscription
from temporalio.common._streams._backend import BackendCalls
from temporalio.common._streams._defn import StreamDefinition, definition_of
from temporalio.common._streams._handle import (
    Creating,
    StreamHandle,
    StreamOptions,
    exit_failure,
)
from temporalio.common._streams._items import End
from temporalio.common._streams._plugin import client_backend
from temporalio.common._streams._reader import Item, Position, Subscription
from temporalio.common._streams._writer import StreamWriter

T = TypeVar("T")

# The handles of one client and Stream write as one Producer. Each client gets its own key,
# since two clients can have two runtime objects, whose writers would send clashing sequences.
_process_key = str(uuid.uuid4())
_clients = itertools.count(1)
_CLIENT_KEYS: weakref.WeakKeyDictionary[temporalio.client.Client, str] = (
    weakref.WeakKeyDictionary()
)


def _new_process_key() -> None:
    # A forked child has the parent's key and count, so two children would give their clients
    # the same Producer ids.
    global _process_key, _clients
    _process_key = str(uuid.uuid4())
    _clients = itertools.count(1)
    _CLIENT_KEYS.clear()


# Windows has no fork.
_register_at_fork = getattr(os, "register_at_fork", None)
if _register_at_fork is not None:
    _register_at_fork(after_in_child=_new_process_key)

# The client whose query result the converter decodes. A ClientStream in the result takes it, so
# a reader uses a Stream that a Workflow hands out with no other call.
_decoding_client: contextvars.ContextVar[temporalio.client.Client | None] = (
    contextvars.ContextVar("__temporal_stream_decoding_client", default=None)
)


def _client_producer(
    client: temporalio.client.Client, named: tuple[str, int] | None
) -> stream_v1.Producer:
    if named is not None:
        name, epoch = named
        return stream_v1.Producer(
            kind=StreamProducerKind.STREAM_PRODUCER_KIND_CLIENT,
            id=_ids.named_client_producer_id(name),
            epoch=stream_v1.ProducerEpoch(attempt=epoch),
        )
    key = _CLIENT_KEYS.get(client)
    if key is None:
        key = f"{_process_key}-{next(_clients)}"
        _CLIENT_KEYS[client] = key
    return stream_v1.Producer(
        kind=StreamProducerKind.STREAM_PRODUCER_KIND_CLIENT,
        id=_ids.client_producer_id(client.identity, key),
        epoch=stream_v1.ProducerEpoch(attempt=1),
    )


class ClientStream(StreamHandle[T]):
    """A Stream in client code. The client can write and read it, and close a client's Stream.

    Get one from :py:meth:`temporalio.client.Client.create_stream` or
    :py:meth:`temporalio.client.Client.stream`. A ``ClientStream`` that a query of a client
    returns, such as a Workflow's ``stream`` query, uses that client.

    Each publish writes before it returns. Use :py:class:`BufferedPublisher` to batch. All
    handles of one client and Stream write as one Producer. :py:meth:`producer` returns a
    handle that writes as a named Producer. Use a handle in ``async with`` to close the Stream
    at the end of the block.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        *,
        id: str,
        workflow_id: str | None = None,
        first_execution_run_id: str | None = None,
        backend: str | None = None,
        payload_type: Any = None,
    ) -> None:
        """Create a handle. Use :py:meth:`temporalio.client.Client.stream` instead."""
        super().__init__(
            id=id,
            workflow_id=workflow_id,
            first_execution_run_id=first_execution_run_id,
            backend=backend,
            payload_type=payload_type,
        )
        self._client = _decoding_client.get()
        self._stream_backend: BackendCalls | None = None
        self._writer: StreamWriter | None = None
        self._closed = False
        # The name and the epoch of a named Producer.
        self._named: tuple[str, int] | None = None

    def _bind(
        self, client: temporalio.client.Client, backend: BackendCalls
    ) -> ClientStream[T]:
        self._client = client
        self._stream_backend = backend
        return self

    def _access(self) -> Access:
        client = self._client
        if client is None:
            raise _errors.StreamConfigError(
                "This ClientStream came from a payload that no client query decoded, so it "
                "has no client. Use Client.stream(definition, stream=handle)."
            )
        if self._stream_backend is None:
            _, self._stream_backend = client_backend(client, self._backend)
        backend = self._stream_backend

        def writer() -> StreamWriter:
            if self._writer is None:
                self._writer = StreamWriter(
                    writers=backend.writers(client.namespace, client.data_converter),
                    stream_id=self._id,
                    producer=_client_producer(client, self._named),
                    free=backend.free_writer,
                )
            return self._writer

        return Access(
            backend=backend,
            converter=client.data_converter,
            namespace=client.namespace,
            stream_id=self._id,
            writer=writer,
        )

    def producer(self, name: str, epoch: int) -> ClientStream[T]:
        """Return this Stream as a handle that writes as the Producer ``name`` at ``epoch``.

        Use it for a writer that must fence its earlier instances, such as a server that
        restarts. Keep ``epoch`` in durable storage, and raise it at each start. After a write
        at a higher epoch, a write at a lower one raises :py:class:`StreamFencedError`. The
        Records of the earlier epochs aren't withdrawn, since they're real messages. Run only
        one live writer for each name.

        Args:
            name: The Producer name, the same across restarts.
            epoch: The writer's epoch, 1 or more.
        """
        if epoch < 1:
            raise ValueError("A Producer's epoch is 1 or more")
        handle: ClientStream[T] = ClientStream(
            id=self._id,
            workflow_id=self._workflow_id,
            first_execution_run_id=self._first_execution_run_id,
            backend=self._backend,
            payload_type=self._payload_type,
        )
        handle._named = (name, epoch)
        handle._client = self._client
        handle._stream_backend = self._stream_backend
        return handle

    async def publish(
        self,
        *values: T,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value. ``rpc_metadata`` and ``rpc_timeout`` are as for
        :py:meth:`get_offset`.

        Raises:
            StreamNotFoundError: Nobody created the Stream.
        """
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return await self._access().publish(values)

    def subscribe(
        self,
        *,
        offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Subscription[T]:
        """Read the Stream's items in order.

        The read ends after a ``Closed`` item, or at the newest Record when
        ``wait_new_records`` is False. To stop after a quiet period, use your own timer, and
        resume later with ``offset=`` set to the read's ``next_offset``. A ``Superseded`` item
        withdraws earlier Records of a failed attempt.

        Args:
            offset: Start at this offset, inclusive, or at ``common.END`` for new
                Records only. Defaults to the first Record.
            last: Start with the newest ``last`` Records, oldest first, such as for the first
                view of a chat. Can't be used with ``offset``. A Record that a later
                ``Superseded`` item withdraws doesn't count. At most 1000.
            wait_new_records: Wait for new Records at the end of the Stream.
            create_wait: How long to wait for a Stream that doesn't exist yet, such as the
                Stream of a Workflow that just started. Defaults to no wait.
            rpc_metadata: Headers of each call of the read. See :py:meth:`get_offset`.
            rpc_timeout: The deadline of each call of the read, not of the whole read. A call
                that waits for new Records returns before the deadline.
        """
        read = read_spec(
            self._payload_type,
            offset=offset,
            last=last,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        return subscription(
            lambda position: self._items(read, position),
            read,
            _backend.options_of(rpc_metadata, rpc_timeout),
        )

    async def _items(
        self, read: ReadSpec, position: Position
    ) -> AsyncGenerator[Item, None]:
        items = self._access().items(read, position)
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def get_offset(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> int:
        """The next offset of the Stream, the newest offset plus one, or 0 if it has no Record.

        Offsets are sparse and above zero, so ``offset=0`` reads from the start.

        Args:
            rpc_metadata: Headers of each call. The Redis and memory backends make no RPC, so
                they send none.
            rpc_timeout: The deadline of each call. A call past it raises ``RPCError`` with
                ``DEADLINE_EXCEEDED``.
        """
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return await self._access().get_offset()

    def _check_close(self) -> None:
        if self._workflow_id:
            # Only the run chain can close a Workflow's Stream. The Store refuses a client's
            # close too, but this check makes no call.
            raise _errors.StreamNotOwnerError(
                f"Can't close the Stream {self._id} from a client, since a Workflow owns it",
                reason=StreamErrorReason.STREAM_ERROR_REASON_NOT_OWNER,
            )

    async def close(
        self,
        *,
        result: Any | None = None,
        failure: BaseException | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Write this handle's Records, then close the Stream.

        Readers get the Records before the close, then a ``Closed`` item with ``result`` or
        ``failure``. A cancel failure, such as a ``CancelledError``, closes the Stream as
        canceled. ``rpc_metadata`` and ``rpc_timeout`` are as for :py:meth:`get_offset`.

        Raises:
            StreamNotOwnerError: A Workflow owns the Stream, so only its run chain can close it.
        """
        self._check_close()
        self._closed = True
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            await self._access().close(result, failure)

    async def __aenter__(self) -> ClientStream[T]:
        """Use the Stream in ``async with``, which closes it at the end.

        Raises:
            StreamNotOwnerError: A Workflow owns the Stream, so only its run chain can close it.
        """
        # Fail early, since the close at the end would raise and hide the block's own error.
        self._check_close()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream, with the exception as its failure if there is one.

        A cancel closes it with a ``CancelledError`` failure. Another ``BaseException``, such as
        ``KeyboardInterrupt``, leaves it open. If this handle already closed the Stream, this
        does nothing.
        """
        if self._closed:
            return
        close, failure = exit_failure(exc)
        if close:
            await self.close(failure=failure)


def create(
    client: temporalio.client.Client,
    definition: type[StreamDefinition[T]],
    *,
    id: str,
    backend: str | None,
    options: StreamOptions,
) -> Creating[ClientStream[T]]:
    """Create a Stream that the client owns, or get the one that exists.

    See :py:meth:`temporalio.client.Client.create_stream`.
    """

    async def create() -> ClientStream[T]:
        info = definition_of(definition)
        backend_name, calls = client_backend(client, backend)
        created = await calls.service(client.namespace).create_stream(
            stream_id=id, retention=options.get("retention")
        )
        if created.status == StreamStatus.STREAM_STATUS_CLOSED:
            raise _errors.StreamClosedError(
                f"The Stream {id} was created and closed before",
                reason=StreamErrorReason.STREAM_ERROR_REASON_CLOSED,
            )
        if created.status == StreamStatus.STREAM_STATUS_EXPIRED:
            raise _errors.StreamExpiredError(
                f"The Stream {id} expired",
                reason=StreamErrorReason.STREAM_ERROR_REASON_EXPIRED,
            )
        stream: ClientStream[T] = ClientStream(
            id=id, backend=backend_name, payload_type=info.type
        )
        return stream._bind(client, calls)

    return Creating(create)


def client_stream(
    client: temporalio.client.Client,
    definition: type[StreamDefinition[T]],
    *,
    id: str | None,
    stream: StreamHandle[Any] | None,
    backend: str | None,
) -> ClientStream[T]:
    """Get a Stream by its id, or from a handle that came in data.

    See :py:meth:`temporalio.client.Client.stream`.
    """
    info = definition_of(definition)
    if (id is None) == (stream is None):
        raise TypeError("stream takes one of id= and stream=")
    if stream is not None:
        if backend and stream.backend and backend != stream.backend:
            raise ValueError(
                f"The handle is of the backend {stream.backend}, not {backend}"
            )
        backend = backend or stream.backend
    stream_id = stream.id if stream else id
    if not stream_id:
        # An empty id would fail only at the first call, with an unclear error.
        raise ValueError("The Stream id is empty")
    backend_name, calls = client_backend(client, backend)
    handle: ClientStream[T] = ClientStream(
        id=stream_id,
        workflow_id=stream.workflow_id if stream else None,
        first_execution_run_id=stream.first_execution_run_id if stream else None,
        backend=backend_name,
        payload_type=info.type,
    )
    return handle._bind(client, calls)


class BindQueryResults(temporalio.client.Interceptor):
    """Gives each ``ClientStream`` in a query result the client that made the query."""

    def intercept_client(
        self, next: temporalio.client.OutboundInterceptor
    ) -> temporalio.client.OutboundInterceptor:
        """Wrap the client's calls."""
        return _BindQueryResults(next)


class _BindQueryResults(temporalio.client.OutboundInterceptor):
    def __init__(self, next: temporalio.client.OutboundInterceptor) -> None:
        super().__init__(next)
        # The innermost interceptor is the client's own, and only it has no next.
        inner: Any = next
        while hasattr(inner, "next"):
            inner = inner.next
        self._client: temporalio.client.Client = inner._client

    async def query_workflow(self, input: temporalio.client.QueryWorkflowInput) -> Any:
        token = _decoding_client.set(self._client)
        try:
            return await super().query_workflow(input)
        finally:
            _decoding_client.reset(token)
