"""Streams in client code."""

from __future__ import annotations

import asyncio
import functools
import itertools
import uuid
import weakref
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import Any, TypeVar, overload

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
from temporalio.common._streams import _backend, _errors, _ids
from temporalio.common._streams._access import (
    Access,
    AsyncTopicBase,
    ReadSpec,
    read_spec,
    subscription,
)
from temporalio.common._streams._backend import BackendCalls
from temporalio.common._streams._defn import TopicDefinition, definition_of
from temporalio.common._streams._handle import Creating, StreamHandle, exit_failure
from temporalio.common._streams._items import End
from temporalio.common._streams._plugin import backend_of, client_backend
from temporalio.common._streams._reader import Item, Position, Subscription
from temporalio.common._streams._writer import StreamWriter
from temporalio.common._streams.system import models

E = TypeVar("E")
T = TypeVar("T")

# The handles of one client and Stream write as one Producer. Each client gets its own key,
# since two clients can have two runtime objects, whose writers would send clashing sequences.
_PROCESS_KEY = str(uuid.uuid4())
_CLIENTS = itertools.count(1)
_CLIENT_KEYS: weakref.WeakKeyDictionary[temporalio.client.Client, str] = (
    weakref.WeakKeyDictionary()
)


def _client_producer(
    client: temporalio.client.Client, named: tuple[str, int] | None
) -> stream_pb.Producer:
    if named is not None:
        name, epoch = named
        return stream_pb.Producer(
            kind=stream_pb.ProducerKind.PRODUCER_KIND_CLIENT,
            id=_ids.named_client_producer_id(name),
            epoch=stream_pb.ProducerEpoch(attempt=epoch),
        )
    key = _CLIENT_KEYS.get(client)
    if key is None:
        key = f"{_PROCESS_KEY}-{next(_CLIENTS)}"
        _CLIENT_KEYS[client] = key
    return stream_pb.Producer(
        kind=stream_pb.ProducerKind.PRODUCER_KIND_CLIENT,
        id=_ids.client_producer_id(client.identity, key),
        epoch=stream_pb.ProducerEpoch(attempt=1),
    )


class ClientTopic(AsyncTopicBase[T]):
    """One Topic of a :py:class:`ClientStream`. All Topics of a Stream share one order.

    .. warning::
        This API is experimental.
    """

    async def publish(
        self,
        *values: T,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value. ``rpc_metadata`` and ``rpc_timeout`` are as for
        :py:meth:`ClientStream.get_offset`.
        """
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return await super().publish(*values)


class ClientStream(StreamHandle[E]):
    """A Stream in client code. The client can write and read it, and close a client's Stream.

    Get one from :py:meth:`temporalio.client.Client.stream`,
    :py:meth:`temporalio.client.WorkflowHandle.stream`,
    :py:meth:`temporalio.client.Client.create_stream`, or a backend's ``create`` function, such
    as :py:func:`temporalio.contrib.stream.create`.

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
        definition: type[E] | None = None,
        activity_id: str | None = None,
        activity_run_id: str | None = None,
    ) -> None:
        """Create a handle. Use :py:meth:`temporalio.client.Client.stream` instead."""
        super().__init__(
            id=id,
            workflow_id=workflow_id,
            first_execution_run_id=first_execution_run_id,
            backend=backend,
            definition=definition,
            activity_id=activity_id,
            activity_run_id=activity_run_id,
        )
        self._client: temporalio.client.Client | None = None
        self._stream_backend: BackendCalls | None = None
        self._find_first_run: Callable[[], Awaitable[str]] | None = None
        self._writer: StreamWriter | None = None
        # The Stream name of a Workflow's Stream whose first run id is not known yet.
        self._stream_name = ""
        self._finding: asyncio.Future[str] | None = None
        self._closed = False
        # The name and the epoch of a named Producer.
        self._named: tuple[str, int] | None = None

    def _bind(
        self,
        client: temporalio.client.Client,
        backend: BackendCalls,
        find_first_run: Callable[[], Awaitable[str]] | None = None,
    ) -> ClientStream[E]:
        self._client = client
        self._stream_backend = backend
        self._find_first_run = find_first_run
        return self

    @property
    def id(self) -> str:
        """The Stream id.

        Raises:
            RuntimeError: The Workflow handle had no first run id, and no call has found it
                yet. Call :py:meth:`get_offset` first.
        """
        if not self._id:
            raise RuntimeError(
                "The Stream id is not known yet. Call get_offset() first."
            )
        return self._id

    async def _stream_id(self) -> str:
        if not self._id:
            # Concurrent calls share one Describe call. A failed Describe is discarded, so a
            # later call tries again.
            if self._finding is None or _failed(self._finding):
                assert self._find_first_run is not None
                self._finding = asyncio.ensure_future(self._find_first_run())
            first_run = await asyncio.shield(self._finding)
            if not self._id:
                assert self._workflow_id is not None
                self._first_execution_run_id = first_run
                self._id = _ids.scoped_stream_id(
                    self._workflow_id, first_run, self._stream_name
                )
        return self._id

    def _fields(self) -> dict[str, str]:
        fields = super()._fields()
        if not self._id:
            # The Stream name tells apart the Streams of one Workflow before the id is known.
            fields["name"] = self._stream_name
        return fields

    async def _resolved_ref(self) -> models.StreamRef:
        # The first call fetches the first run id of a Workflow's Stream, which the ref includes.
        await self._stream_id()
        return self._owner_ref()

    def _access(self) -> Access:
        client, backend = self._client, self._stream_backend
        if client is None or backend is None:
            raise _errors.StreamConfigError(
                "This ClientStream came from a payload, so it has no client. "
                "Use Client.stream(definition, stream=handle)."
            )

        def writer() -> StreamWriter:
            if self._writer is None:
                self._writer = StreamWriter(
                    writers=backend.writers(client.namespace, client.data_converter),
                    ref=self._resolved_ref,
                    producer=_client_producer(client, self._named),
                    free=backend.free_writer,
                )
            return self._writer

        return Access(
            backend=backend,
            converter=client.data_converter,
            namespace=client.namespace,
            ref=self._resolved_ref,
            writer=writer,
        )

    def producer(self, name: str, epoch: int) -> ClientStream[E]:
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
        handle = ClientStream(
            id=self._id,
            workflow_id=self._workflow_id,
            first_execution_run_id=self._first_execution_run_id,
            backend=self._backend,
            definition=self._definition,
            activity_id=self._activity_id,
            activity_run_id=self._activity_run_id,
        )
        handle._stream_name = self._stream_name
        handle._named = (name, epoch)
        if self._client is not None and self._stream_backend is not None:
            handle._bind(self._client, self._stream_backend, self._find_first_run)
        return handle

    @overload
    def topic(self, topic: TopicDefinition[T]) -> ClientTopic[T]: ...

    @overload
    def topic(self, topic: str) -> ClientTopic[Any]: ...

    @overload
    def topic(self, topic: str, *, type: type[T]) -> ClientTopic[T]: ...

    def topic(
        self, topic: str | TopicDefinition[Any], *, type: Any = None
    ) -> ClientTopic[Any]:
        """A Topic of this Stream.

        Args:
            topic: A Topic of the Stream's definition, such as ``Events.turns``, or a name.
                A name in the definition gets the definition's type.
            type: The type of a Topic outside the definition, or of a Stream with no
                definition.
        """
        name, topic_type = self._topic_spec(topic, type)
        return ClientTopic(self, name, topic_type)

    def subscribe(
        self,
        *topics: str | TopicDefinition[Any] | ClientTopic[Any],
        offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        topic_prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
        type: Any = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Subscription[Any]:
        """Read the Stream's items in order.

        The read ends after a ``Closed`` item, or at the newest Record when
        ``wait_new_records`` is False. To stop after a quiet period, use your own timer, and
        resume later with ``offset=`` set to the read's ``next_offset``. A ``Superseded`` item
        withdraws earlier Records of a failed attempt. Topic filters never skip it.

        Args:
            topics: Read these Topics, by Topic of the definition, name or Topic object.
                With no Topics and no ``topic_prefixes``, read every Topic.
            offset: Start at this offset, inclusive, or at ``common.END`` for new
                Records only. Defaults to the first Record.
            last: Start with the newest ``last`` Records that pass the filter, oldest first,
                such as for the first view of a chat. Can't be used with ``offset``. A Record
                that a later ``Superseded`` item withdraws doesn't count. At most 1000.
            wait_new_records: Wait for new Records at the end of the Stream.
            topic_prefixes: Also read the Topics whose names start with one of these.
            create_wait: How long to wait for a Stream that doesn't exist yet, such as the
                Stream of a Workflow that just started. Defaults to no wait.
            type: The type of the values of Topics outside the definition, such as Topics that
                ``topic_prefixes`` match. Topics of the definition keep their own types.
                Defaults to no type, so the converter decodes the values with no type hint.
            rpc_metadata: Headers of each call of the read. See :py:meth:`get_offset`.
            rpc_timeout: The deadline of each call of the read, not of the whole read. A call
                that waits for new Records returns before the deadline.
        """
        read = read_spec(
            self,
            topics,
            topic_prefixes=topic_prefixes,
            default_type=type,
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
        if self._workflow_id or self._activity_id:
            # Only the run chain can close a Workflow's Stream, and only a Standalone Activity can
            # close its own Stream. The Store refuses a client's close too.
            raise _errors.StreamNotOwnerError(
                f"Can't close the Stream {self._id or self._stream_name} from a client, since "
                f"{'a Workflow' if self._workflow_id else 'an Activity'} owns it",
                reason="stream_not_owner",
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
            StreamNotOwnerError: A Workflow or a Standalone Activity owns the Stream, so only
                its run chain or that Activity can close it.
        """
        self._check_close()
        self._closed = True
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            await self._access().close(result, failure)

    async def __aenter__(self) -> ClientStream[E]:
        """Use the Stream in ``async with``, which closes it at the end.

        Raises:
            StreamNotOwnerError: A Workflow or a Standalone Activity owns the Stream, so only
                its run chain or that Activity can close it.
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
    definition: type[E],
    *,
    id: str,
    retention: timedelta | None,
    backend: str | None,
) -> Creating[ClientStream[E]]:
    """Create a Stream that the client owns.

    See :py:meth:`temporalio.client.Client.create_stream`.
    """

    async def create() -> ClientStream[E]:
        definition_of(definition)
        backend_name, calls = client_backend(client, backend)
        response = await calls.service(client.namespace).create_stream(
            models.CreateStreamRequest(stream_id=id, retention=retention)
        )
        if response.status == stream_pb.StreamStatus.STREAM_STATUS_CLOSED:
            raise _errors.StreamClosedError(
                f"The Stream {id} was created and closed before",
                reason="stream_closed",
            )
        if response.status == stream_pb.StreamStatus.STREAM_STATUS_EXPIRED:
            raise _errors.StreamExpiredError(
                f"The Stream {id} expired", reason="stream_expired"
            )
        stream = ClientStream(id=id, backend=backend_name, definition=definition)
        return stream._bind(client, calls)

    return Creating(create)


def client_stream(
    client: temporalio.client.Client,
    definition: type[E],
    *,
    id: str | None,
    stream: StreamHandle[Any] | None,
    backend: str | None,
) -> ClientStream[E]:
    """Get a Stream by its id, or from a handle that came in data.

    See :py:meth:`temporalio.client.Client.stream`.
    """
    definition_of(definition)
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
    handle = ClientStream(
        id=stream_id,
        workflow_id=stream.workflow_id if stream else None,
        first_execution_run_id=stream.first_execution_run_id if stream else None,
        backend=backend_name,
        definition=definition,
        activity_id=stream.activity_id if stream else None,
        activity_run_id=stream.activity_run_id if stream else None,
    )
    return handle._bind(client, calls)


# The first run id of a Workflow handle, or the Describe call that fetches it, so the Streams
# of one handle share the call.
_FIRST_RUNS: weakref.WeakKeyDictionary[
    temporalio.client.WorkflowHandle[Any, Any], asyncio.Future[str] | str
] = weakref.WeakKeyDictionary()


def _failed(future: asyncio.Future[Any]) -> bool:
    return future.done() and (future.cancelled() or future.exception() is not None)


async def _describe_first_run(
    handle: temporalio.client.WorkflowHandle[Any, Any],
) -> str:
    # The Describe uses the call options of the first call that needs the id.
    options = _backend.call_options.get()
    description = await handle.describe(
        rpc_metadata=options.metadata if options else {},
        rpc_timeout=options.timeout if options else None,
    )
    first = description.raw_description.workflow_execution_info.first_run_id
    if not first:
        raise ValueError("The server gave no first run id, so the Stream id is unknown")
    return first


async def _first_run(handle: temporalio.client.WorkflowHandle[Any, Any]) -> str:
    known = _FIRST_RUNS.get(handle)
    if isinstance(known, str):
        return known
    if known is None:
        known = asyncio.ensure_future(_describe_first_run(handle))
        _FIRST_RUNS[handle] = known
        known.add_done_callback(functools.partial(_settle, weakref.ref(handle)))
    return await asyncio.shield(known)


def _settle(
    handle_ref: weakref.ref[temporalio.client.WorkflowHandle[Any, Any]],
    finding: asyncio.Future[str],
) -> None:
    # Store a found id as text. Remove a failed call, so a later call tries again and the
    # failure's traceback doesn't keep the handle alive.
    handle = handle_ref()
    if handle is None or _FIRST_RUNS.get(handle) is not finding:
        return
    if _failed(finding):
        del _FIRST_RUNS[handle]
    else:
        _FIRST_RUNS[handle] = finding.result()


def workflow_stream(
    handle: temporalio.client.WorkflowHandle[Any, Any],
    definition: type[E],
    *,
    backend: str | None,
) -> ClientStream[E]:
    """Get a Stream that a Workflow created.

    See :py:meth:`temporalio.client.WorkflowHandle.stream`.
    """
    stream_name = definition_of(definition).name
    client = handle._client
    backend_name, calls = client_backend(client, backend)
    first_run = handle.first_execution_run_id
    stream = ClientStream(
        id=_ids.scoped_stream_id(handle.id, first_run, stream_name)
        if first_run
        else "",
        workflow_id=handle.id,
        first_execution_run_id=first_run,
        backend=backend_name,
        definition=definition,
    )
    if first_run:
        return stream._bind(client, calls)
    stream._stream_name = stream_name
    return stream._bind(client, calls, lambda: _first_run(handle))


def activity_stream(
    handle: temporalio.client.ActivityHandle[Any],
    definition: type[E],
    *,
    backend: str | None,
) -> ClientStream[E]:
    """Get a Stream that a Standalone Activity created.

    See :py:meth:`temporalio.client.ActivityHandle.stream`.
    """
    stream_name = definition_of(definition).name
    client = handle._client
    # Only a native backend stores the Stream, so the default rule doesn't apply.
    stream_backend = backend_of(client, backend or _ids.NATIVE_BACKEND)
    if not stream_backend._activity_streams:
        raise _errors.StreamConfigError(
            f"The backend {stream_backend.backend_name} can't store the Stream of a Standalone "
            "Activity. Use the native backend."
        )
    if not handle.run_id:
        raise ValueError(
            "The Activity handle has no run id, so the Stream id is unknown. Use the "
            "handle that start_activity returns."
        )
    stream = ClientStream(
        id=_ids.scoped_stream_id(handle.id, handle.run_id, stream_name),
        backend=stream_backend.backend_name,
        definition=definition,
        activity_id=handle.id,
        activity_run_id=handle.run_id,
    )
    return stream._bind(client, stream_backend._backend_for(client))
