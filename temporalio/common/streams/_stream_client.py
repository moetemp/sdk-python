"""Streams in client code."""

from __future__ import annotations

import asyncio
import itertools
import uuid
import weakref
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from datetime import timedelta
from types import TracebackType
from typing import Any, TypeVar, cast, overload

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
from temporalio.common.streams import _backend, _errors, _ids
from temporalio.common.streams._access import (
    Access,
    ReadSpec,
    _AsyncTopic,
    read_spec,
    subscription,
)
from temporalio.common.streams._backend import StreamBackend
from temporalio.common.streams._defn import TopicDefinition, definition_of
from temporalio.common.streams._handle import Creating, StreamHandle, exit_failure
from temporalio.common.streams._items import End
from temporalio.common.streams._plugin import client_backend
from temporalio.common.streams._reader import Item, Position, Subscription
from temporalio.common.streams._writer import StreamWriter
from temporalio.common.streams.system import models

E = TypeVar("E")
T = TypeVar("T")

# The handles of one client and Stream write as one Producer, so the crate shares one writer
# between them. Each client gets its own key, since two clients may have two runtime objects,
# and two writers of one Producer would send clashing sequences.
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


class ClientTopic(_AsyncTopic[T]):
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

    def subscribe(
        self,
        *,
        from_offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Subscription[T]:
        """Read this Topic only. Its Records have the Topic's type. See
        :py:meth:`ClientStream.subscribe`.
        """
        stream = self._stream
        assert isinstance(stream, ClientStream)
        return cast(
            Subscription[T],
            stream.subscribe(
                self,
                from_offset=from_offset,
                last=last,
                wait_new_records=wait_new_records,
                create_wait=create_wait,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ),
        )


class ClientStream(StreamHandle[E]):
    """A Stream that a client writes, reads and closes. Get one from :py:class:`StreamClient`.

    Each publish writes before it returns. Use :py:class:`BufferedPublisher` to batch. The
    handles of one client and Stream write as one Producer, and :py:meth:`producer` gives a
    handle that writes as a named one. Use it in ``async with`` to close it at the end of the
    block.

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
    ) -> None:
        """Create a handle. Use :py:class:`StreamClient` instead."""
        super().__init__(
            id=id,
            workflow_id=workflow_id,
            first_execution_run_id=first_execution_run_id,
            backend=backend,
            definition=definition,
        )
        self._client: temporalio.client.Client | None = None
        self._stream_backend: StreamBackend | None = None
        self._find_first_run: Callable[[], Awaitable[str]] | None = None
        self._writer: StreamWriter | None = None
        # The Stream name of a Workflow's Stream whose first run id is not known yet.
        self._stream_name = ""
        self._finding: asyncio.Future[str] | None = None
        # The backends to look in, in order, until one has the Stream.
        self._probe: list[tuple[str, StreamBackend]] = []
        self._closed = False
        # The name and the epoch of a named Producer.
        self._named: tuple[str, int] | None = None

    def _bind(
        self,
        client: temporalio.client.Client,
        backend: StreamBackend,
        find_first_run: Callable[[], Awaitable[str]] | None = None,
        probe: list[tuple[str, StreamBackend]] | None = None,
    ) -> ClientStream[E]:
        self._client = client
        self._stream_backend = backend
        self._find_first_run = find_first_run
        self._probe = probe or []
        return self

    @property
    def backend(self) -> str | None:
        """Name of the backend. A handle that looks in many backends knows it after a call."""
        return None if self._probe else self._backend

    async def _ready(self) -> None:
        # The first backend that has the Stream keeps it. Two first calls may both look.
        for name, backend in self._probe:
            assert self._client is not None
            request = models.DescribeStreamRequest(
                stream=models.StreamRef(id=await self._stream_id())
            )
            try:
                await backend.service(self._client.namespace).describe_stream(request)
            except _errors.StreamNotFoundError:
                continue
            self._backend, self._stream_backend, self._probe = name, backend, []
            return
        if self._probe:
            raise _errors.StreamNotFoundError(
                f"No backend of {[name for name, _ in self._probe]} has the Stream {self._id}",
                reason="stream_not_found",
            )

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
            # Calls that start together share one Describe call.
            if self._finding is None:
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

    def _access(self) -> Access:
        client, backend = self._client, self._stream_backend
        if client is None or backend is None:
            raise _errors.StreamConfigError(
                "This ClientStream came from a payload, so it has no client. "
                "Use StreamClient.get_handle."
            )

        def writer() -> StreamWriter:
            if self._writer is None:
                self._writer = StreamWriter(
                    writers=backend.writers(client.namespace, client.data_converter),
                    stream_id=self._stream_id,
                    producer=_client_producer(client, self._named),
                    free=backend.free_writer,
                )
            return self._writer

        return Access(
            backend=backend,
            converter=client.data_converter,
            namespace=client.namespace,
            stream_id=self._stream_id,
            writer=writer,
        )

    def producer(self, name: str, epoch: int) -> ClientStream[E]:
        """This Stream as a handle that writes as the Producer ``name`` at ``epoch``.

        It is for a writer that must fence its earlier self, such as a server that restarts.
        Keep ``epoch`` in durable storage, and raise it at each start. After a write at a
        higher epoch, a write at a lower one raises :py:class:`StreamFencedError`. The Records
        of the earlier epochs stay, since they are real messages. Run one live writer for a
        name.

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
        )
        handle._stream_name = self._stream_name
        handle._named = (name, epoch)
        if self._client is not None and self._stream_backend is not None:
            handle._bind(
                self._client, self._stream_backend, self._find_first_run, self._probe
            )
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
                A name of the definition takes its type.
            type: The type of a Topic outside the definition, or of a Stream with no
                definition.
        """
        name, topic_type = self._topic_spec(topic, type)
        return ClientTopic(self, name, topic_type)

    def subscribe(
        self,
        *topics: str | TopicDefinition[Any] | ClientTopic[Any],
        from_offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
        type: Any = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Subscription[Any]:
        """Read the Stream's items in order.

        The read ends after a ``Closed`` item, or at the newest Record when
        ``wait_new_records`` is False. A reader that wants to stop after a quiet time sets its
        own timer, and resumes later with ``from_offset=`` the read's ``next_offset``. A
        ``Superseded`` item withdraws earlier Records of a failed attempt, and it passes every
        Topic filter.

        Args:
            topics: Read these Topics. With no Topics and no prefixes, read every Topic.
            from_offset: Start at this offset, inclusive, or at ``streams.END`` for new
                Records only. Defaults to the first Record.
            last: Start with the newest this many Records that pass the filter, oldest
                first, such as the first view of a chat. Not with ``from_offset``. A Record
                that a later ``Superseded`` item withdraws doesn't count. At most 1000.
            wait_new_records: Wait for new Records at the end of the Stream.
            prefixes: Also read the Topics whose names start with one of these.
            create_wait: How long to wait for a Stream that nobody created yet, such as the
                Stream of a Workflow that just started. Defaults to no wait.
            type: The type of the values of Topics outside the definition, such as Topics that
                ``prefixes`` match. Topics of the definition keep their own types. Defaults to
                no type, so the values decode as the converter gives them.
            rpc_metadata: Headers of each call of the read. See :py:meth:`get_offset`.
            rpc_timeout: The deadline of each call of the read, not of the whole read. A call
                that waits for new Records answers before it.
        """
        read = read_spec(
            self,
            topics,
            prefixes=prefixes,
            default_type=type,
            from_offset=from_offset,
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
        await self._ready()
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
        """The next offset of the Stream, the newest offset plus one.

        Args:
            rpc_metadata: Headers of each call. The Redis and memory backends make no RPC, so
                they send none.
            rpc_timeout: The deadline of each call. A call past it raises ``RPCError`` with
                ``DEADLINE_EXCEEDED``.
        """
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            await self._ready()
            return await self._access().get_offset()

    async def close(
        self,
        *,
        result: Any | None = None,
        failure: BaseException | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Write this handle's Records, then close the Stream.

        Readers get the Records before it, then a ``Closed`` item with ``result`` or
        ``failure``. A ``CancelledError`` failure closes it as canceled. ``rpc_metadata`` and
        ``rpc_timeout`` are as for :py:meth:`get_offset`.

        Raises:
            StreamNotOwnerError: A Workflow owns the Stream, so only its run chain closes it.
        """
        if self._workflow_id:
            # A Workflow's Stream closes only from its run chain. The Store refuses a client's
            # close too, and this error comes with no call.
            raise _errors.StreamNotOwnerError(
                f"Can't close the Stream {self._id or self._stream_name} from a client, since "
                "a Workflow owns it",
                reason="stream_not_owner",
            )
        self._closed = True
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            await self._ready()
            await self._access().close(result, failure)

    async def __aenter__(self) -> ClientStream[E]:
        """Use the Stream in ``async with``, which closes it at the end."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream, with the exception as its failure if there is one.

        A cancel closes it with a ``CancelledError`` failure. Another ``BaseException``, such as
        ``KeyboardInterrupt``, leaves it open. A Stream that this handle closed stays as it is.
        """
        if self._closed:
            return
        close, failure = exit_failure(exc)
        if close:
            await self.close(failure=failure)


class StreamClient:
    """Creates and gets Streams over a Temporal client with a Stream plugin.

    .. warning::
        This API is experimental.
    """

    def __init__(self, client: temporalio.client.Client) -> None:
        """Create a Stream client. It makes no call."""
        self._client = client

    def create(
        self,
        definition: type[E],
        *,
        id: str,
        retention: timedelta | None = None,
        auto_close: timedelta | None = None,
        backend: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Creating[ClientStream[E]]:
        """Create a Stream that this client owns, so only a client closes it.

        It is idempotent for one id, one Retention and one ``auto_close``. Await the call for
        the handle, or use it in ``async with``, which closes the Stream at the end of the
        block.

        Args:
            definition: The Stream definition class.
            id: The Stream id, as a Workflow takes a Workflow id.
            retention: How long the Records stay after the close. Defaults to the backend's.
            auto_close: Close the Stream with no result when no Record was appended for this
                long, so a Stream that nobody closes does not stay open. Defaults to never.
            backend: The name of the backend. Defaults to the client's default backend.
            rpc_metadata: Headers of the call. See :py:meth:`ClientStream.get_offset`.
            rpc_timeout: The deadline of the call.

        Raises:
            StreamConfigError: The client has no backend of that name. No call is made.
            StreamClosedError: A Stream of that id was created and closed before.
            StreamError: The Stream exists with another Retention or ``auto_close``, with
                the reason ``stream_already_exists``.
        """

        async def create() -> ClientStream[E]:
            definition_of(definition)
            backend_name, stream_backend = client_backend(self._client, backend)
            service = stream_backend.service(self._client.namespace)
            with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
                response = await service.create_stream(
                    models.CreateStreamRequest(
                        stream_id=id, retention=retention, auto_close=auto_close
                    )
                )
            if response.status == stream_pb.StreamStatus.STREAM_STATUS_CLOSED:
                raise _errors.StreamClosedError(
                    f"The Stream {id} was created and closed before",
                    reason="stream_closed",
                )
            stream = ClientStream(
                id=id, backend=backend_name or None, definition=definition
            )
            return stream._bind(self._client, stream_backend)

        return Creating(create)

    def get_handle(
        self,
        definition: type[E],
        *,
        id: str | None = None,
        stream: StreamHandle[Any] | None = None,
        backend: str | tuple[str, ...] | None = None,
    ) -> ClientStream[E]:
        """A Stream by its id, or a handle that came as data, such as in a Record. No call.

        Args:
            definition: The Stream definition class.
            id: The Stream id.
            stream: A handle of the Stream, such as one in a Record. Its id, owner and
                backend stay.
            backend: The name of the backend. Defaults to the client's default backend. A
                tuple, such as ``("native", "redis")`` during a move, looks in each in order
                on first use and keeps the first that has the Stream.
        """
        definition_of(definition)
        if stream is not None:
            # An empty name is of any backend, so it needs a client with one.
            backend_name, stream_backend = client_backend(
                self._client, stream.backend, only=True
            )
            probe: list[tuple[str, StreamBackend]] = []
        elif id is not None:
            backend_name, stream_backend, probe = self._backends(backend)
        else:
            raise TypeError("get_handle takes id= or stream=")
        handle = ClientStream(
            id=stream.id if stream else id or "",
            workflow_id=stream.workflow_id if stream else None,
            first_execution_run_id=stream.first_execution_run_id if stream else None,
            backend=backend_name or None,
            definition=definition,
        )
        return handle._bind(self._client, stream_backend, probe=probe)

    def _backends(
        self, backend: str | tuple[str, ...] | None
    ) -> tuple[str, StreamBackend, list[tuple[str, StreamBackend]]]:
        if not isinstance(backend, tuple):
            return (*client_backend(self._client, backend), [])
        probe = [client_backend(self._client, name) for name in backend]
        return (*probe[0], probe if len(probe) > 1 else [])

    def workflow_stream(
        self,
        handle: temporalio.client.WorkflowHandle[Any, Any],
        definition: type[E],
        *,
        backend: str | tuple[str, ...] | None = None,
    ) -> ClientStream[E]:
        """A Stream that a Workflow created, by the Workflow handle and the definition.

        It builds the id from the Workflow id and the first run id, so it makes no call. A
        handle with no first run id makes one Describe call on the Stream's first use.

        Args:
            handle: A handle of the Workflow, best the one that ``start_workflow`` returned.
            definition: The Stream definition class, renamed as the Workflow's create.
            backend: The name of the backend, or a tuple of names to look in, as for
                :py:meth:`get_handle`. Defaults to the client's default backend.
        """
        stream_name = definition_of(definition).name
        backend_name, stream_backend, probe = self._backends(backend)
        first_run = handle.first_execution_run_id
        stream = ClientStream(
            id=_ids.scoped_stream_id(handle.id, first_run, stream_name)
            if first_run
            else "",
            workflow_id=handle.id,
            first_execution_run_id=first_run,
            backend=backend_name or None,
            definition=definition,
        )
        if first_run:
            return stream._bind(self._client, stream_backend, probe=probe)

        async def find_first_run() -> str:
            # The first call that needs the id gives its own call options.
            options = _backend.call_options.get()
            description = await handle.describe(
                rpc_metadata=options.metadata if options else {},
                rpc_timeout=options.timeout if options else None,
            )
            first = description.raw_description.workflow_execution_info.first_run_id
            if not first:
                raise ValueError(
                    "The server gave no first run id, so the Stream id is unknown"
                )
            return first

        stream._stream_name = stream_name
        return stream._bind(self._client, stream_backend, find_first_run, probe)
