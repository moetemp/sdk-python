"""Streams in client code."""

from __future__ import annotations

import asyncio
import itertools
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from datetime import timedelta
from typing import Any, TypeVar, overload

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.client
from temporalio.contrib.streams import _errors, _ids
from temporalio.contrib.streams._access import Access, _AsyncTopic, read_spec
from temporalio.contrib.streams._backend import StreamBackend, client_backend
from temporalio.contrib.streams._defn import TopicDefinition, definition_of
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._items import End
from temporalio.contrib.streams._reader import Item
from temporalio.contrib.streams._writer import StreamWriter
from temporalio.contrib.streams.system import models

E = TypeVar("E")
T = TypeVar("T")

# A client's Producer id has a key per process and handle, so two handles never share a fence
# or a sequence. The crate takes two parts after `client:`, so the handle number goes into the
# key.
_PROCESS_KEY = str(uuid.uuid4())
_WRITERS = itertools.count(1)


def _client_producer(client: temporalio.client.Client) -> stream_pb.Producer:
    key = f"{_PROCESS_KEY}-{next(_WRITERS)}"
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


class ClientStream(StreamHandle[E]):
    """A Stream that a client writes, reads and closes. Get one from :py:class:`StreamClient`.

    Each publish writes before it returns. Use :py:class:`BufferedPublisher` to batch. Each
    handle writes as its own Producer.

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
                    backend=backend,
                    converter=client.data_converter,
                    namespace=client.namespace,
                    stream_id=self._stream_id,
                    producer=_client_producer(client),
                )
            return self._writer

        return Access(
            backend=backend,
            converter=client.data_converter,
            namespace=client.namespace,
            stream_id=self._stream_id,
            writer=writer,
        )

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
        wait_new_records: bool = True,
        prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
    ) -> AsyncGenerator[Item, None]:
        """Read the Stream's items in order.

        The read ends after a ``Closed`` item, or at the newest Record when
        ``wait_new_records`` is False. A reader that wants to stop after a quiet time sets its
        own timer, and resumes later with ``from_offset``. A ``Superseded`` item withdraws
        earlier Records of a failed attempt, and it passes every Topic filter.

        Args:
            topics: Read these Topics. With no Topics and no prefixes, read every Topic.
            from_offset: Start at this offset, inclusive, or at ``streams.END`` for new
                Records only. Defaults to the first Record.
            wait_new_records: Wait for new Records at the end of the Stream.
            prefixes: Also read the Topics whose names start with one of these.
            create_wait: How long to wait for a Stream that nobody created yet, such as the
                Stream of a Workflow that just started. Defaults to no wait.
        """
        names, types = read_spec(self, topics)
        return self._read(
            topics=names,
            prefixes=prefixes,
            types=types,
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )

    async def _read(self, **options: Any) -> AsyncGenerator[Item, None]:
        await self._ready()
        async for item in self._access().read(**options):
            yield item

    async def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one."""
        await self._ready()
        return await self._access().get_offset()

    async def close(
        self, *, result: Any | None = None, failure: BaseException | None = None
    ) -> None:
        """Write this handle's Records, then close the Stream.

        Readers get the Records before it, then a ``Closed`` item with ``result`` or
        ``failure``.
        """
        await self._ready()
        await self._access().close(result, failure)


class StreamClient:
    """Creates and gets Streams over a Temporal client with a Stream plugin.

    .. warning::
        This API is experimental.
    """

    def __init__(self, client: temporalio.client.Client) -> None:
        """Create a Stream client. It makes no call."""
        self._client = client

    async def create(
        self,
        definition: type[E],
        *,
        id: str,
        retention: timedelta | None = None,
        auto_close: timedelta | None = None,
        backend: str | None = None,
    ) -> ClientStream[E]:
        """Create a Stream that this client owns, so only a client closes it.

        It is idempotent for one id, one Retention and one ``auto_close``.

        Args:
            definition: The Stream definition class.
            id: The Stream id, as a Workflow takes a Workflow id.
            retention: How long the Records stay after the close. Defaults to the backend's.
            auto_close: Close the Stream with no result when no Record was appended for this
                long, so a Stream that nobody closes does not stay open. Defaults to never.
            backend: The name of the backend. Defaults to the client's first Stream plugin.

        Raises:
            StreamConfigError: The client has no backend of that name. No call is made.
            StreamClosedError: A Stream of that id was created and closed before.
            StreamError: The Stream exists with another Retention or ``auto_close``, with
                the reason ``stream_already_exists``.
        """
        definition_of(definition)
        backend_name, stream_backend = client_backend(self._client, backend)
        response = await stream_backend.service(self._client.namespace).create_stream(
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
        name: str | None = None,
        backend: str | tuple[str, ...] | None = None,
    ) -> ClientStream[E]:
        """A Stream that a Workflow created, by the Workflow handle and the definition.

        It builds the id from the Workflow id and the first run id, so it makes no call. A
        handle with no first run id makes one Describe call on the Stream's first use.

        Args:
            handle: A handle of the Workflow, best the one that ``start_workflow`` returned.
            definition: The Stream definition class.
            name: The Stream name that the Workflow gave. Defaults to the definition's name.
            backend: The name of the backend, or a tuple of names to look in, as for
                :py:meth:`get_handle`. Defaults to the client's default backend.
        """
        stream_name = name or definition_of(definition).name
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
            description = await handle.describe()
            first = description.raw_description.workflow_execution_info.first_run_id
            if not first:
                raise ValueError(
                    "The server gave no first run id, so the Stream id is unknown"
                )
            return first

        stream._stream_name = stream_name
        return stream._bind(self._client, stream_backend, find_first_run, probe)
