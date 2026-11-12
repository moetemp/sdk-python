"""Streams outside Workflow code."""

from __future__ import annotations

import abc
import asyncio
import itertools
import os
import typing
import uuid
import weakref
from collections.abc import AsyncGenerator, Mapping
from datetime import timedelta
from typing import Any, TypeVar

import temporalio.api.stream.v1 as stream_v1
import temporalio.client
from temporalio.client._streams import Stream
from temporalio.streams import _activity, _backend, _ids
from temporalio.streams._access import Access, ReadSpec, reader
from temporalio.streams._backend import BackendCalls
from temporalio.streams._conflict import StreamIDConflictPolicy
from temporalio.streams._offset import StreamOffset
from temporalio.streams._plugin import ref_backend, select_backend
from temporalio.streams._reader import Item, Position, Reader
from temporalio.streams._ref import StreamRef
from temporalio.streams._writer import RecordWriter
from temporalio.streams.system import models

T = TypeVar("T")

# The Streams of one client and Stream id write as one Producer. Each client gets its own key,
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


def _client_producer(client: temporalio.client.Client) -> stream_v1.Producer:
    key = _CLIENT_KEYS.get(client)
    if key is None:
        key = f"{_process_key}-{next(_clients)}"
        _CLIENT_KEYS[client] = key
    return stream_v1.Producer(
        id=_ids.client_producer_id(client.identity, key),
        client=stream_v1.ClientProducer(),
    )


class StreamCalls(abc.ABC):
    """The calls of one :py:class:`Stream`, such as in client code."""

    @abc.abstractmethod
    def append(
        self,
        values: tuple[Any, ...],
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> asyncio.Future[StreamOffset]:
        """Start a write. See :py:meth:`Stream.append`."""

    @abc.abstractmethod
    def read(
        self,
        read: ReadSpec,
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> Reader[Any]:
        """Start a read. See :py:meth:`Stream.read`."""

    @abc.abstractmethod
    async def get_offset(
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> StreamOffset:
        """Get the newest offset. See :py:meth:`Stream.get_offset`."""

    @abc.abstractmethod
    async def close(
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> None:
        """Close the Stream. See :py:meth:`Stream.close`."""

    @abc.abstractmethod
    async def enter(self) -> None:
        """Start an ``async with`` block of the Stream."""

    @abc.abstractmethod
    async def exit(self, exc: BaseException | None) -> None:
        """End an ``async with`` block of the Stream. See :py:meth:`Stream.__aexit__`."""


class ClientCalls(StreamCalls):
    """The calls of one Stream in client code.

    Each append starts its write at the call. All Streams of one client and Stream id write as
    one Producer.
    """

    def __init__(
        self,
        client: temporalio.client.Client,
        backend: BackendCalls,
        ref: StreamRef[Any],
    ) -> None:
        """Create the calls of ``ref``."""
        self._client = client
        self._backend = backend
        self._ref = ref
        self._writer: RecordWriter | None = None
        self._closed = False

    def _access(self) -> Access:
        client = self._client
        backend = self._backend
        stream_id = self._ref.id

        def writer() -> RecordWriter:
            if self._writer is None:
                self._writer = RecordWriter(
                    writers=backend.writers(client.namespace, client.data_converter),
                    stream_id=stream_id,
                    producer=_client_producer(client),
                )
            return self._writer

        return Access(
            backend=backend,
            converter=client.data_converter,
            namespace=client.namespace,
            stream_id=stream_id,
            writer=writer,
        )

    def _raise_lost_write(self) -> None:
        # Client code has no end, such as a run's, to raise a write error that no caller got. So
        # the Stream's next call raises it.
        if self._writer is None:
            return
        error = self._writer.unretrieved_error()
        if error is not None:
            raise error

    def append(
        self,
        values: tuple[Any, ...],
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> asyncio.Future[StreamOffset]:
        """Start a write. The write's task takes the call options."""
        self._raise_lost_write()
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return self._access().append(values)

    def read(
        self,
        read: ReadSpec,
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> Reader[Any]:
        """Start a read."""
        self._raise_lost_write()
        return reader(
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
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> StreamOffset:
        """Get the newest offset."""
        self._raise_lost_write()
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return StreamOffset._from_wire(await self._access().get_offset())

    async def close(
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> None:
        """Write the Records of these calls, then close the Stream."""
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            await self._access().close()
        # Only a close that the Store took counts, so the end of async with tries a failed one
        # again.
        self._closed = True
        # The close runs first, so a lost write doesn't leave the Stream open.
        self._raise_lost_write()

    async def enter(self) -> None:
        """Enter the block that closes the Stream at its end."""

    async def exit(self, exc: BaseException | None) -> None:
        """Close the Stream, unless these calls closed it already."""
        if not self._closed:
            await self.close({}, None)


async def create(
    client: temporalio.client.Client,
    payload_type: type[T] | None,
    *,
    id: str,
    id_conflict_policy: StreamIDConflictPolicy,
    rpc_metadata: Mapping[str, str | bytes],
    rpc_timeout: timedelta | None,
) -> Stream[T]:
    """Create a Stream.

    See :py:meth:`temporalio.client.Client.create_stream`.
    """
    if not id:
        # An empty id would fail only at the Store, with an unclear error.
        raise ValueError("The Stream id is empty")
    backend_name, calls = select_backend(client, id)
    # Delegate to the generated client. It sends a new request id for each call. A closed,
    # expired or another owner's Stream fails the create.
    await calls.service(client.namespace).create_stream(
        id=id,
        backend=backend_name,
        id_conflict_policy=models.StreamIdConflictPolicy(int(id_conflict_policy)),
        body_type=object,
        rpc_metadata=rpc_metadata,
        rpc_timeout=rpc_timeout,
    )
    ref: StreamRef[T] = StreamRef(id=id, backend=backend_name)
    return _stream(client, calls, ref, payload_type)


def get_stream(
    client: temporalio.client.Client,
    payload_type: type[T] | None,
    *,
    id: str,
) -> Stream[T]:
    """Get a Stream by its id.

    See :py:meth:`temporalio.client.Client.get_stream`.
    """
    if not id:
        # An empty id would fail only at the first call, with an unclear error.
        raise ValueError("The Stream id is empty")
    backend_name, calls = select_backend(client, id)
    ref: StreamRef[T] = StreamRef(id=id, backend=backend_name)
    return _stream(client, calls, ref, payload_type)


def from_ref(
    client: temporalio.client.Client,
    ref: StreamRef[T],
) -> Stream[T]:
    """The Stream of a ref that came as data.

    See :py:meth:`temporalio.client.Client.get_stream`.
    """
    _, calls = ref_backend(client, ref.id, ref.backend)
    # A ref that code built as StreamRef[T] carries the payload type, so reads decode with it.
    arguments = typing.get_args(getattr(ref, "__orig_class__", None))
    payload_type = arguments[0] if arguments else None
    return _stream(client, calls, ref, payload_type)


def _stream(
    client: temporalio.client.Client,
    backend: BackendCalls,
    ref: StreamRef[T],
    payload_type: Any,
) -> Stream[T]:
    streams = _activity.current.get()
    if streams is not None and streams.client is client:
        # The Activity module imports this one.
        from temporalio.streams._activity_stream import ActivityCalls

        # A Stream of the Worker's client writes as the attempt, so a reader can tell a retry's
        # Records from those of the failed attempt.
        return Stream(ref, payload_type, ActivityCalls(streams, ref))
    return Stream(ref, payload_type, ClientCalls(client, backend, ref))
