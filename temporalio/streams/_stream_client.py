"""Streams outside Workflow code."""

from __future__ import annotations

import abc
import asyncio
import itertools
import os
import uuid
import weakref
from collections.abc import AsyncGenerator, Mapping
from datetime import timedelta
from typing import Any, TypeVar

import temporalio.api.stream.v1 as stream_v1
import temporalio.client
from temporalio.client._streams import Stream
from temporalio.streams import _backend, _errors, _ids
from temporalio.streams._access import Access, ReadSpec, subscription
from temporalio.streams._backend import BackendCalls
from temporalio.streams._plugin import client_backend
from temporalio.streams._reader import Item, Position, Subscription
from temporalio.streams._ref import StreamOptions, StreamRef
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


def _client_epoch(attempt: int) -> stream_v1.ProducerEpoch:
    return stream_v1.ProducerEpoch(client=stream_v1.ClientEpoch(attempt=attempt))


def _client_producer(
    client: temporalio.client.Client, named: tuple[str, int] | None
) -> stream_v1.Producer:
    if named is not None:
        name, epoch = named
        return stream_v1.Producer(
            id=_ids.named_client_producer_id(name), epoch=_client_epoch(epoch)
        )
    key = _CLIENT_KEYS.get(client)
    if key is None:
        key = f"{_process_key}-{next(_clients)}"
        _CLIENT_KEYS[client] = key
    return stream_v1.Producer(
        id=_ids.client_producer_id(client.identity, key), epoch=_client_epoch(1)
    )


class StreamCalls(abc.ABC):
    """The calls of one :py:class:`Stream`, such as in client code."""

    @abc.abstractmethod
    def publish(
        self,
        values: tuple[Any, ...],
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> asyncio.Future[int]:
        """Start a write. See :py:meth:`Stream.publish`."""

    @abc.abstractmethod
    def subscribe(
        self,
        read: ReadSpec,
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> Subscription[Any]:
        """Start a read. See :py:meth:`Stream.subscribe`."""

    @abc.abstractmethod
    async def get_offset(
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> int:
        """Get the next offset. See :py:meth:`Stream.get_offset`."""

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

    @abc.abstractmethod
    def producer(self, name: str, epoch: int) -> StreamCalls:
        """The calls of a Producer with a name of its own. See :py:meth:`Stream.producer`."""


class ClientCalls(StreamCalls):
    """The calls of one Stream in client code.

    Each publish starts its write at the call. All Streams of one client and Stream id write as
    one Producer, unless a Stream is of a Producer with a name of its own.
    """

    def __init__(
        self,
        client: temporalio.client.Client,
        backend: BackendCalls,
        ref: StreamRef[Any],
        named: tuple[str, int] | None = None,
    ) -> None:
        """Create the calls of ``ref``. ``named`` is a Producer's name and epoch."""
        self._client = client
        self._backend = backend
        self._ref = ref
        self._named = named
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
                    producer=_client_producer(client, self._named),
                    free=backend.free_writer,
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
        error = self._writer.unretrieved_error() if self._writer else None
        if error is not None:
            raise error

    def publish(
        self,
        values: tuple[Any, ...],
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> asyncio.Future[int]:
        """Start a write. The write's task takes the call options."""
        self._raise_lost_write()
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return self._access().publish(values)

    def subscribe(
        self,
        read: ReadSpec,
        rpc_metadata: Mapping[str, str | bytes],
        rpc_timeout: timedelta | None,
    ) -> Subscription[Any]:
        """Start a read."""
        self._raise_lost_write()
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
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> int:
        """Get the next offset."""
        self._raise_lost_write()
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            return await self._access().get_offset()

    def _check_close(self) -> None:
        if self._ref.owner is not None:
            # Only the run chain closes a Stream bound to a Workflow. The Store refuses a
            # client's close too, but this check makes no call.
            raise _errors.StreamNotOwnerError(
                f"Can't close the Stream {self._ref.id} from a client, since it is bound to a "
                "Workflow"
            )

    async def close(
        self, rpc_metadata: Mapping[str, str | bytes], rpc_timeout: timedelta | None
    ) -> None:
        """Write the Records of these calls, then close the Stream."""
        self._check_close()
        with _backend.using(_backend.options_of(rpc_metadata, rpc_timeout)):
            await self._access().close()
        # Only a close that the Store took counts, so the end of async with tries a failed one
        # again.
        self._closed = True
        # The close runs first, so a lost write doesn't leave the Stream open.
        self._raise_lost_write()

    async def enter(self) -> None:
        """Check that the client can close the Stream at the end of the block."""
        # Fail early, since the close at the end would raise and hide the block's own error.
        self._check_close()

    async def exit(self, exc: BaseException | None) -> None:
        """Close the Stream, unless these calls closed it already."""
        if not self._closed:
            await self.close({}, None)

    def producer(self, name: str, epoch: int) -> StreamCalls:
        """The calls of the same Stream and client as a Producer with a name of its own."""
        if epoch < 1:
            raise ValueError("A Producer's epoch is 1 or more")
        return ClientCalls(self._client, self._backend, self._ref, (name, epoch))


async def create(
    client: temporalio.client.Client,
    payload_type: type[T] | None,
    *,
    id: str,
    backend: str | None,
    options: StreamOptions,
    or_get: bool,
) -> Stream[T]:
    """Create a Stream, or with ``or_get`` get the one that exists.

    See :py:attr:`temporalio.client.Client.create_stream`.
    """
    if not id:
        # An empty id would fail only at the Store, with an unclear error.
        raise ValueError("The Stream id is empty")
    backend_name, calls = client_backend(client, backend)
    policy = (
        models.StreamIdConflictPolicy.StreamIdConflictPolicyUseExisting
        if or_get
        else models.StreamIdConflictPolicy.StreamIdConflictPolicyFail
    )
    # The generated create sends a new request id for each call. A closed, expired or another
    # owner's Stream fails either create.
    await calls.service(client.namespace).create_stream[object](
        id=id,
        backend=backend_name,
        retention=options.get("retention"),
        id_conflict_policy=policy,
    )
    ref: StreamRef[T] = StreamRef(id=id, backend=backend_name)
    return _stream(client, calls, ref, payload_type)


def get_stream(
    client: temporalio.client.Client,
    payload_type: type[T] | None,
    *,
    id: str,
    backend: str | None,
) -> Stream[T]:
    """Get a Stream by its id.

    See :py:attr:`temporalio.client.Client.get_stream`.
    """
    if not id:
        # An empty id would fail only at the first call, with an unclear error.
        raise ValueError("The Stream id is empty")
    backend_name, calls = client_backend(client, backend)
    ref: StreamRef[T] = StreamRef(id=id, backend=backend_name)
    return _stream(client, calls, ref, payload_type)


def to_stream(
    client: temporalio.client.Client,
    ref: StreamRef[T],
    *,
    payload_type: type[T] | None,
) -> Stream[T]:
    """The Stream of a ref that came as data.

    See :py:meth:`temporalio.client.Client.to_stream`.
    """
    _, calls = client_backend(client, ref.backend, hint="the ref's backend field")
    return _stream(client, calls, ref, payload_type)


def _stream(
    client: temporalio.client.Client,
    backend: BackendCalls,
    ref: StreamRef[T],
    payload_type: Any,
) -> Stream[T]:
    return Stream(ref, payload_type, ClientCalls(client, backend, ref))
