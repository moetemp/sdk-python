"""Streams outside Workflow code.

This module imports the Streams API of :py:mod:`temporalio.streams` on first use, so a client
with no Stream never imports it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import Self, Unpack

if TYPE_CHECKING:
    import temporalio.api.stream.v1
    import temporalio.client
    from temporalio.streams import End, StreamOptions, StreamRef, Subscription
    from temporalio.streams._stream_client import StreamCalls

T = TypeVar("T")
_U = TypeVar("_U")


class Stream(Generic[T]):
    """A Stream outside Workflow code, with its calls. ``T`` is the payload type.

    Get one with :py:attr:`temporalio.client.Client.create_stream`,
    :py:attr:`temporalio.client.Client.create_or_get_stream`,
    :py:attr:`temporalio.client.Client.get_stream`, or
    :py:meth:`temporalio.client.Client.to_stream` for a ref that came as data. Its
    :py:attr:`ref` is the data to pass on, such as to a Workflow.

    All Streams of one client and Stream id write as one Producer, and :py:meth:`producer`
    returns a Stream that writes as a Producer with a name of its own. A write error that no
    caller got raises at the next call of the Stream.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self, ref: StreamRef[T], payload_type: Any, calls: StreamCalls
    ) -> None:
        """Create a Stream. Use a client's ``create_stream``, ``get_stream`` or ``to_stream``."""
        self._ref = ref
        # The type that a read decodes each value with. None decodes with no type hint.
        self._payload_type = payload_type
        self._calls = calls

    @property
    def ref(self) -> StreamRef[T]:
        """The data that identifies the Stream, to pass on."""
        return self._ref

    @property
    def proto_ref(self) -> temporalio.api.stream.v1.StreamRef:
        """The ``StreamRef`` wire message of the Stream, for a field of a proto message."""
        from temporalio.streams import _ref

        return _ref.to_proto(self._ref)

    def publish(
        self,
        *values: T,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> asyncio.Future[int]:
        """Start the write of one Record for each value, in order, and return its future.

        The write has its place in the Stream's order when the call returns, also when no
        caller awaits the future. The writes of one Stream keep the order of the calls. A cancel
        of the future doesn't stop the write. A list is one value. The next call of the Stream
        raises a write error that no caller got.

        Args:
            values: One value for each Record.
            rpc_metadata: Headers of each call. The Redis and memory backends make no RPC, so
                they ignore it.
            rpc_timeout: The deadline of each call, on every backend. A call past it raises
                ``RPCError`` with ``DEADLINE_EXCEEDED``.

        Returns:
            The future of the write. It gives the offset of the last Record, or raises
            ``StreamError`` when the backend refused the write, such as
            ``StreamNotFoundError`` for a Stream that nobody created.

        Raises:
            StreamClosedError: This object closed the Stream.
        """
        return self._calls.publish(values, rpc_metadata, rpc_timeout)

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

        The read ends when the Stream is closed and the read gave its last item, or at the
        newest Record when ``wait_new_records`` is False. To stop after a quiet period, use your
        own timer, and resume later with ``offset=`` set to the read's ``next_offset``. A
        ``Superseded`` item withdraws earlier Records of a failed attempt.

        Args:
            offset: Start at this offset, inclusive, or at ``streams.END`` for new Records
                only. Defaults to the first Record.
            last: Start with the newest ``last`` Records, oldest first, such as for the first
                view of a chat. Can't be used with ``offset``. A Record that a later
                ``Superseded`` item withdraws doesn't count. At most 1000.
            wait_new_records: Wait for new Records at the end of the Stream.
            create_wait: How long to wait for a Stream that doesn't exist yet, such as one
                that a Workflow creates after it starts. Defaults to no wait.
            rpc_metadata: Headers of each call of the read. The Redis and memory backends
                ignore it.
            rpc_timeout: The deadline of each call of the read, on every backend, and not of the
                whole read. A call that waits for new Records returns before the deadline.
        """
        from temporalio.streams._access import read_spec

        read = read_spec(
            self._payload_type,
            offset=offset,
            last=last,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        return self._calls.subscribe(read, rpc_metadata, rpc_timeout)

    async def get_offset(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> int:
        """The next offset of the Stream, the newest offset plus one, or 0 if it has no Record.

        Offsets are sparse and above zero, so ``offset=0`` reads from the start.

        Args:
            rpc_metadata: Headers of the call. The Redis and memory backends ignore it.
            rpc_timeout: The deadline of the call, on every backend.
        """
        return await self._calls.get_offset(rpc_metadata, rpc_timeout)

    async def close(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Write the Records that this object published, then close the Stream.

        A close carries no value, so code with a last value publishes it first. Readers get the
        Records before the close, then their read ends. Later writes fail with
        ``StreamClosedError``.

        Args:
            rpc_metadata: Headers of each call. The Redis and memory backends ignore it.
            rpc_timeout: The deadline of each call, on every backend.

        Raises:
            StreamNotOwnerError: The code can't close the Stream. Only the run chain closes a
                Stream bound to a Workflow, and it raises with no call.
        """
        await self._calls.close(rpc_metadata, rpc_timeout)

    async def __aenter__(self) -> Self:
        """Use the Stream in ``async with``, which closes it at the end.

        Raises:
            StreamNotOwnerError: The code can't close the Stream. See :py:meth:`close`.
        """
        await self._calls.enter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream, also when the block raised any exception or was canceled.

        If this object already closed the Stream, this does nothing.
        """
        await self._calls.exit(exc)

    def producer(self, name: str, epoch: int) -> Stream[T]:
        """Return this Stream as a Stream that writes as the Producer ``name`` at ``epoch``.

        Use it for a writer that must fence its earlier instances, such as a server that
        restarts. Keep ``epoch`` in durable storage, and raise it at each start. After a write
        at a higher epoch, a write at a lower one raises
        :py:class:`temporalio.streams.StreamFencedError`. The Records of the earlier epochs
        aren't withdrawn, since they're real messages. Run only one live writer for each name.

        Args:
            name: The Producer name, the same across restarts.
            epoch: The writer's epoch, 1 or more.

        Raises:
            ValueError: The epoch is below 1.
        """
        return Stream(self._ref, self._payload_type, self._calls.producer(name, epoch))

    def __repr__(self) -> str:
        """Show the ref."""
        return f"{type(self).__name__}({self._ref!r})"


# Python has no type arguments on a call, so a call that takes the payload type is an object
# whose subscript binds the type, as in client.get_stream[Turn]("chat-42"). The subscript passes
# the class on, so a read decodes each Record with it, and type checkers see Stream[Turn]. With
# no subscript, T is Any and a read gives JSON values.


class CreateStream(Generic[T]):
    """The call of :py:attr:`temporalio.client.Client.create_stream` and
    :py:attr:`temporalio.client.Client.create_or_get_stream`.

    Subscript it with the payload type, as in ``client.create_stream[Turn]("chat-42")``.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        client: temporalio.client.Client,
        payload_type: type[T] | None,
        *,
        or_get: bool,
    ) -> None:
        """Create the call. Use the client's property instead."""
        self._client = client
        self._payload_type = payload_type
        self._or_get = or_get

    def __getitem__(self, payload_type: type[_U]) -> CreateStream[_U]:
        """The call for Streams of ``payload_type``.

        It takes a class, such as ``Turn`` or ``list[str]``. Type checkers refuse a union, such
        as ``str | None``, so use ``Any`` or a class that holds the value instead.
        """
        return CreateStream(self._client, payload_type, or_get=self._or_get)

    async def __call__(
        self,
        id: str,
        *,
        backend: str | None = None,
        **options: Unpack[StreamOptions],
    ) -> Stream[T]:
        """Create the Stream, and return it.

        Args:
            id: The Stream id, which the caller picks, as for a Workflow ID.
            backend: The name of the backend. Defaults to the registered backend with
                ``default=True``.
            options: The options of a new Stream, such as ``retention``. See
                :py:class:`temporalio.streams.StreamOptions`.

        Raises:
            StreamAlreadyExistsError: ``create_stream`` of an id that exists.
            StreamClosedError: A Stream of that id closed.
            StreamExpiredError: A Stream of that id expired.
            StreamConfigError: The client has no backend with that name, or no backend has
                ``default=True`` and the call gives no backend. No call is made.
        """
        from temporalio.streams import _stream_client

        return await _stream_client.create(
            self._client,
            self._payload_type,
            id=id,
            backend=backend,
            options=options,
            or_get=self._or_get,
        )


class GetStream(Generic[T]):
    """The call of :py:attr:`temporalio.client.Client.get_stream`.

    Subscript it with the payload type, as in ``client.get_stream[Turn]("chat-42")``.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self, client: temporalio.client.Client, payload_type: type[T] | None
    ) -> None:
        """Create the call. Use the client's property instead."""
        self._client = client
        self._payload_type = payload_type

    def __getitem__(self, payload_type: type[_U]) -> GetStream[_U]:
        """The call for Streams of ``payload_type``. It takes a class, as for ``create_stream``."""
        return GetStream(self._client, payload_type)

    def __call__(self, id: str, *, backend: str | None = None) -> Stream[T]:
        """Get the Stream with no call.

        Args:
            id: The Stream id.
            backend: The name of the backend. Defaults to the registered backend with
                ``default=True``.

        Raises:
            StreamConfigError: The client has no backend with that name, or no backend has
                ``default=True`` and the call gives no backend.
            ValueError: The id is empty.
        """
        from temporalio.streams import _stream_client

        return _stream_client.get_stream(
            self._client, self._payload_type, id=id, backend=backend
        )
