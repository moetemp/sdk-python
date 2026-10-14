"""Reaching a stream from an Activity or from client code.

Every call goes to Core's stream service, which checks the owner, mints and
checks cursors, and synthesizes ``SUPERSEDED``. What stays here is what
needs the plaintext or the user's types: converting values, the two hashes
taken before the codec, and the codec itself.

An Activity reaches the stream of the Workflow that scheduled it, and writes
as itself: Core derives its producer id as
``<Activity id>@<scheduling run id>`` and its Temporal attempt is the
producer attempt, so a retry is reported to readers as ``SUPERSEDED``. The
run is part of the id because a stream outlives a run, and Activity ids
repeat across the runs of a chain.

Any process holding a client reaches a Workflow's stream by Workflow id and
writes with a producer id and attempt of its own.
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any, Generic, TypeVar, cast, overload

import temporalio.activity
from temporalio.bridge.proto.streams import (
    ActivityProducer,
    AppendRecord,
    AppendRequest,
    LatestRequest,
    NamedProducer,
    ReadRequest,
    StreamAddress,
    StreamOwnerKind,
)
from temporalio.client import Client
from temporalio.contrib.streams._body import batch_digest, content_hash, encode_bodies
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._errors import (
    StreamError,
    StreamNotFoundError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._plugin import (
    StreamStorePlugin,
    call,
    store_for_client,
)
from temporalio.contrib.streams._record import Cursor, RecordKind, StreamRecord
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import WireRecord, from_read, to_wire
from temporalio.converter import DataConverter, WorkflowSerializationContext
from temporalio.service import RPCError, RPCStatusCode

__all__ = [
    "ActivityStreamHandle",
    "StreamHandle",
    "StreamProducer",
    "activity_handle",
    "get_stream_handle",
]

T = TypeVar("T")

_READ_WAIT = timedelta(seconds=30)


class StreamProducer(Generic[T]):
    """Appends to one topic as one producer attempt.

    Every append is visible as soon as the store accepts it. Each record
    carries the producer id, the attempt and a sequence that starts at one
    for each attempt, which lets the store deduplicate a retry and lets a
    reader tell a retry from a new attempt.
    """

    def __init__(
        self,
        handle: StreamHandle,
        topic: str,
        producer_id: str,
        attempt: int,
        activity: ActivityProducer | None = None,
    ) -> None:
        """Prefer :meth:`StreamHandle.producer`."""
        self._handle = handle
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._activity = activity
        self._sequence = 1
        self._last = BEGINNING
        # A batch reads the sequence, then awaits the codec and Core, then
        # moves it on; calls one at a time keep each batch's sequences.
        self._lock = asyncio.Lock()

    @property
    def producer_id(self) -> str:
        """Who this producer writes as."""
        return self._producer_id

    @property
    def attempt(self) -> int:
        """The attempt this producer writes."""
        return self._attempt

    async def append(self, *values: T) -> Cursor:
        """Append ``values`` as one batch and return the cursor of its last record.

        The batch lands whole and in order. The producer moves its sequence
        past the batch only when the append succeeds, so a call that raised
        :class:`temporalio.contrib.streams.StreamOutcomeUnknownError` can be
        repeated with the same values: the store answers a repeat of the
        batch it holds last for this producer attempt with the original
        position and writes nothing.

        An empty call writes nothing and returns the cursor of this
        producer's last record, or ``BEGINNING`` when it has written none.

        Calls on one producer run one at a time, in the order they were
        made, so concurrent calls take consecutive sequences. A call that was
        cancelled may or may not have written its batch, and the producer
        cannot tell: start a new attempt (a new producer with a higher
        ``attempt``) rather than continuing this one.

        Raises:
            StreamProducerError: The sequence was already used with different
                content, or it is below the newest one the store holds for
                this producer attempt.
            StreamOutcomeUnknownError: The store may or may not have written
                the batch. Retry the same values on this producer.
            StreamRefusedError: The store refused the batch, for example
                because it is out of memory. Nothing was written.
            StreamClosedError: The stream refuses appends.
        """
        async with self._lock:
            if not values:
                return self._last
            return await self._write(RecordKind.DATA, values)

    async def finish(self) -> Cursor:
        """Write ``FINISH`` for this producer on this topic and return its cursor.

        Says this producer has nothing more to send. It does not say the
        work behind it succeeded, and it does not end anyone's read.
        """
        async with self._lock:
            return await self._write(RecordKind.FINISH, [None])

    async def _write(self, kind: RecordKind, values: Any) -> Cursor:
        converter = self._handle._converter
        wires = [
            to_wire(
                converter.payload_converter,
                topic=self._topic,
                kind=kind,
                value=value,
                producer_id=self._producer_id,
                attempt=self._attempt,
                sequence=self._sequence + index,
            )
            for index, value in enumerate(values)
        ]
        request = AppendRequest(
            stream=self._handle._address(self._topic),
            sequence=self._sequence,
            records=await _append_records(converter, wires),
            digest=batch_digest(wires),
        )
        if self._activity is not None:
            request.activity.CopyFrom(self._activity)
        else:
            request.named.CopyFrom(
                NamedProducer(producer_id=self._producer_id, attempt=self._attempt)
            )
        service = await self._handle._plugin._service_for(self._handle._client)
        response = await call(service.append(request))
        self._sequence += len(wires)
        self._last = Cursor(response.last_cursor)
        return self._last


async def _append_records(
    converter: Any, wires: list[WireRecord]
) -> list[AppendRecord]:
    bodies = iter(
        await encode_bodies(converter, [w.body for w in wires if w.HasField("body")])
    )
    records = []
    for wire in wires:
        record = AppendRecord(kind=wire.kind)
        if wire.HasField("body"):
            record.body.CopyFrom(next(bodies))
            record.content_hash = content_hash(wire.body)
        records.append(record)
    return records


class StreamHandle:
    """One stream, addressed by topic, from outside Workflow code.

    A handle on a Workflow's stream follows the run chain, so a read goes on
    across Continue-as-New and ends when the chain closes. A topic is a
    :class:`temporalio.contrib.streams.StreamTopic`, which carries the record
    type, or a plain string with ``result_type=`` for a name decided at
    runtime. A call that names no topic addresses the topic of :attr:`ref`.
    """

    def __init__(
        self,
        plugin: StreamStorePlugin,
        client: Client,
        ref: StreamRef,
    ) -> None:
        """Prefer :func:`temporalio.contrib.streams.get_stream_handle`."""
        self._plugin = plugin
        self._client = client
        self._ref = ref
        # The stream is the Workflow's, and the Workflow encodes its records with
        # its own context, so every reader and producer on it uses that context.
        self._converter: DataConverter = client.data_converter.with_context(
            WorkflowSerializationContext(
                namespace=client.namespace, workflow_id=ref.workflow_id
            )
        )

    @property
    def ref(self) -> StreamRef:
        """The stream this handle is on, as data another process can open."""
        return self._ref

    @overload
    def read(
        self, *, topic: StreamTopic[T], after: Cursor = ...
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: type[T],
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """Yield the records on ``topic`` after ``after`` as they arrive.

        ``BEGINNING`` starts at the oldest record the topic retains, and
        ``END`` at whatever is appended after the read starts. Any other
        cursor came from a record of this stream, and the read resumes just
        past it, so a reader that keeps the last cursor it handled and hands
        it back sees every stored record once. A resumed read knows the
        attempt of the record at its cursor, so a new attempt of that
        producer still arrives as ``SUPERSEDED``. Earlier attempts of other
        producers are not known to it. A ``SUPERSEDED`` record withdraws only
        what its producer wrote in earlier attempts. The other producers'
        records on the topic stay valid. The read ends when the owner's run
        chain is closed and every retained record has been delivered. The
        result is a generator, so a caller that stops early calls
        ``aclose()`` on it.

        The read starts when the generator first runs, not when this call
        returns, and ``END`` is resolved then. A reader that must not miss
        the next record, such as one about to send something the Workflow
        answers on the stream, takes ``latest()`` first and reads after it::

            start = await handle.latest()
            await client.execute_update(...)
            async for record in handle.read(after=start):
                ...

        Raises:
            ValueError: ``result_type`` was passed with a topic definition,
                or the topic is empty. Raised by this call.
            StreamCursorError: ``after`` came from another store or another
                stream. Raised by the first iteration.
            StreamExpiredError: ``after`` names a record the store no longer
                retains, or the read fell behind retention while it ran.
            StreamRecordError: A record did not decode. Read on from its
                cursor to skip it.
            StreamNotFoundError: The owner doesn't exist.
            StreamStorageError: The store failed or couldn't be reached, as
                a Redis blip during a long read does.

        Any of these ends the generator. Start a new read after the last
        cursor you handled. It reads on from there, though it no longer knows
        which earlier attempts of other producers it delivered.
        """
        name, result_type = self._resolve(topic, result_type)
        return self._read(name, after, result_type)

    async def _read(
        self, topic: str, after: Cursor, result_type: type | None
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        service = await self._plugin._service_for(self._client)
        address = self._address(topic, run_id=await self._read_run_id())
        request = ReadRequest(stream=address, after=after.token)
        request.wait.FromTimedelta(_READ_WAIT)
        while True:
            response = await call(service.read(request))
            for record in response.records:
                yield await from_read(self._converter, record, result_type)
            if response.done:
                return
            request.after = response.cursor
            request.state = response.state

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """The cursor of the newest record on ``topic``, or ``BEGINNING`` when empty.

        ``read(after=latest())`` yields only what is appended after this call
        returned, which is how a client positions itself before it sends
        something the Workflow answers on the stream.
        """
        name, _ = self._resolve(topic, None)
        service = await self._plugin._service_for(self._client)
        response = await call(service.latest(LatestRequest(stream=self._address(name))))
        return Cursor(response.cursor)

    @overload
    def producer(
        self, *, topic: StreamTopic[T], producer_id: str, attempt: int
    ) -> StreamProducer[T]: ...

    @overload
    def producer(
        self, *, topic: str | None = None, producer_id: str, attempt: int
    ) -> StreamProducer[Any]: ...

    def producer(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        producer_id: str,
        attempt: int,
    ) -> StreamProducer[Any]:
        """A producer on ``topic`` that writes as ``producer_id`` in ``attempt``.

        A process that restarts its work raises ``attempt``, which starts a
        new sequence and tells readers that what the earlier attempt of this
        ``producer_id`` wrote is superseded. Records of other producers are
        not. Keep one producer object per ``(topic, producer_id,
        attempt)``. Each object numbers its own records from one. A second
        object for the same session is refused as stale once the first moved
        past its first batch. Before that, a second object's first batch with
        the same content as the first object's is taken as a retry, and the
        store writes nothing, with no error.

        Raises:
            ValueError: ``producer_id`` is empty or ``attempt`` is below one.
        """
        if not producer_id:
            raise ValueError("producer_id must not be empty")
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError(f"attempt must be an int of at least 1, got {attempt!r}")
        name, _ = self._resolve(topic, None)
        return StreamProducer(self, name, producer_id, attempt)

    def _resolve(
        self, topic: str | StreamTopic[Any] | None, result_type: type | None
    ) -> tuple[str, type | None]:
        return resolve_topic(self._ref.topic if topic is None else topic, result_type)

    def _address(self, topic: str, *, run_id: str | None = None) -> StreamAddress:
        return StreamAddress(
            namespace=self._client.namespace,
            owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
            workflow_id=self._ref.workflow_id,
            run_id=(self._ref.chain_run_id or "") if run_id is None else run_id,
            topic=topic,
        )

    async def _read_run_id(self) -> str:
        """The run a read names to Core, which ends a read when that run closes.

        A read must end only with its chain. While the chain that holds
        ``chain_run_id`` is the Workflow id's newest, the read names no run
        and follows that chain to its end. Once a newer chain took the id,
        the old chain has ended, and a read on any of its runs delivers what
        is left and ends.

        Raises:
            StreamNotFoundError: Temporal holds no run ``chain_run_id``.
            StreamError: Describing the Workflow failed.
        """
        run_id = self._ref.chain_run_id
        if not run_id:
            return ""
        workflow_id = self._ref.workflow_id
        try:
            named = await self._client.get_workflow_handle(
                workflow_id, run_id=run_id
            ).describe()
            newest = await self._client.get_workflow_handle(workflow_id).describe()
        except RPCError as error:
            if error.status == RPCStatusCode.NOT_FOUND:
                raise StreamNotFoundError(
                    f"Workflow {workflow_id!r} has no run {run_id!r}"
                ) from error
            raise StreamError(
                f"could not describe Workflow {workflow_id!r}: {error}"
            ) from error
        first = named.raw_description.workflow_execution_info.first_run_id
        if first == newest.raw_description.workflow_execution_info.first_run_id:
            return ""
        return run_id


class ActivityStreamHandle:
    """The scheduling Workflow's stream, as the running Activity sees it.

    Reads and positions as :class:`temporalio.contrib.streams.StreamHandle`
    does. :meth:`producer` writes as this Activity, which is the only
    identity that lets readers tell this Activity's retry from a new
    producer.
    """

    def __init__(
        self,
        inner: StreamHandle,
        identity: ActivityProducer,
        producers: dict[str, StreamProducer[Any]],
    ) -> None:
        """Prefer :func:`temporalio.contrib.streams.activity_handle`."""
        self._inner = inner
        self._identity = identity
        self._producers = producers

    @property
    def ref(self) -> StreamRef:
        """The stream this handle is on, as data another process can open."""
        return self._inner.ref

    @overload
    def read(
        self, *, topic: StreamTopic[T], after: Cursor = ...
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: type[T],
    ) -> AsyncGenerator[StreamRecord[T], None]: ...

    @overload
    def read(
        self,
        *,
        topic: str | None = None,
        after: Cursor = ...,
        result_type: None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]: ...

    def read(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor = BEGINNING,
        result_type: type | None = None,
    ) -> AsyncGenerator[StreamRecord[Any], None]:
        """See :meth:`temporalio.contrib.streams.StreamHandle.read`."""
        # One call forwards every overload, which the checker can't match one by one.
        inner = cast(Any, self._inner)
        return inner.read(topic=topic, after=after, result_type=result_type)

    async def latest(self, *, topic: str | StreamTopic[Any] | None = None) -> Cursor:
        """See :meth:`temporalio.contrib.streams.StreamHandle.latest`."""
        return await self._inner.latest(topic=topic)

    @overload
    def producer(self, *, topic: StreamTopic[T]) -> StreamProducer[T]: ...

    @overload
    def producer(self, *, topic: str | None = None) -> StreamProducer[Any]: ...

    def producer(
        self, *, topic: str | StreamTopic[Any] | None = None
    ) -> StreamProducer[Any]:
        """A producer on ``topic`` that writes as this Activity attempt.

        The producer id is ``<Activity id>@<scheduling run id>`` and the
        attempt is the Activity's Temporal attempt, so the first record of a
        retry makes readers see ``SUPERSEDED``, which withdraws what this
        Activity's earlier attempts wrote and nothing else. A retry starts a
        new sequence, and the store deduplicates a repeated append within one
        attempt.

        Every call in one attempt returns the same producer for a topic, so
        two parts of the Activity that each ask for it number their records
        in one sequence.
        """
        name, _ = self._inner._resolve(topic, None)
        producer = self._producers.get(name)
        if producer is None:
            identity = self._identity
            producer = self._producers[name] = StreamProducer(
                self._inner,
                name,
                f"{identity.activity_id}@{identity.run_id}",
                identity.attempt,
                activity=identity,
            )
        return producer


def activity_handle() -> ActivityStreamHandle:
    """The stream of the Workflow that scheduled the running Activity.

    Its ref names the chain of the scheduling run with ``chain_run_id``, so
    its producers write to that chain even once a new chain took the
    Workflow id. A read on it follows that chain across Continue-as-New and
    ends when the chain ends. To write to another
    Workflow's stream, use
    :func:`get_stream_handle` with ``temporalio.activity.client()`` and a
    producer id of your own.

    A Local Activity has no stream producer in this release. Its attempt is
    not durable, so after a Worker crash it runs again as the same attempt,
    and the store refuses its first append as stale. Use a regular Activity
    instead.

    Raises:
        StreamUnsupportedError: The Activity is a Local Activity, or it was
            not scheduled by a Workflow, since streams owned by an Activity
            are not supported in this release.
        ValueError: No stream store is registered on the Worker's client.
        RuntimeError: Not called from inside an Activity.
    """
    info = temporalio.activity.info()
    if info.is_local:
        raise StreamUnsupportedError(
            "a Local Activity has no stream producer in this release, since its "
            "attempt isn't durable; use a regular Activity instead"
        )
    if info.workflow_id is None or info.workflow_run_id is None:
        raise StreamUnsupportedError(
            "this Activity was not scheduled by a Workflow, and streams owned by "
            "an Activity are not supported in this release"
        )
    client = temporalio.activity.client()
    ref = StreamRef.for_workflow(info.workflow_id, chain_run_id=info.workflow_run_id)
    identity = ActivityProducer(
        workflow_id=info.workflow_id,
        run_id=info.workflow_run_id,
        activity_id=info.activity_id,
        attempt=info.attempt,
    )
    inner = StreamHandle(store_for_client(client), client, ref)
    return ActivityStreamHandle(inner, identity, _attempt_producers())


# The producers of each running Activity attempt, by topic, under the id of the
# attempt's context. The context isn't hashable, so a finalizer drops the entry
# with it.
_activity_producers: dict[int, dict[str, StreamProducer[Any]]] = {}


def _attempt_producers() -> dict[str, StreamProducer[Any]]:
    context = temporalio.activity._Context.current()
    key = id(context)
    producers = _activity_producers.get(key)
    if producers is None:
        producers = _activity_producers[key] = {}
        weakref.finalize(context, _activity_producers.pop, key, None)
    return producers


def get_stream_handle(
    client: Client,
    workflow_id: str | StreamRef,
    *,
    topic: str | StreamTopic[Any] | None = None,
    chain_run_id: str | None = None,
) -> StreamHandle:
    """A handle on a Workflow's stream, through the client's store.

    The handle follows the Workflow's run chain, so a read continues across
    Continue-as-New and ends when the chain closes. ``topic`` is the topic a
    call that names none addresses.

    Args:
        client: A client that carries a stream store plugin.
        workflow_id: The Workflow that owns the stream, or a
            :class:`temporalio.contrib.streams.StreamRef` that names it.
        topic: The handle's default topic. Without one it is
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        chain_run_id: Any run of the chain to open. Without one the handle
            opens the Workflow id's newest chain. It does not end a read
            when that run closes.

    Raises:
        ValueError: ``topic`` or ``chain_run_id`` was given with a ref, or no
            stream store is registered on ``client``.
    """
    if isinstance(workflow_id, StreamRef):
        if topic is not None or chain_run_id is not None:
            raise ValueError("a StreamRef carries its own topic and chain_run_id")
        ref = workflow_id
    else:
        ref = StreamRef.for_workflow(
            workflow_id, topic=topic, chain_run_id=chain_run_id
        )
    ref._require_supported()
    return StreamHandle(store_for_client(client), client, ref)
