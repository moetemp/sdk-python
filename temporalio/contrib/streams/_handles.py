"""Reaching a stream from an Activity or from client code.

Every call goes to the stream runtime's service, which checks the owner, mints
and checks cursors, and synthesizes ``SUPERSEDED``. What stays here is what
needs the plaintext or the user's types: converting values, the two hashes
taken before the codec, and the codec itself.

An Activity reaches the stream of the Workflow that scheduled it, or its own
stream when the Workflow scheduled it to own one, and writes as itself. The
stream runtime derives its producer id as ``<Activity id>@<scheduling run id>``
and its producer attempt from the attempt's start time and its Temporal
attempt, so a retry is reported to readers as ``SUPERSEDED``. The run is part
of the id because a stream outlives a run, and Activity ids repeat across the
runs of a chain. A standalone Activity has no scheduling Workflow. It owns a
stream of its own and writes it as ``<Activity id>@<Activity run id>``. A
Local Activity writes as ``__temporal_local_<call number>@<scheduling run id>``,
with an attempt that grows across its reruns and the facts of the Workflow Task
that Core dispatched it in (see ``_local_activity``). Its attempt doesn't fit
the Activity form's 32 bits, so it writes as a named producer.

Any process holding a client reaches a Workflow's stream by Workflow id and
writes with a producer id and attempt of its own.
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Any, Generic, Literal, TypeVar, cast, overload

from google.protobuf.timestamp_pb2 import Timestamp

import temporalio.activity
from temporalio.bridge.proto.streams import (
    ActivityProducer,
    AppendRecord,
    AppendRequest,
    CloseRequest,
    LatestRequest,
    NamedProducer,
    ReadRequest,
    ReadResponse,
    StreamAddress,
    WaitForNotificationRequest,
)
from temporalio.bridge.streams_generated import StreamService
from temporalio.client import Client
from temporalio.contrib.streams._body import batch_digest, content_hash, encode_bodies
from temporalio.contrib.streams._cursor import BEGINNING
from temporalio.contrib.streams._errors import (
    StreamError,
    StreamNotFoundError,
    StreamUnsupportedError,
)
from temporalio.contrib.streams._follow import FollowedItem, FollowPosition, follow
from temporalio.contrib.streams._local_activity import (
    LocalCall,
    current_local_call,
    owns_stream,
)
from temporalio.contrib.streams._plugin import (
    StreamStorePlugin,
    call,
    store_for_client,
)
from temporalio.contrib.streams._record import (
    Cursor,
    LinkEvent,
    RecordKind,
    StreamLink,
    StreamRecord,
)
from temporalio.contrib.streams._ref import StreamRef, owner_context, stream_address
from temporalio.contrib.streams._topic import StreamTopic, resolve_topic
from temporalio.contrib.streams._wire import WireRecord, from_read, to_wire
from temporalio.converter import DataConverter
from temporalio.service import RPCError, RPCStatusCode

__all__ = [
    "ActivityStreamHandle",
    "StreamHandle",
    "StreamProducer",
    "activity_handle",
    "stream_handle",
]

T = TypeVar("T")

_READ_WAIT = timedelta(seconds=30)
# How long a read that sleeps on the notifier waits in the store before it sleeps.
_IDLE_WAIT = timedelta(seconds=2)


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
        local_call: LocalCall | None = None,
    ) -> None:
        """Prefer :meth:`StreamHandle.producer`."""
        self._handle = handle
        self._topic = topic
        self._producer_id = producer_id
        self._attempt = attempt
        self._activity = activity
        self._local_call = local_call
        self._sequence = 1
        self._last = BEGINNING
        # A batch reads the sequence, then awaits the codec and the store, then
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
        position and writes nothing. A store without producer state writes
        the repeat and answers its position, and readers drop the copy.

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

    async def link(
        self,
        ref: StreamRef,
        *,
        name: str,
        after: Cursor | None = None,
        value: Any = None,
    ) -> Cursor:
        """Write a ``LINK`` that opens ``ref``'s stream, and return its cursor.

        A follower reads the linked stream from strictly after ``after``, or
        from its oldest record, in one order with this stream. ``name`` names
        the link within this producer, and :meth:`join` names it again.
        ``ref`` is stored as plaintext. ``value``, when given, goes through
        the converter and codec as a ``DATA`` value does.

        Raises:
            ValueError: ``name`` is empty or over 256 bytes.
            StreamError: As :meth:`append` raises.
        """
        link = StreamLink(LinkEvent.OPEN, ref, name, after=after)
        async with self._lock:
            return await self._write(RecordKind.LINK, [value], link)

    async def join(self, *, name: str, until: Cursor | None = None) -> Cursor:
        """Write a ``LINK`` that says this producer waited for link ``name``.

        A follower holds this stream's later records until the linked stream
        reached ``until``, or its end when ``until`` is ``None``.

        Raises:
            ValueError: ``name`` is empty or over 256 bytes.
            StreamError: As :meth:`append` raises.
        """
        link = StreamLink(LinkEvent.JOIN, None, name, until=until)
        async with self._lock:
            return await self._write(RecordKind.LINK, [None], link)

    async def _write(
        self, kind: RecordKind, values: Any, link: StreamLink | None = None
    ) -> Cursor:
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
                link=link,
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
            if self._local_call is not None:
                request.named.task_started_event_id = (
                    self._local_call.task_started_event_id
                )
                request.named.completed_started_event_ids.extend(
                    self._local_call.completed_started_event_ids
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
        if wire.HasField("link"):
            record.link.CopyFrom(wire.link)
        if wire.HasField("body"):
            record.body.CopyFrom(next(bodies))
            record.content_hash = content_hash(wire.body)
        records.append(record)
    return records


class StreamHandle:
    """One stream, addressed by topic, from outside Workflow code.

    A handle on a Workflow's stream follows the run chain, so a read goes on
    across Continue-as-New and ends when the chain closes. A handle on a
    standalone Activity's stream ends a read when the Activity run ends, and
    a handle on the stream of an Activity that a Workflow scheduled ends a
    read when the Workflow saw the Activity end. A topic is a
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
        """Prefer :func:`temporalio.contrib.streams.stream_handle`."""
        self._plugin = plugin
        self._client = client
        self._ref = ref
        # Every reader and producer of one owner's streams uses one context,
        # which each of them can build from the ref alone.
        self._converter: DataConverter = client.data_converter.with_context(
            owner_context(client.namespace, ref.workflow_id, ref.activity_id)
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

        ``BEGINNING`` starts at the oldest record the topic retains, in the
        store's archive when it has one, and ``END`` at whatever is appended
        after the read starts. Any other
        cursor came from a record of this stream, and the read resumes just
        past it, so a reader that keeps the last cursor it handled and hands
        it back sees every stored record once. A resumed read knows the
        attempt of the record at its cursor, so a new attempt of that
        producer still arrives as ``SUPERSEDED``. Earlier attempts of other
        producers are not known to it. A ``SUPERSEDED`` record withdraws only
        what its producer wrote in earlier attempts. The other producers'
        records on the topic stay valid. The read ends when the owner's run
        chain is closed and every retained record has been delivered, or
        when the topic was closed and read to its end. A topic closed with
        an ``error=`` raises that error, through the client's failure
        converter, after its last record. The result is a generator, so a
        caller that stops early calls ``aclose()`` on it.

        With :meth:`temporalio.contrib.streams.StreamStorePlugin.notify_on_append`
        on, the default, an idle read sleeps on the stream's notifier on the server. A read
        that waits 2 seconds and finds nothing comes back empty, and the
        reader waits until a producer notifies, the stream closes, or the server's wait
        limit (60 seconds by default) passes, and then reads again. That read
        also finds records whose notification was lost. Without
        notifications, or on a server without the stream notifier, an idle
        read waits in the store instead, which holds a store connection while
        it waits.

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
            StreamNotFoundError: The owner doesn't exist, and didn't start
                within the store's ``owner_start_grace``. Until then the
                read waits.
            StreamStorageError: The store failed or couldn't be reached, as
                a Redis blip during a long read does.
            BaseException: The error the topic was closed with, after its
                last record.

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
        sleep = self._plugin._waits_on_notifier()
        self._set_wait(request, sleep)
        while True:
            response = await call(service.read(request))
            for record in response.records:
                yield await from_read(self._converter, record, result_type)
            if response.done:
                if response.HasField("close_failure"):
                    raise await self._converter.decode_failure(response.close_failure)
                return
            request.after = response.cursor
            request.state = response.state
            if sleep and not response.records:
                sleep = await self._wait_for_notification(service, request, response)

    @staticmethod
    def _set_wait(request: ReadRequest, sleep: bool) -> None:
        # A read that sleeps on the notifier waits only its idle time in the
        # store before it sleeps.
        request.wait.FromTimedelta(_IDLE_WAIT if sleep else _READ_WAIT)

    async def _wait_for_notification(
        self, service: StreamService, request: ReadRequest, empty: ReadResponse
    ) -> bool:
        """Wait until the stream moves past an empty read, closes, or the poll ends.

        Every answer leads to another read. After the server's wait limit,
        that read finds records whose notification was lost, or comes back
        empty and sleeps again. Returns whether the read can sleep on the
        notifier again.
        """
        wait = WaitForNotificationRequest(
            stream=request.stream, after_counter=empty.wake_after
        )
        try:
            await call(service.wait_for_notification(wait))
        except StreamUnsupportedError:
            # The server has no stream notifier, so the store's blocking read is
            # the only way to wait, for this read and the process's later ones.
            self._plugin._notifier_unsupported = True
            self._set_wait(request, False)
            return False
        except StreamNotFoundError:
            # The owner hasn't started yet. The next read waits out the owner
            # start grace, and fails once it ends.
            pass
        return True

    def follow(
        self,
        *,
        topic: str | StreamTopic[Any] | None = None,
        after: Cursor | FollowPosition = BEGINNING,
        max_depth: int = 4,
        max_open: int = 16,
        result_type: type | None = None,
    ) -> AsyncGenerator[FollowedItem[Any], None]:
        """Yield the records of ``topic`` and of the streams its links open, in one order.

        Each ``LINK`` with event ``OPEN`` opens a segment of the linked
        stream, and the follower reads it in place. A child's records never
        come before its ``OPEN``. A record that comes after a ``JOIN`` waits
        until the child reached the join point or its end. Other records
        come in the order they were read. No clock decides the order. Each
        item names its source stream, and keeps that stream's own cursor.

        Link reports come as items with ``link_state`` set. ``OPENED`` when a
        segment starts, ``DONE`` with the child's close outcome when it ends,
        ``UNAVAILABLE`` with the error when the child can't be read, and
        ``SKIPPED`` for a link past ``max_depth`` or ``max_open``, to a
        stream already open, to an ancestor, or behind another Nexus
        endpoint. A child never fails the follow. The link records
        themselves come too, as records of their stream.

        ``result_type`` applies to the root's records. A child's records
        decode with the default rules, since a child's topic may hold
        another type.

        Only the last item of each batch carries ``position``, which resumes
        the root and every open segment with no loss and no repeat. An
        earlier item carries ``None``. To resume from it, resume from the
        previous batch end, which repeats the rest of that batch. The follow
        ends when the root is done and every segment ended. The root's close
        failure raises after its last item.

        Raises:
            ValueError: A bound is below one, or ``result_type`` was passed
                with a topic definition.
            StreamError: As :meth:`read` raises for the root. Only a
                ``STORAGE`` failure of a child fails the follow.
        """
        if max_depth < 1 or max_open < 1:
            raise ValueError("max_depth and max_open must be at least 1")
        name, result_type = self._resolve(topic, result_type)
        return follow(self, name, after, max_depth, max_open, result_type)

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

    async def close(
        self,
        result: Any = None,
        *,
        topic: str | StreamTopic[Any] | None = None,
        error: BaseException | None = None,
    ) -> None:
        """Close ``topic`` with ``result``, or with ``error`` as a failure.

        The store refuses appends from then on, and every read ends after
        the last record. The stream runtime then closes the stream's notifier on the
        server, so each reader that sleeps on it wakes and sees the outcome.
        ``result`` goes through the client's data converter and codec, and
        ``error`` through its failure converter. A close with neither ends
        the stream with a null result.

        A close of a stream that is already closed is accepted, and the
        first close's outcome stays, so a retried close is safe. On a server
        without the stream notifier, the close ends the stream in the store
        only.

        Raises:
            ValueError: Both ``result`` and ``error`` are set.
            StreamError: The store or the notifier refused the close.

        .. warning::
            This API is experimental.
        """
        await self._close(result, topic=topic, error=error)

    async def _close(
        self,
        result: Any,
        *,
        topic: str | StreamTopic[Any] | None,
        error: BaseException | None,
        closed_by: str = "",
    ) -> None:
        if result is not None and error is not None:
            raise ValueError("a stream closes with a result or an error, not both")
        name, _ = self._resolve(topic, None)
        request = CloseRequest(stream=self._address(name), closed_by=closed_by)
        if error is not None:
            await self._converter.encode_failure(error, request.failure)
        elif result is not None:
            [payload] = await encode_bodies(
                self._converter, self._converter.payload_converter.to_payloads([result])
            )
            request.result.CopyFrom(payload)
        service = await self._plugin._service_for(self._client)
        await call(service.close(request))

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
        return stream_address(
            self._client.namespace, self._ref, topic=topic, run_id=run_id
        )

    async def _read_run_id(self) -> str:
        """The run a read names to the store, which ends a read when that run closes.

        A read must end only with its chain. While the chain that holds
        ``chain_run_id`` is the Workflow id's newest, the read names no run
        and follows that chain to its end. Once a newer chain took the id,
        the old chain has ended, and a read on any of its runs delivers what
        is left and ends. An Activity's stream belongs to one run, so a read
        on it names the ref's run and asks Temporal nothing.

        Raises:
            StreamNotFoundError: Temporal holds no run ``chain_run_id``.
            StreamError: Describing the Workflow failed.
        """
        run_id = self._ref.chain_run_id
        if not run_id or self._ref.kind != "workflow":
            return run_id or ""
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
    """The running Activity's stream, as the Activity sees it.

    It is the Activity's own stream or the scheduling Workflow's stream, as
    :func:`temporalio.contrib.streams.activity_handle` picked.

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
        local_call: LocalCall | None = None,
    ) -> None:
        """Prefer :func:`temporalio.contrib.streams.activity_handle`."""
        self._inner = inner
        self._identity = identity
        self._producers = producers
        self._local_call = local_call

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

    async def close(
        self,
        result: Any = None,
        *,
        topic: str | StreamTopic[Any] | None = None,
        error: BaseException | None = None,
    ) -> None:
        """See :meth:`temporalio.contrib.streams.StreamHandle.close`.

        The close names this Activity's producer id as its ``closed_by``.
        """
        identity = self._identity
        await self._inner._close(
            result,
            topic=topic,
            error=error,
            closed_by=f"{identity.activity_id}@{identity.run_id}",
        )

    @overload
    def producer(self, *, topic: StreamTopic[T]) -> StreamProducer[T]: ...

    @overload
    def producer(self, *, topic: str | None = None) -> StreamProducer[Any]: ...

    def producer(
        self, *, topic: str | StreamTopic[Any] | None = None
    ) -> StreamProducer[Any]:
        """A producer on ``topic`` that writes as this Activity attempt.

        The producer id is ``<Activity id>@<scheduling run id>``, or
        ``<Activity id>@<Activity run id>`` in a standalone Activity. The
        attempt is ``started_ms << 11 | min(temporal_attempt, 2047)``, where
        ``started_ms`` is the Unix time in milliseconds at which the server
        started this attempt. A retry starts later, and so does an attempt
        after a reset that set the Temporal attempt back to one, so the
        first record of either makes readers see ``SUPERSEDED``, which
        withdraws what this Activity's earlier attempts wrote and nothing
        else. An attempt that timed out but still writes started earlier, so
        readers see its records as ``stale``. A new attempt starts a new
        sequence, and the store deduplicates a repeated append within one
        attempt.

        In a Local Activity the producer id is
        ``__temporal_local_<call number>@<scheduling run id>``, where the number counts
        the run's Local Activity calls in order. The attempt is ``task attempt
        << 14 | min(a, 16383)``. The task attempt is ``prefix << 16 | retry``
        of the Workflow Task that scheduled the call, as the Workflow's own
        records write it, and ``a`` is the Local Activity's attempt. So a
        rerun after a Worker crash or a failed Workflow Task, and a local
        retry, each write a higher attempt. The records carry the task's
        started event id and the run's last completed tasks, so a reader
        withdraws them when the task never completed.

        Every call in one attempt returns the same producer for a topic, so
        two parts of the Activity that each ask for it number their records
        in one sequence.
        """
        name, _ = self._inner._resolve(topic, None)
        producer = self._producers.get(name)
        if producer is None:
            identity, local_call = self._identity, self._local_call
            if local_call is not None:
                producer = StreamProducer(
                    self._inner,
                    name,
                    f"{local_call.producer_id_prefix}@{identity.run_id}",
                    local_call.attempt(identity.attempt),
                    local_call=local_call,
                )
            else:
                producer = StreamProducer(
                    self._inner,
                    name,
                    f"{identity.activity_id}@{identity.run_id}",
                    _activity_attempt(identity),
                    activity=identity,
                )
            self._producers[name] = producer
        return producer


def activity_handle(
    scope: Literal["activity", "workflow"] | None = None,
) -> ActivityStreamHandle:
    """The running Activity's stream.

    ``scope="activity"`` is the Activity's own stream, and
    ``scope="workflow"`` the stream of the Workflow that scheduled it. With
    no ``scope``, an Activity that a Workflow scheduled with
    :func:`temporalio.contrib.streams.start_activity_with_stream` gets its own
    stream, and any other Activity that a Workflow scheduled gets the
    Workflow's stream. A standalone Activity has only its own stream.

    The Workflow's stream is named by its ref with ``chain_run_id`` set to
    the scheduling run, so its producers write to that chain even once a new
    chain took the Workflow id. A read on it follows that chain across
    Continue-as-New and ends when the chain ends.

    The own stream of an Activity that a Workflow scheduled is named by
    :meth:`temporalio.contrib.streams.StreamRef.for_workflow_activity` with
    the Workflow id, the scheduling run and the Activity id. Every attempt
    writes to it. It closes when the scheduling Workflow sees the Activity
    end: with a null result when the Activity completed, else with the
    failure that History holds. The Activity may close it first itself.

    The own stream of a standalone Activity is named by
    :meth:`temporalio.contrib.streams.StreamRef.for_activity` with the
    Activity id and run. Every attempt of the run writes to it, and it stays
    open until the Activity closes it or the run ends. When the run ends
    with no close, the stream's notifier closes it with the run's outcome.
    The SDK closes nothing when the Activity returns, so call
    :meth:`ActivityStreamHandle.close` as the last step.

    To write to another stream, use :func:`stream_handle` with
    ``temporalio.activity.client()`` and a producer id of your own.

    A Local Activity writes the Workflow's stream too, while the Workflow
    Task that runs it is still open. Its attempt is not durable, so the
    store plugin's Worker interceptor stamps each call with a number for a
    producer id that survives a rerun. Core gives it the facts of the
    Workflow Task it was dispatched in, for an attempt that grows across
    reruns. See
    :meth:`ActivityStreamHandle.producer`.

    Raises:
        StreamUnsupportedError: A standalone Activity got no run id from
            the server, or asked for ``scope="workflow"``. A Local Activity
            asked for ``scope="activity"``, since it has no server state
            that could end its stream. Or it is a Local Activity that the
            Worker's stream interceptor did not stamp.
        ValueError: ``scope`` is not ``"activity"``, ``"workflow"`` or
            ``None``, or no stream store is registered on the Worker's
            client.
        RuntimeError: Not called from inside an Activity.
    """
    # A caller without a type checker can pass any value.
    if cast(Any, scope) not in (None, "activity", "workflow"):
        raise ValueError(f"scope must be 'activity' or 'workflow', got {scope!r}")
    info = temporalio.activity.info()
    if info.workflow_id is None or info.workflow_run_id is None:
        if scope == "workflow":
            raise StreamUnsupportedError(
                "a standalone Activity has no scheduling Workflow, so it has no "
                "Workflow stream"
            )
        return _standalone_handle(info)
    if info.is_local and scope == "activity":
        raise StreamUnsupportedError(
            "a Local Activity can't own a stream, since the server holds no state "
            "for it that could end the stream"
        )
    if scope is None:
        scope = "activity" if owns_stream() else "workflow"
    local_call = None
    if info.is_local:
        local_call = current_local_call()
        if local_call is None:
            raise StreamUnsupportedError(
                "this Local Activity was scheduled without the stream store's "
                "Worker interceptor, so it has no producer identity that survives "
                "a rerun; build the Worker from a client that carries the store"
            )
    client = temporalio.activity.client()
    if scope == "activity":
        ref = StreamRef.for_workflow_activity(
            info.workflow_id,
            run_id=info.workflow_run_id,
            activity_id=info.activity_id,
        )
    else:
        ref = StreamRef.for_workflow(
            info.workflow_id, chain_run_id=info.workflow_run_id
        )
    identity = ActivityProducer(
        workflow_id=info.workflow_id,
        run_id=info.workflow_run_id,
        activity_id=info.activity_id,
        attempt=info.attempt,
    )
    _set_attempt_start(identity, info.started_time)
    inner = StreamHandle(store_for_client(client), client, ref)
    return ActivityStreamHandle(inner, identity, _attempt_producers(scope), local_call)


def _standalone_handle(info: temporalio.activity.Info) -> ActivityStreamHandle:
    if not info.activity_run_id:
        raise StreamUnsupportedError(
            "the server gave this standalone Activity no run id, so it has no "
            "stream of its own"
        )
    client = temporalio.activity.client()
    ref = StreamRef.for_activity(info.activity_id, run_id=info.activity_run_id)
    identity = ActivityProducer(
        workflow_id="",
        run_id=info.activity_run_id,
        activity_id=info.activity_id,
        attempt=info.attempt,
    )
    _set_attempt_start(identity, info.started_time)
    inner = StreamHandle(store_for_client(client), client, ref)
    return ActivityStreamHandle(inner, identity, _attempt_producers())


def _set_attempt_start(identity: ActivityProducer, started: datetime) -> None:
    started_time = Timestamp()
    started_time.FromDatetime(started)
    # A start time the stream runtime refuses, such as the epoch that a test environment
    # gives, leaves the Temporal attempt as the producer attempt.
    if attempt_key(identity.attempt, started_time.ToMilliseconds()) is not None:
        identity.attempt_started_time.CopyFrom(started_time)


def _activity_attempt(identity: ActivityProducer) -> int:
    started_ms = None
    if identity.HasField("attempt_started_time"):
        started_ms = identity.attempt_started_time.ToMilliseconds()
    key = attempt_key(identity.attempt, started_ms)
    assert key is not None
    return key


def attempt_key(attempt: int, started_ms: int | None) -> int | None:
    """The producer attempt the stream runtime writes for an Activity attempt.

    ``started_ms`` is the Unix time in milliseconds at which the server
    started the attempt, or ``None`` when the producer sends none. Returns
    ``None`` for an attempt that the stream runtime refuses.
    """
    if started_ms is None:
        return attempt
    if attempt < 1 or not 0 < started_ms < 1 << 42:
        return None
    return started_ms << 11 | min(attempt, 0x7FF)


# The producers of each running Activity attempt, by scope and topic, under
# the id of the attempt's context. The context isn't hashable, so a finalizer
# drops the entry with it.
_activity_producers: dict[int, dict[str, dict[str, StreamProducer[Any]]]] = {}


def _attempt_producers(scope: str = "activity") -> dict[str, StreamProducer[Any]]:
    context = temporalio.activity._Context.current()
    key = id(context)
    scopes = _activity_producers.get(key)
    if scopes is None:
        scopes = _activity_producers[key] = {}
        weakref.finalize(context, _activity_producers.pop, key, None)
    return scopes.setdefault(scope, {})


def stream_handle(
    client: Client,
    workflow_id: str | StreamRef,
    *,
    topic: str | StreamTopic[Any] | None = None,
    chain_run_id: str | None = None,
) -> StreamHandle:
    """A handle on a stream, through the client's store.

    A handle on a Workflow's stream follows the Workflow's run chain, so a
    read continues across Continue-as-New and ends when the chain closes.
    ``topic`` is the topic a call that names none addresses. To open the
    stream of an Activity, pass a ref from
    :meth:`temporalio.contrib.streams.StreamRef.for_activity` or
    :meth:`temporalio.contrib.streams.StreamRef.for_workflow_activity`.

    Args:
        client: A client that carries a stream store plugin.
        workflow_id: The Workflow that owns the stream, or a
            :class:`temporalio.contrib.streams.StreamRef` that names any
            owner.
        topic: The handle's default topic. Without one it is
            :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        chain_run_id: Any run of the chain to open. Without one the handle
            opens the Workflow id's newest chain. It does not end a read
            when that run closes.

    Raises:
        ValueError: ``topic`` or ``chain_run_id`` was given with a ref, or no
            stream store is registered on ``client``.
        StreamUnsupportedError: The ref names an owner kind this release
            lacks.
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
