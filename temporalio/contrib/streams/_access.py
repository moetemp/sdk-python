"""Stream calls outside Workflow code, which Activity and client handles share."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, TypeVar, cast

import temporalio.converter
from temporalio.contrib.streams import _convert, _reader
from temporalio.contrib.streams._backend import CallOptions, StreamBackend
from temporalio.contrib.streams._defn import TopicDefinition
from temporalio.contrib.streams._handle import StreamHandle, TopicBase
from temporalio.contrib.streams._items import End, close_status
from temporalio.contrib.streams._reader import Item, Position, Subscription
from temporalio.contrib.streams._writer import StreamWriter
from temporalio.contrib.streams.system import models

T = TypeVar("T")


@dataclass
class Access:
    """The calls of one Stream handle, bound to a backend."""

    backend: StreamBackend
    converter: temporalio.converter.DataConverter
    namespace: str
    stream_id: Callable[[], Awaitable[str]]
    writer: Callable[[], StreamWriter]
    # The run chain that a close names, which the Store checks against the Stream's owner.
    workflow_id: str | None = None
    first_execution_run_id: str | None = None

    async def publish(self, topic: str, values: Sequence[Any]) -> int:
        """Write the values as Records now, and return the offset of the last one."""
        if not values:
            raise TypeError("publish takes one or more values")
        return await self.writer().write([(topic, value) for value in values])

    async def publish_records(self, records: Sequence[tuple[str, Any]]) -> int:
        """Write ``(topic, value)`` pairs as Records now, in order, and return the offset of
        the last one.
        """
        return await self.writer().write(records)

    async def items(
        self, read: ReadSpec, position: Position
    ) -> AsyncGenerator[Item, None]:
        """Read the Stream's items in order, and move ``position`` past each page."""
        items = _reader.read(
            self.backend.service(self.namespace, _convert.raw(self.converter)),
            self.converter,
            await self.stream_id(),
            position=position,
            topics=read.topics,
            topic_prefixes=read.prefixes,
            types=read.types,
            default_type=read.default_type,
            from_offset=read.from_offset,
            last=read.last,
            wait_new_records=read.wait_new_records,
            create_wait=read.create_wait,
        )
        try:
            async for item in items:
                yield item
        finally:
            await items.aclose()

    async def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one.

        It is 0 for a Stream with no Record, and ``from_offset=0`` reads from the start.
        Offsets are sparse and above zero, so 0 is never a Record's offset.
        """
        response = await self.backend.service(self.namespace).describe_stream(
            models.DescribeStreamRequest(
                stream=models.StreamRef(id=await self.stream_id())
            )
        )
        # Offsets start above zero, so zero means the Stream has no Record.
        return response.last_offset + 1 if response.last_offset else 0

    async def close(self, result: Any | None, failure: BaseException | None) -> None:
        """Write this Producer's Records, then close the Stream."""
        writer = self.writer()
        await writer.close()
        producer = models._ProducerTransferTypeConverter().from_transfer_type(
            writer.producer, models.Producer
        )
        await self.backend.service(self.namespace, self.converter).close_stream(
            models.CloseStreamRequest(
                stream=models.StreamRef(
                    id=await self.stream_id(),
                    workflow_id=self.workflow_id,
                    first_execution_run_id=self.first_execution_run_id,
                ),
                producer=producer,
                result=result,
                failure=failure,
                status=close_status(failure),
            )
        )


@dataclass
class ReadSpec:
    """What one read asks for."""

    topics: list[str] | None
    prefixes: Sequence[str]
    types: dict[str, Any]
    default_type: Any
    from_offset: int | End | None
    last: int | None
    wait_new_records: bool
    create_wait: timedelta | None


def read_spec(
    handle: StreamHandle[Any],
    topics: Sequence[
        str | TopicDefinition[Any] | _AsyncTopic[Any] | SyncTopicBase[Any]
    ],
    *,
    prefixes: Sequence[str],
    default_type: Any,
    from_offset: int | End | None,
    last: int | None,
    wait_new_records: bool,
    create_wait: timedelta | None,
) -> ReadSpec:
    """The Topic names of a read, the decode type of each known Topic, and its options.

    Raises:
        ValueError: ``last`` is not above zero, or it comes with ``from_offset``.
    """
    if last is not None and (last < 1 or from_offset is not None):
        raise ValueError("last takes a count above zero and no from_offset")
    types = handle._known_types()
    names = []
    for topic in topics:
        if isinstance(topic, (_AsyncTopic, SyncTopicBase)):
            name, topic_type = topic.name, topic.type
        elif isinstance(topic, TopicDefinition):
            name, topic_type = handle._topic_spec(topic, None)
        else:
            # A read names a Topic outside the definition with no type, and its values
            # decode with the read's default type.
            name, topic_type = topic, types.get(topic)
        names.append(name)
        if topic_type is not None:
            types[name] = topic_type
    return ReadSpec(
        topics=names or None,
        prefixes=prefixes,
        types=types,
        default_type=default_type,
        from_offset=from_offset,
        last=last,
        wait_new_records=wait_new_records,
        create_wait=create_wait,
    )


def subscription(
    items: Callable[[Position], AsyncGenerator[Item, None]],
    read: ReadSpec,
    options: CallOptions | None = None,
) -> Subscription[Any]:
    """A read over the items that ``items`` gives."""
    # Before its first page, a read from the start resumes at 0, and a read from END or of the
    # last Records has no offset yet.
    if read.last is not None or read.from_offset is End.END:
        position = Position(None)
    else:
        position = Position(read.from_offset or 0)
    return Subscription(items(position), position, options)


class _AsyncTopic(TopicBase[T]):
    # A Topic of an Activity or client handle. Each publish writes before it returns.

    async def publish(self, *values: T) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value.
        """
        await self._stream._ready()
        return await self._stream._access().publish(self._name, values)

    def subscribe(
        self,
        *,
        from_offset: int | End | None = None,
        last: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
    ) -> Subscription[T]:
        """Read this Topic only. Its Records have the Topic's type. See the Stream's
        ``subscribe``.
        """
        return cast(
            Subscription[T],
            self._stream.subscribe(
                self,
                from_offset=from_offset,
                last=last,
                wait_new_records=wait_new_records,
                create_wait=create_wait,
            ),
        )


class SyncTopicBase(TopicBase[T]):
    """Base of a Topic whose calls block, for sync Activities."""
