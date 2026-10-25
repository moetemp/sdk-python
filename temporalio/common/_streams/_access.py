"""Stream calls outside Workflow code, which Activity and client handles share."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, TypeVar

import temporalio.converter
from temporalio.common._streams import _convert, _reader
from temporalio.common._streams._backend import BackendCalls, CallOptions
from temporalio.common._streams._defn import TopicDefinition
from temporalio.common._streams._handle import StreamHandle, TopicBase
from temporalio.common._streams._items import End, close_status
from temporalio.common._streams._reader import Item, Position, Subscription
from temporalio.common._streams._writer import StreamWriter
from temporalio.common._streams.system import models

T = TypeVar("T")


@dataclass
class Access:
    """The calls of one Stream handle, bound to a backend."""

    backend: BackendCalls
    converter: temporalio.converter.DataConverter
    namespace: str
    stream_id: Callable[[], Awaitable[str]]
    writer: Callable[[], StreamWriter]
    # A close sends these, and the Store checks them against the Stream's owner.
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
            topic_prefixes=read.topic_prefixes,
            types=read.types,
            default_type=read.default_type,
            from_offset=read.offset,
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
        """The next offset of the Stream, the newest offset plus one, or 0 if it has no Record.

        Offsets are sparse and above zero, so ``offset=0`` reads from the start.
        """
        response = await self.backend.service(self.namespace).describe_stream(
            models.DescribeStreamRequest(
                stream=models.StreamRef(id=await self.stream_id())
            )
        )
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
    topic_prefixes: Sequence[str]
    types: dict[str, Any]
    default_type: Any
    offset: int | End | None
    last: int | None
    wait_new_records: bool
    create_wait: timedelta | None


def read_spec(
    handle: StreamHandle[Any],
    topics: Sequence[
        str | TopicDefinition[Any] | _AsyncTopic[Any] | SyncTopicBase[Any]
    ],
    *,
    topic_prefixes: Sequence[str],
    default_type: Any,
    offset: int | End | None,
    last: int | None,
    wait_new_records: bool,
    create_wait: timedelta | None,
) -> ReadSpec:
    """The Topic names of a read, the decode type of each known Topic, and its options.

    Raises:
        ValueError: ``last`` is not above zero, or the call also gives ``offset``.
    """
    if last is not None and (last < 1 or offset is not None):
        raise ValueError("last takes a count above zero and no offset")
    types = handle._known_types()
    names = []
    for topic in topics:
        if isinstance(topic, (_AsyncTopic, SyncTopicBase)):
            name, topic_type = topic.name, topic.type
        elif isinstance(topic, TopicDefinition):
            name, topic_type = handle._topic_spec(topic, None)
        else:
            # A Topic name that the definition doesn't have gets no type here, so its values
            # decode with the read's default type.
            name, topic_type = topic, types.get(topic)
        names.append(name)
        if topic_type is not None:
            types[name] = topic_type
    return ReadSpec(
        topics=names or None,
        topic_prefixes=topic_prefixes,
        types=types,
        default_type=default_type,
        offset=offset,
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
    # A read from END or of the last Records has no resume offset until its first page.
    if read.last is not None or read.offset is End.END:
        position = Position(None)
    else:
        position = Position(read.offset or 0)
    return Subscription(items(position), position, options)


class _AsyncTopic(TopicBase[T]):
    # A Topic of an Activity or client handle. Each publish writes before it returns.

    async def publish(self, *values: T) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value.
        """
        return await self._stream._access().publish(self._name, values)


class SyncTopicBase(TopicBase[T]):
    """Base class of a Topic whose calls block, for sync Activities."""
