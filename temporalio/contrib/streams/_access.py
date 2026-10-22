"""Stream calls outside Workflow code, which Activity and client handles share."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar

import temporalio.api.streamservice.v1 as stream_pb
import temporalio.converter
from temporalio.contrib.streams import _convert, _reader
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.contrib.streams._defn import TopicDefinition
from temporalio.contrib.streams._handle import StreamHandle
from temporalio.contrib.streams._reader import Item
from temporalio.contrib.streams._writer import StreamWriter

T = TypeVar("T")


@dataclass
class Access:
    """The calls of one Stream handle, bound to a backend."""

    backend: StreamBackend
    converter: temporalio.converter.DataConverter
    namespace: str
    stream_id: Callable[[], Awaitable[str]]
    writer: Callable[[], StreamWriter]

    async def publish(self, topic: str, values: Sequence[Any]) -> int:
        """Write the values as Records now, and return the offset of the last one."""
        if not values:
            raise TypeError("publish takes one or more values")
        writer = self.writer()
        for value in values:
            writer.publish(topic, value, False)
        return await writer.flush()

    def read(
        self,
        *,
        topics: list[str] | None,
        prefixes: Sequence[str],
        types: dict[str, Any],
        from_offset: int | None,
        wait_new_records: bool,
        create_wait: timedelta | None,
    ) -> AsyncGenerator[Item, None]:
        """Read the Stream's items in order."""
        return self._read(
            topics=topics,
            prefixes=prefixes,
            types=types,
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )

    async def _read(
        self,
        *,
        topics: list[str] | None,
        prefixes: Sequence[str],
        types: dict[str, Any],
        from_offset: int | None,
        wait_new_records: bool,
        create_wait: timedelta | None,
    ) -> AsyncGenerator[Item, None]:
        items = _reader.read(
            self.backend.service(self.namespace),
            self.converter,
            await self.stream_id(),
            topics=topics,
            topic_prefixes=prefixes,
            types=types,
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )
        async for item in items:
            yield item

    async def get_offset(self) -> int:
        """The next offset of the Stream, the newest offset plus one."""
        response = await self.backend.call_async(
            "DescribeStream",
            stream_pb.DescribeStreamRequest(
                namespace=self.namespace,
                stream=stream_pb.StreamRef(id=await self.stream_id()),
            ),
            stream_pb.DescribeStreamResponse,
        )
        # Offsets start above zero, so zero means the Stream has no Record.
        return response.last_offset + 1 if response.last_offset else 0

    async def close(self, result: Any | None, failure: BaseException | None) -> None:
        """Write this Producer's Records, then close the Stream."""
        writer = self.writer()
        await writer.close()
        request = stream_pb.CloseStreamRequest(
            namespace=self.namespace,
            stream=stream_pb.StreamRef(id=await self.stream_id()),
            producer=writer.producer,
        )
        if result is not None:
            request.result.CopyFrom(
                (await _convert.encode(self.converter, result)).payload
            )
        if failure is not None:
            await self.converter.encode_failure(failure, request.failure)
        await self.backend.call_async(
            "CloseStream", request, stream_pb.CloseStreamResponse
        )


def read_spec(
    handle: StreamHandle[Any],
    topics: Sequence[
        str | TopicDefinition[Any] | _AsyncTopic[Any] | SyncTopicBase[Any]
    ],
) -> tuple[list[str] | None, dict[str, Any]]:
    """The Topic names of a read and the decode type of each known Topic."""
    types = handle._known_types()
    names = []
    for topic in topics:
        if isinstance(topic, (_AsyncTopic, SyncTopicBase)):
            name, topic_type = topic.name, topic.type
        elif isinstance(topic, TopicDefinition):
            name, topic_type = handle._topic_spec(topic, None)
        else:
            # A read names a Topic outside the definition with no type, and its values
            # decode as the converter gives them.
            name, topic_type = topic, types.get(topic)
        names.append(name)
        if topic_type is not None:
            types[name] = topic_type
    return names or None, types


class _AsyncTopic(Generic[T]):
    # A Topic of an Activity or client handle. Each publish writes before it returns.
    def __init__(self, stream: Any, name: str, type: Any) -> None:
        self._stream = stream
        self._name = name
        self._type = type

    @property
    def name(self) -> str:
        """Name of the Topic."""
        return self._name

    @property
    def type(self) -> Any:
        """Type that a read decodes the Topic's values with."""
        return self._type

    async def publish(self, *values: T) -> int:
        """Write one Record for each value, in order, and return the last Record's offset.

        A list is one value.
        """
        return await self._stream._access().publish(self._name, values)

    def subscribe(
        self,
        *,
        from_offset: int | None = None,
        wait_new_records: bool = True,
        create_wait: timedelta | None = None,
    ) -> AsyncGenerator[Item, None]:
        """Read this Topic only. See the Stream's ``subscribe``."""
        return self._stream.subscribe(
            self,
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            create_wait=create_wait,
        )


class SyncTopicBase(Generic[T]):
    """Base of a Topic whose calls block, for sync Activities."""

    def __init__(self, stream: Any, name: str, type: Any) -> None:
        """Create a Topic. Use the Stream's ``topic`` instead."""
        self._stream = stream
        self._name = name
        self._type = type

    @property
    def name(self) -> str:
        """Name of the Topic."""
        return self._name

    @property
    def type(self) -> Any:
        """Type that a read decodes the Topic's values with."""
        return self._type
