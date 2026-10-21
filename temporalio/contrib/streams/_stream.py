"""Stream references and Topics."""

from __future__ import annotations

import abc
from collections.abc import AsyncGenerator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from temporalio.contrib.streams._reader import Item

T = TypeVar("T")


@dataclass(frozen=True)
class StreamRef:
    """Names one Stream. Pass it to Workflows and Activities as an argument.

    .. warning::
        This API is experimental.
    """

    id: str
    """The Stream id. It is opaque to callers."""

    workflow_id: str | None = None
    """For a Workflow's Stream, the id of the Workflow that owns it. None for a standalone
    Stream."""

    first_execution_run_id: str | None = None
    """For a Workflow's Stream, the first run id of the owning run chain. None for a
    standalone Stream."""


class _Writable(abc.ABC):
    # A Stream kind that Topics publish to.
    @abc.abstractmethod
    def _publish(self, topic: str, value: Any, force_flush: bool) -> None: ...


class _Readable(_Writable):
    # A Stream kind that Topics read from: Activities and clients.
    @abc.abstractmethod
    def subscribe(
        self,
        *,
        topics: Sequence[Topic[Any]] | None = None,
        topic_prefixes: Sequence[str] = (),
        from_offset: int | None = None,
        wait_new_records: bool = True,
        idle_timeout: timedelta | None = None,
        type: Any = None,
    ) -> AsyncGenerator[Item, None]: ...


class Topic(Generic[T]):
    """One Topic of a Stream. All Topics of a Stream share one order.

    .. warning::
        This API is experimental.
    """

    def __init__(self, stream: _Writable, name: str, type: Any) -> None:
        """Create a Topic. Use ``topic()`` of a Stream instead."""
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

    def publish(self, value: T, *, force_flush: bool = False) -> None:
        """Add a Record to the Topic. It returns at once and the Record is buffered.

        Args:
            value: The value. It goes through the payload converter and codec.
            force_flush: Outside Workflow code, start a flush at once. Workflow code writes
                its Records when the Workflow Task completes, so it has no effect there.
        """
        self._stream._publish(self._name, value, force_flush)

    def subscribe(
        self,
        *,
        from_offset: int | None = None,
        wait_new_records: bool = True,
        idle_timeout: timedelta | None = None,
    ) -> AsyncGenerator[Item, None]:
        """Read this Topic only. See the Stream's ``subscribe``."""
        if not isinstance(self._stream, _Readable):
            raise TypeError("Workflow code can't read a Stream")
        return self._stream.subscribe(
            topics=[self],
            from_offset=from_offset,
            wait_new_records=wait_new_records,
            idle_timeout=idle_timeout,
        )
