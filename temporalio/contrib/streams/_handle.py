"""The shared part of the Stream handles of each context."""

from __future__ import annotations

import asyncio
import typing
from collections.abc import Callable, Coroutine, Generator
from types import TracebackType
from typing import Any, Generic, Protocol, TypeVar

from typing_extensions import Self

import temporalio.converter
import temporalio.exceptions
from temporalio.contrib.streams import _errors
from temporalio.contrib.streams._defn import TopicDefinition, definition_of
from temporalio.contrib.streams.system import models

E = TypeVar("E")
T = TypeVar("T")


def exit_failure(exc: BaseException | None) -> tuple[bool, BaseException | None]:
    """Whether the end of an ``async with`` block closes the Stream, and with which failure.

    A cancel closes it with a ``CancelledError`` failure. Another ``BaseException``, such as
    an eviction, leaves it open, since the block did not end in the caller's code.
    """
    if isinstance(exc, asyncio.CancelledError):
        return True, temporalio.exceptions.CancelledError(str(exc) or "Cancelled")
    if exc is None or isinstance(exc, Exception):
        return True, exc
    return False, None


class _ClosesOnExit(Protocol):
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


S = TypeVar("S", bound=_ClosesOnExit)


class Creating(Generic[S]):
    """A Stream create. Await it for the handle, or use it in ``async with``, which closes the
    Stream at the end of the block.

    The create starts when it is awaited or entered, not before.

    .. warning::
        This API is experimental.
    """

    def __init__(self, create: Callable[[], Coroutine[Any, Any, S]]) -> None:
        """Create it. Use a context's create call instead."""
        self._create = create
        self._stream: S | None = None

    def __await__(self) -> Generator[Any, None, S]:
        """Create the Stream and give its handle."""
        return self._create().__await__()

    async def __aenter__(self) -> S:
        """Create the Stream and give its handle for the block."""
        self._stream = await self._create()
        return self._stream

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the Stream as the handle's own ``async with`` does."""
        assert self._stream is not None
        await self._stream.__aexit__(exc_type, exc, tb)


class TopicBase(Generic[T]):
    """Base of the Topic of each handle class. All Topics of a Stream share one order."""

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


H = TypeVar("H", bound="StreamHandle[Any]")


def _to_json(handle: StreamHandle[Any]) -> dict[str, str]:
    # The proto JSON of temporal.api.streamservice.v1.StreamRef, with empty fields left out as
    # MessageToJson does, so every SDK reads a handle that another wrote.
    json = {"id": handle.id}
    if handle.workflow_id:
        json["workflowId"] = handle.workflow_id
    if handle.first_execution_run_id:
        json["firstExecutionRunId"] = handle.first_execution_run_id
    if handle.backend:
        json["backend"] = handle.backend
    if handle.activity_id:
        json["activityId"] = handle.activity_id
    if handle.activity_run_id:
        json["activityRunId"] = handle.activity_run_id
    return json


def _from_json(value: Any, type_hint: Any) -> StreamHandle[Any]:
    origin = typing.get_origin(type_hint) or type_hint
    if not isinstance(origin, type) or not issubclass(origin, StreamHandle):
        raise TypeError(f"{type_hint!r} is not a Stream handle type")
    if isinstance(value, origin):
        return value
    if isinstance(value, StreamHandle):
        value = _to_json(value)
    if not isinstance(value, dict) or not isinstance(value.get("id"), str):
        raise TypeError(f"Expected the JSON object of a Stream, got {type(value)}")
    if origin is StreamHandle:
        raise TypeError(
            "Give a Stream parameter the handle type of its context, such as "
            "ActivityStream[Events]"
        )
    args = typing.get_args(type_hint)
    # Any is a class from Python 3.11, and a hint such as ActivityStream[Any] has no definition.
    definition = (
        args[0] if args and args[0] is not Any and isinstance(args[0], type) else None
    )
    return origin(
        id=value["id"],
        workflow_id=value.get("workflowId") or None,
        first_execution_run_id=value.get("firstExecutionRunId") or None,
        backend=value.get("backend") or None,
        definition=definition,
        activity_id=value.get("activityId") or None,
        activity_run_id=value.get("activityRunId") or None,
    )


class _StreamHandleJson(
    temporalio.converter.TransferTypeConverter["StreamHandle[Any]", dict[str, str]]
):
    # One form for every handle type, and the parameter's type hint picks the class, so a
    # Workflow passes a Stream to an Activity as data.
    transfer_type: type[dict[str, str]] | None = dict

    def to_transfer_type(self, value: StreamHandle[Any]) -> dict[str, str]:
        return _to_json(value)

    def from_transfer_type(
        self, value: dict[str, str], type_hint: type[StreamHandle[Any]]
    ) -> StreamHandle[Any]:
        return _from_json(value, type_hint)


class StreamHandle(Generic[E]):
    """Base of the Stream handle of each context.

    A handle is data: it passes as a Workflow or Activity argument, as the proto JSON of the
    ``StreamRef`` message. The parameter's type, such as ``ActivityStream[Events]``, gives the
    handle class and the definition.

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
        activity_id: str | None = None,
        activity_run_id: str | None = None,
    ) -> None:
        """Create a handle. Use the create and get calls of a context instead."""
        self._id = id
        self._workflow_id = workflow_id
        self._first_execution_run_id = first_execution_run_id
        self._activity_id = activity_id
        self._activity_run_id = activity_run_id
        self._backend = backend
        self._definition = definition
        if definition is not None:
            definition_of(definition)

    @classmethod
    def from_handle(cls: type[H], handle: StreamHandle[Any]) -> H:
        """The same Stream as a handle of this class, such as an ``ActivityStream`` from a
        ``WorkflowStream``.

        A handle passes as data, so a Workflow uses it to give an Activity's input the type
        that the Activity declares.
        """
        return cls(
            id=handle.id,
            workflow_id=handle.workflow_id,
            first_execution_run_id=handle.first_execution_run_id,
            backend=handle.backend,
            definition=handle.definition,
            activity_id=handle.activity_id,
            activity_run_id=handle.activity_run_id,
        )

    @property
    def id(self) -> str:
        """The Stream id. It is opaque to callers."""
        return self._id

    @property
    def workflow_id(self) -> str | None:
        """For a Workflow's Stream, the id of the Workflow that owns it."""
        return self._workflow_id

    @property
    def first_execution_run_id(self) -> str | None:
        """For a Workflow's Stream, the first run id of the owning run chain."""
        return self._first_execution_run_id

    @property
    def activity_id(self) -> str | None:
        """For a Standalone Activity's Stream, the id of the Activity that owns it."""
        return self._activity_id

    @property
    def activity_run_id(self) -> str | None:
        """For a Standalone Activity's Stream, the run id of the owning Activity."""
        return self._activity_run_id

    @property
    def backend(self) -> str | None:
        """Name of the backend that keeps the Stream. None means the default one."""
        return self._backend

    @property
    def definition(self) -> type[E] | None:
        """The Stream definition class, or None for a Stream with no definition."""
        return self._definition

    def _topic_spec(
        self, topic: str | TopicDefinition[Any], type: Any
    ) -> tuple[str, Any]:
        # The name and the decode type of a Topic. A Topic of a definition keeps its type,
        # and a name outside the definition needs a type, so a typo does not pass.
        definition = definition_of(self._definition) if self._definition else None
        if isinstance(topic, TopicDefinition):
            if type is not None:
                raise TypeError("A Topic of a definition already has its type")
            if (
                definition is not None
                and definition.topics.get(topic.name) is not topic
            ):
                raise _errors.StreamError(
                    f"The Stream definition {definition.name} has no Topic {topic.name}",
                    reason="unknown_topic",
                )
            return topic.name, topic.type
        if definition is not None:
            known = definition.topics.get(topic)
            if known is not None:
                if type is not None and type is not known.type:
                    raise TypeError(
                        f"The Topic {topic} of {definition.name} has the type "
                        f"{known.type!r}"
                    )
                return topic, known.type
            if type is None:
                raise _errors.StreamError(
                    f"The Stream definition {definition.name} has no Topic {topic}. "
                    "Pass type= for a Topic outside the definition.",
                    reason="unknown_topic",
                )
        return topic, type

    def _owner_ref(self) -> models.StreamRef:
        # A Workflow's or a Standalone Activity's Stream carries its owner on every call, so a
        # server that nests it in its owner finds it.
        return models.StreamRef(
            id=self._id,
            workflow_id=self._workflow_id or None,
            first_execution_run_id=self._first_execution_run_id or None,
            activity_id=self._activity_id or None,
            activity_run_id=self._activity_run_id or None,
        )

    def _known_types(self) -> dict[str, Any]:
        # A read of every Topic still decodes each Topic of the definition with its type.
        if self._definition is None:
            return {}
        topics = definition_of(self._definition).topics
        return {name: topic.type for name, topic in topics.items()}

    async def _ready(self) -> None:
        # A handle that must look up its backend first does it here.
        pass

    def _fields(self) -> dict[str, str]:
        # The fields that name the Stream. They need no call, so a handle whose id is not known
        # yet still compares, hashes and prints.
        fields = {"id": self._id}
        if self._workflow_id:
            fields["workflowId"] = self._workflow_id
        if self._first_execution_run_id:
            fields["firstExecutionRunId"] = self._first_execution_run_id
        if self._backend:
            fields["backend"] = self._backend
        if self._activity_id:
            fields["activityId"] = self._activity_id
        if self._activity_run_id:
            fields["activityRunId"] = self._activity_run_id
        return fields

    def __eq__(self, other: object) -> bool:
        """Handles of one class are equal when they name the same Stream."""
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, StreamHandle)
        return self._fields() == other._fields()

    def __hash__(self) -> int:
        """Hash of the fields that never change, so it stays when a client learns the id."""
        return hash((type(self), self._backend, self._workflow_id or self._id))

    def __repr__(self) -> str:
        """Show the Stream fields."""
        fields = ", ".join(f"{key}={value!r}" for key, value in self._fields().items())
        return f"{type(self).__name__}({fields})"

    def __copy__(self) -> Self:
        """A copy is a handle of the same Stream in the same context."""
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        """A deep copy is the same handle, since dataclasses.asdict copies its fields."""
        return self

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> Any:
        """Give pydantic models the proto JSON form, as the payload converter does.

        Pydantic is optional, so it is imported only when a model uses a handle.
        """
        from pydantic_core import core_schema

        return core_schema.no_info_plain_validator_function(
            lambda value: _from_json(value, source),
            json_schema_input_schema=core_schema.typed_dict_schema(
                {
                    "id": core_schema.typed_dict_field(core_schema.str_schema()),
                    **{
                        key: core_schema.typed_dict_field(
                            core_schema.str_schema(), required=False
                        )
                        for key in ("workflowId", "firstExecutionRunId", "backend")
                    },
                }
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(_to_json),
        )


# Registered as a call, not a decorator, so type checkers keep the class generic.
temporalio.converter.transfer_type_convertible(_StreamHandleJson)(StreamHandle)
