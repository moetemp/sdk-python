"""Stream definitions: the Stream name and the type of each Topic."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Generic, TypeVar, cast, overload

T = TypeVar("T")
ClassT = TypeVar("ClassT", bound=type)

_DEFINITION_ATTR = "__temporal_stream_definition"


class TopicDefinition(Generic[T]):
    """One Topic of a Stream definition. Use :py:func:`topic` in the definition class.

    .. warning::
        This API is experimental.
    """

    def __init__(self, type: Any, name: str | None = None) -> None:
        """Create a Topic definition. Use :py:func:`topic` instead."""
        self._type = type
        self._name = name
        self._owner: type | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        """Take the attribute name as the Topic name, unless the Topic has a name."""
        if self._name is None:
            self._name = name
        self._owner = owner

    @property
    def name(self) -> str:
        """Name of the Topic."""
        if self._name is None:
            raise TypeError(
                "A Topic definition must be an attribute of a definition class"
            )
        return self._name

    @property
    def type(self) -> Any:
        """Type that a read decodes the Topic's values with."""
        return self._type

    def __repr__(self) -> str:
        """Show the Topic name and type."""
        return f"TopicDefinition(name={self._name!r}, type={self._type!r})"


@overload
def topic(type: type[T], *, name: str | None = None) -> TopicDefinition[T]: ...


@overload
def topic(type: Any, *, name: str | None = None) -> TopicDefinition[Any]: ...


def topic(type: Any, *, name: str | None = None) -> TopicDefinition[Any]:
    """Declare a Topic in a Stream definition class.

    ``RawValue`` keeps each Record's Payload.

    Args:
        type: The type of the Topic's values.
        name: The Topic name. Defaults to the attribute name.

    .. warning::
        This API is experimental.
    """
    return TopicDefinition(type, name)


@dataclass(frozen=True)
class StreamDefinition:
    """What :py:func:`defn` takes from a definition class.

    .. warning::
        This API is experimental.
    """

    name: str
    """Name of a Stream of this definition."""

    topics: Mapping[str, TopicDefinition[Any]]
    """The Topics by name."""


@overload
def defn(cls: ClassT) -> ClassT: ...


@overload
def defn(*, name: str | None = None) -> Callable[[ClassT], ClassT]: ...


def defn(
    cls: ClassT | None = None, *, name: str | None = None
) -> ClassT | Callable[[ClassT], ClassT]:
    """Mark a class as a Stream definition. Its :py:func:`topic` attributes are the Topics.

    A definition has no backend in it, so Workflows, Activities and clients share it.

    Args:
        cls: The class.
        name: The name of a Stream of this definition. Defaults to the class name. Use
            :py:func:`with_name` for another Stream of the same Topics.

    .. warning::
        This API is experimental.
    """

    def decorate(cls: ClassT) -> ClassT:
        # A subclass attribute replaces the base class attribute of the same name.
        by_attribute: dict[str, TopicDefinition[Any]] = {}
        for klass in reversed(cls.__mro__):
            for attribute, value in vars(klass).items():
                if isinstance(value, TopicDefinition):
                    by_attribute[attribute] = value
        topics: dict[str, TopicDefinition[Any]] = {}
        for value in by_attribute.values():
            if value.name in topics:
                raise ValueError(
                    f"The Stream definition {cls.__qualname__} has two Topics "
                    f"named {value.name}"
                )
            topics[value.name] = value
        setattr(
            cls,
            _DEFINITION_ATTR,
            StreamDefinition(name=name or cls.__name__, topics=topics),
        )
        return cls

    if cls is not None:
        return decorate(cls)
    return decorate


def with_name(definition: ClassT, name: str) -> ClassT:
    """The definition with the same Topics and another Stream name, such as one per turn.

    It is a subclass of ``definition``, which stays unchanged. Its Topics are the same
    attributes, so ``Events.tokens`` is a Topic of ``with_name(Events, "turn-3")`` too.

    Args:
        definition: The definition class.
        name: The Stream name.

    .. warning::
        This API is experimental.
    """
    renamed = replace(definition_of(definition), name=name)
    return cast(
        ClassT,
        type(
            definition.__name__,
            (definition,),
            {
                _DEFINITION_ATTR: renamed,
                "__module__": definition.__module__,
                "__qualname__": definition.__qualname__,
            },
        ),
    )


def definition_of(cls: type) -> StreamDefinition:
    """The definition that :py:func:`defn` gave a class.

    Raises:
        TypeError: The class has no ``@common.defn``.
    """
    definition = cls.__dict__.get(_DEFINITION_ATTR) if isinstance(cls, type) else None
    if not isinstance(definition, StreamDefinition):
        raise TypeError(
            f"{cls!r} is not a Stream definition. Add @temporalio.common.defn."
        )
    return definition
