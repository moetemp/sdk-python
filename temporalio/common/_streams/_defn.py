"""Stream definitions, which give the Stream name and its one payload type."""

from __future__ import annotations

import typing
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, overload

T = TypeVar("T")
ClassT = TypeVar("ClassT", bound=type)

_DEFINITION_ATTR = "__temporal_stream_definition"


class StreamDefinition(Generic[T]):
    """Base class of a Stream definition. Its type argument is the Stream's payload type.

    Mark a subclass with :py:func:`defn`::

        @common.defn(name="turns")
        class Turns(common.StreamDefinition[Turn]): ...

    A definition doesn't name a backend, so Workflows, Activities and clients can share it.
    Separate channels, such as turns and token deltas, are separate Streams. Use ``RawValue`` as
    the payload type to read each Record's Payload with no conversion.

    .. warning::
        This API is experimental.
    """


@dataclass(frozen=True)
class DefinitionInfo:
    """What :py:func:`defn` takes from a definition class."""

    name: str
    """Name of a Stream of this definition."""

    type: Any
    """The payload type, which a read decodes each Record's value with."""


@overload
def defn(cls: ClassT) -> ClassT: ...


@overload
def defn(*, name: str | None = None) -> Callable[[ClassT], ClassT]: ...


def defn(
    cls: ClassT | None = None, *, name: str | None = None
) -> ClassT | Callable[[ClassT], ClassT]:
    """Mark a subclass of :py:class:`StreamDefinition` as a Stream definition.

    Args:
        cls: The class.
        name: The name of a Stream of this definition. Defaults to the class name.

    Raises:
        TypeError: The class doesn't subclass ``StreamDefinition`` with a payload type.

    .. warning::
        This API is experimental.
    """

    def decorate(cls: ClassT) -> ClassT:
        info = DefinitionInfo(name=name or cls.__name__, type=_payload_type(cls))
        setattr(cls, _DEFINITION_ATTR, info)
        return cls

    if cls is not None:
        return decorate(cls)
    return decorate


def _payload_type(cls: type) -> Any:
    if not isinstance(cls, type) or not issubclass(cls, StreamDefinition):
        raise TypeError(
            f"{cls!r} must subclass StreamDefinition with its payload type, such as "
            "StreamDefinition[Turn]"
        )
    for klass in cls.__mro__:
        for base in klass.__dict__.get("__orig_bases__", ()):
            if typing.get_origin(base) is StreamDefinition:
                [payload] = typing.get_args(base)
                if not isinstance(payload, TypeVar):
                    return payload
    raise TypeError(
        f"{cls.__qualname__} gives StreamDefinition no payload type, such as "
        "StreamDefinition[Turn]"
    )


def definition_of(cls: type) -> DefinitionInfo:
    """The definition that :py:func:`defn` gave a class.

    Raises:
        TypeError: The class has no ``@common.defn``.
    """
    definition = cls.__dict__.get(_DEFINITION_ATTR) if isinstance(cls, type) else None
    if not isinstance(definition, DefinitionInfo):
        raise TypeError(
            f"{cls!r} is not a Stream definition. Add @temporalio.common.defn."
        )
    return definition
