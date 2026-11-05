"""The Stream ref, the data that identifies one Stream."""

from __future__ import annotations

import typing
from datetime import timedelta
from typing import Any, Generic, TypeVar

import google.protobuf.json_format
from typing_extensions import Self, TypedDict

import temporalio.api.stream.v1 as stream_v1
import temporalio.converter
from temporalio.streams.system import models

T = TypeVar("T")


class StreamOptions(TypedDict, total=False):
    """The options of a new Stream. ``create_or_get_stream`` ignores them on a Stream that exists.

    .. warning::
        This API is experimental.
    """

    retention: timedelta
    """How long a Record stays readable, even while the Stream is open.

    A closed Stream is kept for one Retention after its close. Defaults to the backend's
    Retention.
    """


def _model(ref: StreamRef[Any]) -> models.StreamRef:
    return models.StreamRef(id=ref.id, owner=ref.owner, backend=ref.backend)


def to_proto(ref: StreamRef[Any]) -> stream_v1.StreamRef:
    """The ``temporal.api.stream.v1.StreamRef`` message of a ref."""
    return models._StreamRefTransferTypeConverter().to_transfer_type(_model(ref))


def _to_json(ref: StreamRef[Any]) -> dict[str, Any]:
    # The proto JSON of temporal.api.stream.v1.StreamRef, as MessageToJson writes it, so every SDK
    # reads a ref that another wrote.
    return google.protobuf.json_format.MessageToDict(to_proto(ref))


def _from_json(value: Any, type_hint: Any) -> StreamRef[Any]:
    origin = typing.get_origin(type_hint) or type_hint
    if not isinstance(origin, type) or not issubclass(origin, StreamRef):
        raise TypeError(f"{type_hint!r} is not a Stream ref type")
    if isinstance(value, origin):
        return value
    if not isinstance(value, dict) or not isinstance(value.get("id"), str):
        raise TypeError(f"Expected the JSON object of a Stream ref, got {type(value)}")
    message = google.protobuf.json_format.ParseDict(
        value, stream_v1.StreamRef(), ignore_unknown_fields=True
    )
    model = models._StreamRefTransferTypeConverter().from_transfer_type(
        message, models.StreamRef
    )
    # A ref is data, so it keeps any owner, such as a Standalone Activity's. A close checks it.
    owner = model.owner if model.owner != models.Execution() else None
    return origin(id=model.id, owner=owner, backend=model.backend)


class _StreamRefJson(
    temporalio.converter.TransferTypeConverter["StreamRef[Any]", dict[str, Any]]
):
    # The JSON converter writes a nested dataclass as its fields, so the ref isn't the generated
    # dataclass. It goes through the generated converter, as a dict so a payload stays json/plain.
    transfer_type: type[dict[str, Any]] | None = dict

    def to_transfer_type(self, value: StreamRef[Any]) -> dict[str, Any]:
        return _to_json(value)

    def from_transfer_type(
        self, value: dict[str, Any], type_hint: type[StreamRef[Any]]
    ) -> StreamRef[Any]:
        return _from_json(value, type_hint)


class StreamRef(Generic[T]):
    """The data that identifies one Stream. ``T`` is the payload type, for type checking only.

    A ref has the fields of the wire model of the ``temporal.api.stream.v1.StreamRef`` message.
    Code passes it as a Workflow or Activity argument, a query result, a Record, or a field of a
    dataclass or a pydantic model, in the proto JSON form of that message.

    A ref has no calls. To make calls, Workflow code gets a Stream with
    ``workflow.to_stream(ref)``, and other code with ``client.to_stream(ref)``.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        id: str,
        owner: models.Execution | None = None,
        backend: str | None = None,
    ) -> None:
        """Create a ref. Code gets one from a Stream's ``ref`` instead."""
        self._id = id
        self._owner = owner
        self._backend = backend

    @property
    def id(self) -> str:
        """The Stream id. It is opaque to callers."""
        return self._id

    @property
    def owner(self) -> models.Execution | None:
        """The owner of a bound Stream, such as a Workflow run chain. None for a standalone one."""
        return self._owner

    @property
    def backend(self) -> str | None:
        """Name of the backend that stores the Stream. None means the default one."""
        return self._backend

    def __eq__(self, other: object) -> bool:
        """Refs of one class are equal when their fields are equal."""
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, StreamRef)
        return (self._id, self._owner, self._backend) == (
            other._id,
            other._owner,
            other._backend,
        )

    def __hash__(self) -> int:
        """Hash of the id and the backend, which identify the Stream."""
        return hash((type(self), self._id, self._backend))

    def __repr__(self) -> str:
        """Show the fields."""
        return (
            f"{type(self).__name__}(id={self._id!r}, owner={self._owner!r}, "
            f"backend={self._backend!r})"
        )

    def __copy__(self) -> Self:
        """A ref doesn't change, so a copy is the same ref."""
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        """Return the same ref, since dataclasses.asdict deep-copies a ref field."""
        return self

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> Any:
        """Give pydantic models the proto JSON form, as the payload converter does.

        Pydantic is optional, so it is imported only when a model uses a ref.
        """
        from pydantic_core import core_schema

        return core_schema.no_info_plain_validator_function(
            lambda value: _from_json(value, source),
            json_schema_input_schema=core_schema.typed_dict_schema(
                {
                    "id": core_schema.typed_dict_field(core_schema.str_schema()),
                    "owner": core_schema.typed_dict_field(
                        core_schema.typed_dict_schema(
                            {
                                key: core_schema.typed_dict_field(
                                    core_schema.str_schema(), required=False
                                )
                                for key in ("type", "businessId", "runId")
                            }
                        ),
                        required=False,
                    ),
                    "backend": core_schema.typed_dict_field(
                        core_schema.str_schema(), required=False
                    ),
                }
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(_to_json),
        )


# Registered as a call, not a decorator, so type checkers keep the class generic.
temporalio.converter.transfer_type_convertible(_StreamRefJson)(StreamRef)
