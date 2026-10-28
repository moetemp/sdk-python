"""The shared part of the Stream handles of each context."""

from __future__ import annotations

import asyncio
import typing
from collections.abc import Callable, Coroutine, Generator
from datetime import timedelta
from types import TracebackType
from typing import Any, Generic, Protocol, TypeVar

import google.protobuf.json_format
from typing_extensions import Self, TypedDict

import temporalio.converter
import temporalio.exceptions
from temporalio.common._streams.system import models

T = TypeVar("T")


def exit_failure(exc: BaseException | None) -> tuple[bool, BaseException | None]:
    """Whether the end of an ``async with`` block closes the Stream, and with which failure.

    A cancel closes it with a ``CancelledError`` failure. Another ``BaseException``, such as
    ``KeyboardInterrupt``, leaves it open, since the caller's code didn't end the block.
    """
    if isinstance(exc, asyncio.CancelledError):
        return True, temporalio.exceptions.CancelledError(str(exc) or "Cancelled")
    if exc is None or isinstance(exc, Exception):
        return True, exc
    return False, None


class StreamOptions(TypedDict, total=False):
    """The options of a Stream create. A create of a Stream that exists ignores them.

    .. warning::
        This API is experimental.
    """

    retention: timedelta
    """The age after which a Record is deleted, even while the Stream is open.

    A closed Stream is kept for one Retention after its close. Defaults to the backend's
    Retention.
    """


class _ClosesOnExit(Protocol):
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


S = TypeVar("S", bound=_ClosesOnExit)


class Creating(Generic[S]):
    """A pending Stream create.

    Await it to get the handle, or use it in ``async with``, which closes the Stream at the end
    of the block. The create starts when it's awaited or entered, not before.

    .. warning::
        This API is experimental.
    """

    def __init__(self, create: Callable[[], Coroutine[Any, Any, S]]) -> None:
        """Create the object. Use a context's create function instead."""
        self._create = create
        self._stream: S | None = None

    def __await__(self) -> Generator[Any, None, S]:
        """Create the Stream and return its handle."""
        return self._create().__await__()

    async def __aenter__(self) -> S:
        """Create the Stream and return its handle for the block."""
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


H = TypeVar("H", bound="StreamHandle[Any]")


# The owner type of a Workflow's Stream in proto JSON. A client's Stream has no owner.
_WORKFLOW_OWNER = "EXECUTION_TYPE_WORKFLOW"


def workflow_owner(
    workflow_id: str | None, first_execution_run_id: str | None
) -> models.Execution | None:
    """The owner of a Workflow's Stream in a ref. A client's Stream has no owner."""
    if not workflow_id and not first_execution_run_id:
        return None
    return models.Execution(
        type=models.ExecutionType.ExecutionTypeWorkflow,
        business_id=workflow_id or None,
        run_id=first_execution_run_id or None,
    )


def _to_json(handle: StreamHandle[Any]) -> dict[str, Any]:
    # The proto JSON of temporal.api.stream.v1.StreamRef, as MessageToJson writes it, so every
    # SDK reads a handle that another wrote.
    ref = models.StreamRef(
        id=handle.id,
        owner=workflow_owner(handle.workflow_id, handle.first_execution_run_id),
        backend=handle.backend or None,
    )
    return google.protobuf.json_format.MessageToDict(
        models._StreamRefTransferTypeConverter().to_transfer_type(ref)
    )


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
            "ActivityStream[Turn]"
        )
    owner = value.get("owner") or {}
    if not isinstance(owner, dict) or (
        owner and owner.get("type") not in (_WORKFLOW_OWNER, 1)
    ):
        raise TypeError("A Stream handle's owner must be a Workflow run chain")
    args = typing.get_args(type_hint)
    # A hint such as ActivityStream[Any] gives no payload type, so values decode with no hint.
    payload_type = (
        args[0]
        if args and args[0] is not Any and not isinstance(args[0], TypeVar)
        else None
    )
    return origin(
        id=value["id"],
        workflow_id=owner.get("businessId") or None,
        first_execution_run_id=owner.get("runId") or None,
        backend=value.get("backend") or None,
        payload_type=payload_type,
    )


class _StreamHandleJson(
    temporalio.converter.TransferTypeConverter["StreamHandle[Any]", dict[str, str]]
):
    # Every handle type has the same JSON form, and the parameter's type hint picks the class.
    # So a Workflow can pass a Stream to an Activity as data.
    transfer_type: type[dict[str, str]] | None = dict

    def to_transfer_type(self, value: StreamHandle[Any]) -> dict[str, str]:
        return _to_json(value)

    def from_transfer_type(
        self, value: dict[str, str], type_hint: type[StreamHandle[Any]]
    ) -> StreamHandle[Any]:
        return _from_json(value, type_hint)


class StreamHandle(Generic[T]):
    """Base class of the Stream handle of each context. ``T`` is the Stream's payload type.

    A handle is data. It's passed as a Workflow or Activity argument in the proto JSON form of
    the ``StreamRef`` message. The parameter's type, such as ``ActivityStream[Turn]``, sets the
    handle class and the payload type.

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
        payload_type: Any = None,
    ) -> None:
        """Create a handle. Use a context's create and get functions instead."""
        self._id = id
        self._workflow_id = workflow_id
        self._first_execution_run_id = first_execution_run_id
        self._backend = backend
        # The type that a read decodes each value with. None decodes with no type hint.
        self._payload_type = payload_type

    @classmethod
    def from_handle(cls: type[H], handle: StreamHandle[Any]) -> H:
        """Return the same Stream as a handle of this class.

        For example, a Workflow converts a ``WorkflowStream`` to an ``ActivityStream`` for an
        Activity's input, so the input has the type that the Activity declares.
        """
        return cls(
            id=handle.id,
            workflow_id=handle.workflow_id,
            first_execution_run_id=handle.first_execution_run_id,
            backend=handle.backend,
            payload_type=handle._payload_type,
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
    def backend(self) -> str | None:
        """Name of the backend that stores the Stream. None means the default one."""
        return self._backend

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
        return fields

    def __eq__(self, other: object) -> bool:
        """Handles of one class are equal when they name the same Stream."""
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, StreamHandle)
        return self._fields() == other._fields()

    def __hash__(self) -> int:
        """Hash of the fields that never change, so it's the same after a client learns the id."""
        return hash((type(self), self._backend, self._workflow_id or self._id))

    def __repr__(self) -> str:
        """Show the Stream fields."""
        fields = ", ".join(f"{key}={value!r}" for key, value in self._fields().items())
        return f"{type(self).__name__}({fields})"

    def __copy__(self) -> Self:
        """A copy is a handle of the same Stream in the same context."""
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        """Return the same handle, since dataclasses.asdict deep-copies a handle field."""
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
temporalio.converter.transfer_type_convertible(_StreamHandleJson)(StreamHandle)
