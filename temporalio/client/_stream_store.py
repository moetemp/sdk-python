from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import google.protobuf.message
import nexusrpc

import temporalio.activity
import temporalio.api.stream.v1
import temporalio.bridge._visitor
import temporalio.bridge.runtime
import temporalio.common
import temporalio.converter
import temporalio.converter._payload_converter
import temporalio.nexus.system.stream_service
import temporalio.runtime
from temporalio.converter import StorageDriverActivityInfo, StorageDriverWorkflowInfo
from temporalio.exceptions import StreamConfigError

if TYPE_CHECKING:
    import temporalio.bridge.streams


@dataclasses.dataclass(frozen=True)
class StreamStoreCallOptions:
    """Options of the Local Activity that calls a Stream Store for Workflow code.

    Workflow code can't do I/O, so a worker calls its Stream Store from a Local
    Activity.

    A Workflow's call whose ref names a Store takes the options of that Store. A call
    whose ref names no Store, such as a create, takes the options of the only Store.
    With several Stores, such a call takes the defaults of this class, since the Local
    Activity picks the Store only when it runs.

    .. warning::
        This API is experimental and subject to change.
    """

    start_to_close_timeout: timedelta | None = timedelta(seconds=60)
    """Most time for one attempt."""

    schedule_to_close_timeout: timedelta | None = None
    """Most time for all attempts."""

    retry_policy: temporalio.common.RetryPolicy = dataclasses.field(
        default_factory=lambda: temporalio.common.RetryPolicy(maximum_attempts=1)
    )
    """Retries of a failed call. The default is one attempt."""

    def __post_init__(self) -> None:
        """Check that a timeout is set."""
        if not self.start_to_close_timeout and not self.schedule_to_close_timeout:
            raise ValueError(
                "start_to_close_timeout or schedule_to_close_timeout must be set"
            )


class StreamStore(ABC):
    """A Store that answers the Stream operations of a client.

    A client with a Store sends each Stream operation to it instead of the server. A
    Store is configuration. Use
    :py:class:`temporalio.contrib.redis.streams.RedisStreamStore`, or
    :py:class:`temporalio.testing.MemoryStreamStore` in tests.

    .. warning::
        This API is experimental and subject to change.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the Store."""
        ...

    @property
    def call_options(self) -> StreamStoreCallOptions:
        """Options of the Local Activity that calls this Store for Workflow code."""
        return StreamStoreCallOptions()

    @abstractmethod
    async def _call(self, operation: str, request: bytes) -> bytes:
        """Answer one Stream operation, in the form that the bridge defines.

        Args:
            operation: The Nexus operation name, such as ``CreateStream``,
                ``AppendRecords``, ``ReadRecords``, ``CloseStream`` or
                ``DescribeStream``.
            request: The serialized request message of
                ``temporal.api.streamservice.v1``.

        Returns:
            The serialized response message of ``temporal.api.streamservice.v1``.
        """
        ...


class _BridgeStreamStore(StreamStore):  # pyright: ignore[reportUnusedClass]
    """A Store that runs on the Stream runtime of the bridge.

    It builds the runtime at its first call, so the Store can be built before the
    Runtime and its Tokio threads exist.
    """

    def __init__(
        self,
        *,
        name: str,
        runtime: temporalio.runtime.Runtime | None,
        call_options: StreamStoreCallOptions | None = None,
    ) -> None:
        self._name = name
        self._runtime = runtime
        self._call_options = call_options
        self._stream_runtime: temporalio.bridge.streams.StreamRuntime | None = None

    @property
    def name(self) -> str:
        """Name of the Store."""
        return self._name

    @property
    def call_options(self) -> StreamStoreCallOptions:
        """Options of the Local Activity that calls this Store for Workflow code."""
        return self._call_options or super().call_options

    @abstractmethod
    def _new_stream_runtime(
        self, runtime: temporalio.bridge.runtime.Runtime
    ) -> temporalio.bridge.streams.StreamRuntime: ...

    async def _call(self, operation: str, request: bytes) -> bytes:
        """Answer one Stream operation with the Stream runtime of the bridge.

        Raises:
            temporalio.exceptions.StreamError: The call failed.
            temporalio.exceptions.StreamConfigError: This build of temporalio has no
                Stream runtime, or not this Store.
        """
        if self._stream_runtime is None:
            runtime = self._runtime or temporalio.runtime.Runtime.default()
            self._stream_runtime = self._new_stream_runtime(runtime._core_runtime)
        return await self._stream_runtime.call(operation, request)


@dataclasses.dataclass(frozen=True)
class StreamSelectContext:
    """Context passed to :py:attr:`StreamConfig.store_selector` calls.

    .. warning::
        This API is experimental and subject to change.
    """

    id: str
    """The id of the Stream."""

    target: StorageDriverActivityInfo | StorageDriverWorkflowInfo | None = None
    """The Workflow or Activity that makes the call, or ``None`` for a plain client.

    A call in an Activity has that Activity here. A Workflow's call runs the selector
    in the SDK's Local Activity, and has that Workflow here.
    """


@dataclasses.dataclass(frozen=True)
class StreamConfig:
    """The Stream Stores of a client, and the rule that picks one for a new Stream.

    A client with this config sends each Stream operation to one of its Stores
    instead of the server. It holds Stores and a selector, as
    :py:class:`temporalio.converter.ExternalStorage` holds drivers and a selector.

    A ref carries the name of its Store, so a call on a known Stream goes to the
    Store of that name. A call whose ref names no Store, such as a create, goes to
    the one Store, or to the Store that :py:attr:`store_selector` returns.

    .. warning::
        This API is experimental and subject to change.
    """

    stores: Sequence[StreamStore]
    """At least one Store. Each Store has a unique name.

    A ref carries the :py:attr:`StreamStore.name` of its Store, and a call looks
    the Store up by that name. With more than one Store, :py:attr:`store_selector`
    must also be set. A Worker serves every Store of its client.
    """

    store_selector: Callable[[StreamSelectContext], StreamStore] | None = None
    """Picks the Store of a new Stream. Required with more than one Store.

    It gets a :py:class:`StreamSelectContext` for each call whose ref names no
    Store, and returns one of the instances in :py:attr:`stores`. When it is
    ``None`` and there is one Store, every new Stream goes to that Store.

    A Workflow's create runs the selector in a Local Activity. History keeps the
    ref with the Store's name, so a replay doesn't call the selector again. A Stream
    that code builds from an id only goes through the selector on each call, so the
    selector must give the same Store for the same id.
    """

    _store_map: dict[str, StreamStore] = dataclasses.field(
        init=False, repr=False, compare=False
    )
    """Name-keyed index of :py:attr:`stores`, built at construction time."""

    def __post_init__(self) -> None:
        """Check the Stores and build the name-keyed map of them.

        Raises :py:class:`temporalio.exceptions.StreamConfigError` if there is no
        Store, if two Stores share a name, or if more than one Store is given
        without a :py:attr:`store_selector`.
        """
        if not self.stores:
            raise StreamConfigError("StreamConfig.stores must have at least one Store")
        store_map: dict[str, StreamStore] = {}
        for store in self.stores:
            if store.name in store_map:
                raise StreamConfigError(
                    f"StreamConfig.stores has more than one Store named '{store.name}'. "
                    "Each Store needs a unique name."
                )
            store_map[store.name] = store
        if len(self.stores) > 1 and self.store_selector is None:
            raise StreamConfigError(
                "StreamConfig.store_selector must be set when there is more than one Store"
            )
        object.__setattr__(self, "_store_map", store_map)

    def _select_store(self, context: StreamSelectContext) -> StreamStore:
        selector = self.store_selector
        if selector is None:
            return self.stores[0]
        store = selector(context)
        if (
            not isinstance(store, StreamStore)
            or self._store_map.get(store.name) is not store
        ):
            raise StreamConfigError(
                f"The store_selector returned {store!r}, which isn't one of "
                "StreamConfig.stores"
            )
        return store

    def _get_store_by_name(self, name: str) -> StreamStore:
        store = self._store_map.get(name)
        if store is None:
            raise StreamConfigError(
                f"The ref names the Stream Store '{name}', which this client doesn't have"
            )
        return store

    def _store_for(
        self,
        operation: str,
        request: bytes,
        target: StorageDriverActivityInfo | StorageDriverWorkflowInfo | None,
    ) -> StreamStore:
        ref = _request_ref(operation, request)
        if ref.store:
            return self._get_store_by_name(ref.store)
        return self._select_store(StreamSelectContext(id=ref.id, target=target))


def _request_ref(operation: str, request: bytes) -> temporalio.api.stream.v1.StreamRef:
    definition = _STREAM_SERVICE.operation_definitions[operation]
    message = _proto_type(definition.input_type).FromString(request)
    # A create calls its ref `stream_ref` and every other request calls it `ref`, so
    # the field is found by its type.
    for field in message.DESCRIPTOR.fields:
        if field.message_type is temporalio.api.stream.v1.StreamRef.DESCRIPTOR:
            return getattr(message, field.name)
    raise TypeError(f"The {operation} request has no StreamRef")


def _activity_target() -> StorageDriverActivityInfo | None:  # pyright: ignore[reportUnusedFunction]
    # External storage builds the same value from the running Activity.
    if not temporalio.activity.in_activity():
        return None
    info = temporalio.activity.info()
    return StorageDriverActivityInfo(
        id=info.activity_id,
        type=info.activity_type,
        run_id=info.activity_run_id,
        namespace=info.namespace,
    )


def _refuse_store_plugins(  # pyright: ignore[reportUnusedFunction]
    plugins: Sequence[object],
) -> None:
    for plugin in plugins:
        if isinstance(plugin, StreamStore):
            raise StreamConfigError(
                f"The Stream Store '{plugin.name}' is in plugins. Give it in "
                "stream_config=StreamConfig(stores=[...]) instead."
            )


def _check_not_negative(**durations: timedelta | None) -> None:  # pyright: ignore[reportUnusedFunction]
    for label, value in durations.items():
        if value is not None and value < timedelta():
            raise ValueError(f"{label} must not be negative")


def _stream_service() -> nexusrpc.ServiceDefinition:
    service = nexusrpc.get_service_definition(
        temporalio.nexus.system.stream_service.StreamService
    )
    if service is None:
        raise RuntimeError("The generated StreamService has no service definition")
    return service


_STREAM_SERVICE = _stream_service()


def _proto_type(model_type: Any) -> type[google.protobuf.message.Message]:
    converter = temporalio.converter._payload_converter._get_transfer_type_converter(
        model_type
    )
    if converter is None or converter.transfer_type is None:
        raise TypeError(f"{model_type} has no proto transfer type")
    return converter.transfer_type


async def _call_stream_store(  # pyright: ignore[reportUnusedFunction]
    store: StreamStore,
    data_converter: temporalio.converter.DataConverter,
    operation: str,
    request: bytes,
) -> bytes:
    if not data_converter.payload_codec:
        return await store._call(operation, request)
    # The worker package imports the client, so a module level import would cycle.
    from temporalio.bridge.worker import _Visitor

    definition = _STREAM_SERVICE.operation_definitions[operation]
    visitor = temporalio.bridge._visitor.PayloadVisitor(skip_search_attributes=True)
    request_message = _proto_type(definition.input_type).FromString(request)
    await visitor.visit(
        _Visitor(data_converter._encode_payload_sequence), request_message
    )
    response = await store._call(operation, request_message.SerializeToString())
    response_message = _proto_type(definition.output_type).FromString(response)
    await visitor.visit(
        _Visitor(data_converter._decode_payload_sequence), response_message
    )
    return response_message.SerializeToString()


def _stream_response(  # pyright: ignore[reportUnusedFunction]
    payload_converter: temporalio.converter.PayloadConverter,
    operation: str,
    result_type: type | None,
    response: bytes,
) -> Any:
    definition = _STREAM_SERVICE.operation_definitions[operation]
    [payload] = payload_converter.to_payloads(
        [_proto_type(definition.output_type).FromString(response)]
    )
    [result] = payload_converter.from_payloads(
        [payload], [result_type] if result_type else None
    )
    return result
