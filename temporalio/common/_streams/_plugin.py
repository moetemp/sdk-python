"""The base of the Stream backends, and the rules of their registration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

import temporalio.client
import temporalio.converter
import temporalio.service
import temporalio.worker
from temporalio.common._streams import _errors, _ids
from temporalio.common._streams._backend import BackendCalls, _ExtensionCalls
from temporalio.plugin import SimplePlugin

# The default backend when no registered backend has default=True. Every client, Worker and
# Replayer has it with no registration.
NATIVE = _ids.NATIVE_BACKEND


class StreamBackend(SimplePlugin):
    """Base class of the Stream backends. Each instance registers one backend under its name.

    Register backends on the client with ``Client.connect(..., stream_backends=[...])``. A
    Worker uses its client's backends. A ``Replayer`` takes ``stream_backends=`` too. Every
    client, Worker and Replayer also has the native backend, named ``native``, with no
    registration. Register a ``NativeStreamBackend`` only to configure it, such as for its
    Retention. Names must be unique, and only the native backend can be named ``native``. With
    more than one registered backend, exactly one must have ``default=True``. With no backend
    marked, the default is ``native``.

    A backend is not a plugin, so passing it in ``plugins=`` raises ``StreamConfigError``.

    .. warning::
        This API is experimental.
    """

    # The kind of Store, such as "redis". The functions of a backend's package use it to find
    # the registered backend of their kind.
    _kind = ""
    # Whether the backend stores the Streams of a Standalone Activity and closes each with its
    # Activity. Only the server does, so no Stream of an ended Activity stays open.
    _activity_streams = False
    # The most bytes of one append that the Store accepts, as it counts them.
    _max_batch_bytes = 4 * 1024 * 1024
    # Whether the Store finds a Workflow's Stream only by its owner fields, so a ref with a
    # Workflow id and no first run id names nothing.
    _nests_workflow_streams = False

    def __init__(
        self,
        name: str,
        *,
        backend_name: str,
        default: bool,
        **options: Any,
    ) -> None:
        """Create the backend. ``options`` go to :py:class:`temporalio.plugin.SimplePlugin`."""
        self._backend_name = backend_name
        self._default = default
        super().__init__(name, **options)

    @property
    def backend_name(self) -> str:
        """The backend name that Stream handles store."""
        return self._backend_name

    @property
    def default(self) -> bool:
        """Whether a create with no backend name uses this backend."""
        return self._default

    def _backend_for(
        self,
        client: temporalio.client.Client,  # pyright: ignore[reportUnusedParameter]
    ) -> BackendCalls:
        # Each backend subclass returns its calls for the client and its Workers.
        raise NotImplementedError(f"{type(self).__name__} gives no calls")

    async def connect_service_client(
        self,
        config: temporalio.service.ConnectConfig,
        next: Callable[
            [temporalio.service.ConnectConfig],
            Awaitable[temporalio.service.ServiceClient],
        ],
    ) -> temporalio.service.ServiceClient:
        """Refuse a backend in ``plugins=``, before the client connects."""
        raise _not_a_plugin(self)

    def configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        """Refuse a backend in ``plugins=``."""
        raise _not_a_plugin(self)

    def configure_worker(
        self, config: temporalio.worker.WorkerConfig
    ) -> temporalio.worker.WorkerConfig:
        """Refuse a backend in ``plugins=``."""
        raise _not_a_plugin(self)

    def configure_replayer(
        self, config: temporalio.worker.ReplayerConfig
    ) -> temporalio.worker.ReplayerConfig:
        """Refuse a backend in ``plugins=``."""
        raise _not_a_plugin(self)

    # Stream registration calls these, so the public hooks above only run for a backend that
    # was passed in plugins= by mistake.

    def _configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        return super().configure_client(config)

    def _configure_worker(
        self, config: temporalio.worker.WorkerConfig
    ) -> temporalio.worker.WorkerConfig:
        return super().configure_worker(config)

    def _configure_replayer(
        self, config: temporalio.worker.ReplayerConfig
    ) -> temporalio.worker.ReplayerConfig:
        return super().configure_replayer(config)


def _not_a_plugin(backend: StreamBackend) -> _errors.StreamConfigError:
    return _errors.StreamConfigError(
        f"{type(backend).__name__} is a Stream backend, not a plugin. Pass it in "
        "stream_backends= of Client.connect, or of the Replayer."
    )


def check_backends(backends: Sequence[object], owner: str) -> list[StreamBackend]:
    """Check the registered backends, and return them.

    A replayed create uses the default backend, so the default must not depend on the order
    of the list.

    Raises:
        TypeError: An entry is not a ``StreamBackend``.
        StreamConfigError: Two backends have the same name, a backend that isn't native has
            the name ``native``, or there is more than one backend and not exactly one has
            ``default=True``.
    """
    checked: list[StreamBackend] = []
    for backend in backends:
        if not isinstance(backend, StreamBackend):
            raise TypeError(f"{backend!r} is not a StreamBackend")
        if any(other.backend_name == backend.backend_name for other in checked):
            raise _errors.StreamConfigError(
                f"The {owner} has two Stream backends named {backend.backend_name}"
            )
        if backend.backend_name == NATIVE and backend._kind != NATIVE:
            raise _errors.StreamConfigError(
                f"The name {NATIVE} is the native backend's, so {type(backend).__name__} "
                "takes another name"
            )
        checked.append(backend)
    if len(checked) < 2:
        return checked
    defaults = [b.backend_name for b in checked if b.default]
    if len(defaults) != 1:
        names = ", ".join(b.backend_name for b in checked)
        marked = f"{', '.join(defaults)} have" if defaults else "none has"
        raise _errors.StreamConfigError(
            f"The {owner} has the Stream backends {names}, and {marked} default=True. "
            "Mark exactly one with default=True."
        )
    return checked


def default_name(backends: Sequence[StreamBackend]) -> str:
    """The name of the backend with ``default=True``, or ``native`` if none has it."""
    return next((b.backend_name for b in backends if b.default), NATIVE)


def with_native(backends: Sequence[StreamBackend]) -> list[StreamBackend]:
    """The registered backends, and the native one when none of them is named ``native``."""
    from temporalio.common._streams import _native

    if any(b.backend_name == NATIVE for b in backends):
        return list(backends)
    return [*backends, _native.implicit()]


def configure_client(
    config: temporalio.client.ClientConfig,
) -> temporalio.client.ClientConfig:
    """Check the client's backends, and apply the plugin configuration of each one."""
    backends = check_backends(config.get("stream_backends", ()), "client")
    config["stream_backends"] = backends
    for backend in backends:
        config = backend._configure_client(config)
    return config


def configure_worker(
    config: temporalio.worker.WorkerConfig,
) -> temporalio.worker.WorkerConfig:
    """Apply the plugin configuration of the client's backends and the native one to a Worker."""
    client = config["client"]  # type: ignore[reportTypedDictNotRequiredAccess]
    for backend in with_native(client_backends(client)):
        config = backend._configure_worker(config)
    return config


def local_activities(
    config: temporalio.worker.WorkerConfig,
) -> list[Callable[..., Any]]:
    """The Local Activities that a Worker's Workflows run for Streams.

    The write Local Activities route by the backend in each request, so every backend of the
    Worker shares one set. Each backend reads with its own.
    """
    from temporalio.common._streams import _extension, _writes

    client = config["client"]  # type: ignore[reportTypedDictNotRequiredAccess]
    activities = list(cast(list[Callable[..., Any]], _writes.LOCAL_ACTIVITIES))
    for backend in with_native(client_backends(client)):
        if isinstance(backend, _extension.ExtensionStreamBackend):
            activities.extend(backend._activities())
    return activities


def replayer_backends(config: temporalio.worker.ReplayerConfig) -> list[StreamBackend]:
    """The Replayer's backends, or else those of its ``stream_client``, and the native one."""
    backends = config.get("stream_backends") or ()
    client = config.get("stream_client")
    if not backends and client is not None:
        backends = client_backends(client)
    return with_native(check_backends(backends, "Replayer"))


def configure_replayer(
    config: temporalio.worker.ReplayerConfig,
) -> temporalio.worker.ReplayerConfig:
    """Check the Replayer's backends, and apply the plugin config of each and of the native one."""
    for backend in replayer_backends(config):
        config = backend._configure_replayer(config)
    return config


def worker_converter(
    client: temporalio.client.Client, converter: temporalio.converter.DataConverter
) -> temporalio.converter.DataConverter:
    """The converter of a Worker's Workflows.

    Its codec step turns a native read's positions back into its page.
    """
    from temporalio.common._streams import _native

    native = next(
        b for b in with_native(client_backends(client)) if b.backend_name == NATIVE
    )
    assert isinstance(native, _native.NativeStreamBackend)
    return _native.wrap(lambda: native._backend_for(client), converter)


def replayer_converter(
    config: temporalio.worker.ReplayerConfig,
    converter: temporalio.converter.DataConverter,
) -> temporalio.converter.DataConverter:
    """The converter of a Replayer's Workflows, as :py:func:`worker_converter`.

    A replay reads a native page again with the connection of the Replayer's ``stream_client``.
    """
    from temporalio.common._streams import _native

    native = next(b for b in replayer_backends(config) if b.backend_name == NATIVE)
    assert isinstance(native, _native.NativeStreamBackend)
    client = config.get("stream_client")

    def backend() -> _ExtensionCalls:
        if client is None:
            raise _errors.StreamConfigError(
                "A replay reads each page of a native Workflow read again, so pass the "
                "Replayer a stream_client= of the server that keeps the Streams"
            )
        return native._backend_for(client)

    return _native.wrap(backend, converter)


def client_backends(client: temporalio.client.Client) -> list[StreamBackend]:
    """The backends registered on a client."""
    return list(client.config(active_config=True).get("stream_backends", ()))


def backend_of(client: temporalio.client.Client, name: str | None) -> StreamBackend:
    """A client's backend. A ``name`` of None picks the default.

    Raises:
        StreamConfigError: The client has no backend with that name.
    """
    backends = with_native(client_backends(client))
    wanted = name or default_name(backends)
    for backend in backends:
        if backend.backend_name == wanted:
            return backend
    raise _errors.StreamConfigError(
        f"The client has no Stream backend {wanted}. It has "
        f"{', '.join(b.backend_name for b in backends)}."
    )


def client_backend(
    client: temporalio.client.Client, name: str | None
) -> tuple[str, BackendCalls]:
    """The name and the calls of a client's backend. A ``name`` of None picks the default.

    Raises:
        StreamConfigError: The client has no backend with that name.
    """
    backend = backend_of(client, name)
    return backend.backend_name, backend._backend_for(client)
