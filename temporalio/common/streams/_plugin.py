"""The base of the Stream backends, and the rules of their registration."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, cast

import temporalio.client
import temporalio.converter
import temporalio.worker
from temporalio.common.streams import _errors, _ids
from temporalio.common.streams._backend import BackendCalls, _ExtensionCalls
from temporalio.plugin import SimplePlugin

# The default backend when no registered backend has default=True. Every client, Worker and
# Replayer has it with no registration.
NATIVE = _ids.NATIVE_BACKEND


class StreamBackend(SimplePlugin):
    """Base of the Stream backends. Each registers one backend under its name.

    Register backends on the client with ``Client.connect(..., stream_backends=[...])``. A
    Worker takes its client's backends, and a ``Replayer`` takes ``stream_backends=`` too.
    Every client, Worker and Replayer also has the native backend, named ``native``, with no
    registration. Register a ``NativeStreamBackend`` only to configure it, such as for its
    Retention. Names are unique, and only the native backend is named ``native``. With more
    than one registered backend, exactly one has ``default=True``. With none marked, the
    default is ``native``.

    A backend is not a plugin, so passing it in ``plugins=`` raises ``StreamConfigError``.

    .. warning::
        This API is experimental.
    """

    # The kind of Store, such as "redis", so the calls of a backend's package find the
    # registered backend of their kind.
    _kind = ""
    # Whether the backend keeps the Streams of a Standalone Activity, which it closes with the
    # Activity. Only the server does, so a Stream that nobody closes can't stay.
    _activity_streams = False
    # The most bytes of one append that the Store takes, as it counts them.
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
        calls: BackendCalls | None = None,
        **options: Any,
    ) -> None:
        """Create the backend. ``options`` go to :py:class:`temporalio.plugin.SimplePlugin`.

        A backend with no ``calls`` gives each client its own in :py:meth:`backend_for`.
        """
        self._backend_name = backend_name
        self._default = default
        self._calls = calls
        super().__init__(name, **options)

    @property
    def backend_name(self) -> str:
        """The name that Stream handles carry for this backend."""
        return self._backend_name

    @property
    def default(self) -> bool:
        """Whether a create with no backend name uses this backend."""
        return self._default

    def backend_for(
        self,
        client: temporalio.client.Client,  # pyright: ignore[reportUnusedParameter]
    ) -> BackendCalls:
        """The calls of this backend for ``client`` and its Workers."""
        if self._calls is None:
            raise NotImplementedError(f"{type(self).__name__} gives no calls")
        return self._calls

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

    # The registration calls these, so the plugin hooks above only see a backend that went
    # into plugins= by mistake.

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

    The default backend is part of Workflow code, since a replayed create uses it. So the
    default must not follow the order of the list.

    Raises:
        TypeError: An entry is not a ``StreamBackend``.
        StreamConfigError: Two backends have one name, a backend that is not native has the
            name ``native``, or more than one backend and not exactly one with
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
    """The name of the default backend: the one with ``default=True``, else ``native``."""
    return next((b.backend_name for b in backends if b.default), NATIVE)


def with_native(backends: Sequence[StreamBackend]) -> list[StreamBackend]:
    """The registered backends, and the native one when none of them is named ``native``."""
    from temporalio.common.streams import _native

    if any(b.backend_name == NATIVE for b in backends):
        return list(backends)
    return [*backends, _native.implicit()]


def configure_client(
    config: temporalio.client.ClientConfig,
) -> temporalio.client.ClientConfig:
    """Check the client's backends, and add the parts of each to the client."""
    for backend in check_backends(config.get("stream_backends", ()), "client"):
        config = backend._configure_client(config)
    return config


def configure_worker(
    config: temporalio.worker.WorkerConfig,
) -> temporalio.worker.WorkerConfig:
    """Add the parts of the client's backends and of the native one to a Worker."""
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
    from temporalio.common.streams import _extension, _writes

    client = config["client"]  # type: ignore[reportTypedDictNotRequiredAccess]
    activities = list(cast(list[Callable[..., Any]], _writes.ACTIVITIES))
    for backend in with_native(client_backends(client)):
        if isinstance(backend, _extension.ExtensionStreamBackend):
            activities.extend(backend._activities())
    return activities


def replayer_backends(config: temporalio.worker.ReplayerConfig) -> list[StreamBackend]:
    """The Replayer's backends, else those of its ``stream_client``, and the native one."""
    backends = config.get("stream_backends") or ()
    client = config.get("stream_client")
    if not backends and client is not None:
        backends = client_backends(client)
    return with_native(check_backends(backends, "Replayer"))


def configure_replayer(
    config: temporalio.worker.ReplayerConfig,
) -> temporalio.worker.ReplayerConfig:
    """Check the Replayer's backends, and add the parts of each and of the native one."""
    for backend in replayer_backends(config):
        config = backend._configure_replayer(config)
    return config


def worker_converter(
    client: temporalio.client.Client, converter: temporalio.converter.DataConverter
) -> temporalio.converter.DataConverter:
    """The converter of a Worker's Workflows, whose codec step turns a native read's
    positions back into its page.
    """
    from temporalio.common.streams import _native

    native = next(
        b for b in with_native(client_backends(client)) if b.backend_name == NATIVE
    )
    assert isinstance(native, _native.NativeStreamBackend)
    return _native.wrap(lambda: native.backend_for(client), converter)


def replayer_converter(
    config: temporalio.worker.ReplayerConfig,
    converter: temporalio.converter.DataConverter,
) -> temporalio.converter.DataConverter:
    """The converter of a Replayer's Workflows, as :py:func:`worker_converter`. A replay
    reads a native page again on the connection of the Replayer's ``stream_client``.
    """
    from temporalio.common.streams import _native

    native = next(b for b in replayer_backends(config) if b.backend_name == NATIVE)
    assert isinstance(native, _native.NativeStreamBackend)
    client = config.get("stream_client")

    def backend() -> _ExtensionCalls:
        if client is None:
            raise _errors.StreamConfigError(
                "A replay reads each page of a native Workflow read again, so pass the "
                "Replayer a stream_client= of the server that keeps the Streams"
            )
        return native.backend_for(client)

    return _native.wrap(backend, converter)


def client_backends(client: temporalio.client.Client) -> list[StreamBackend]:
    """The backends registered on a client."""
    return list(client.config(active_config=True).get("stream_backends", ()))


def backend_of(client: temporalio.client.Client, name: str | None) -> StreamBackend:
    """A client's backend. None picks the default backend.

    Raises:
        StreamConfigError: The client has no backend of that name.
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
    """The name and the calls of a client's backend. None picks the default backend.

    Raises:
        StreamConfigError: The client has no backend of that name.
    """
    backend = backend_of(client, name)
    return backend.backend_name, backend.backend_for(client)
