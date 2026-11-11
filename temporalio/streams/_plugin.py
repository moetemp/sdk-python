"""The base of the Stream backends, the Stream config, and the rules of their registration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

import temporalio.activity
import temporalio.client
import temporalio.service
import temporalio.worker
from temporalio.converter import StorageDriverActivityInfo, StorageDriverWorkflowInfo
from temporalio.plugin import SimplePlugin
from temporalio.streams import _errors
from temporalio.streams._backend import BackendCalls


class StreamBackend(SimplePlugin):
    """Base class of the Stream backends. Each instance registers one backend under its name.

    Register backends on the client in a :py:class:`StreamConfig`, with
    ``Client.connect(..., stream_config=StreamConfig(backends=[...]))``. A Worker uses its
    client's config. A ``Replayer`` takes ``stream_config=`` too.

    A backend is not a plugin, so passing it in ``plugins=`` raises ``StreamConfigError``.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        name: str,
        *,
        backend_name: str,
        **options: Any,
    ) -> None:
        """Create the backend. ``options`` go to :py:class:`temporalio.plugin.SimplePlugin`."""
        self._backend_name = backend_name
        super().__init__(name, **options)

    @property
    def backend_name(self) -> str:
        """The backend name that Stream refs store."""
        return self._backend_name

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
        f"{type(backend).__name__} is a Stream backend, not a plugin. Pass it in a "
        "StreamConfig, as stream_config= of Client.connect or of the Replayer."
    )


@dataclass(frozen=True)
class StreamSelectContext:
    """The context of a :py:attr:`StreamConfig.backend_selector` call.

    It follows :py:class:`temporalio.converter.StorageDriverSelectContext`.

    .. warning::
        This API is experimental.
    """

    id: str
    """The Stream id."""

    target: StorageDriverActivityInfo | StorageDriverWorkflowInfo | None = None
    """The Workflow or Activity that makes the call, as in ``StorageDriverSelectContext``.

    An Activity that a Workflow started gets the Workflow, and an Activity with no Workflow gets
    the Activity. A client outside an Activity gets None. Workflow code never calls the
    selector, since a Worker serves only one backend.
    """


@dataclass(frozen=True)
class StreamConfig:
    """The Stream backends of a client and its Workers.

    It follows :py:class:`temporalio.converter.ExternalStorage`, which holds drivers and a
    driver selector. Pass it with ``Client.connect(..., stream_config=...)``. With one backend,
    every Stream goes to it. With more than one, :py:attr:`backend_selector` is required.

    A Worker serves only one backend, so a Worker whose client has two raises
    ``StreamConfigError``. During a migration, give each Worker a client with one backend.

    .. warning::
        This API is experimental.
    """

    backends: Sequence[StreamBackend]
    """The backends, at least one. Each has a unique :py:attr:`StreamBackend.backend_name`."""

    backend_selector: Callable[[StreamSelectContext], StreamBackend] | None = None
    """Picks the backend of a create, and of a get by id. It returns one of :py:attr:`backends`.

    Required with more than one backend. With one backend, every call uses it and no selector
    runs. A ref carries its backend's name, so a call through a ref finds its backend by name
    and doesn't call the selector. A get by id must reach the backend that the create of that
    id picked, so a selector uses facts that don't change, such as the Stream id.
    """

    def __post_init__(self) -> None:
        """Check the backends.

        Raises:
            TypeError: An entry is not a ``StreamBackend``.
            StreamConfigError: There is no backend, two backends have the same name, or there
                is more than one backend and no selector.
        """
        names: set[str] = set()
        for backend in self.backends:
            if not isinstance(backend, StreamBackend):
                raise TypeError(f"{backend!r} is not a StreamBackend")
            if backend.backend_name in names:
                raise _errors.StreamConfigError(
                    f"The Stream config has two backends with the name {backend.backend_name}"
                )
            names.add(backend.backend_name)
        if not names:
            raise _errors.StreamConfigError("The Stream config has no backend")
        if len(names) > 1 and self.backend_selector is None:
            raise _errors.StreamConfigError(
                "The Stream config has the backends "
                f"{', '.join(b.backend_name for b in self.backends)}, and no "
                "backend_selector. Set one to pick the backend of each Stream."
            )

    def _select(self, stream_id: str) -> StreamBackend:
        if len(self.backends) == 1:
            return self.backends[0]
        assert self.backend_selector is not None
        target: StorageDriverActivityInfo | StorageDriverWorkflowInfo | None = None
        if temporalio.activity.in_activity():
            # The same target that the Activity Worker gives external storage, so a selector
            # and a driver_selector see one target for one Activity.
            info = temporalio.activity.info()
            if info.in_workflow:
                target = StorageDriverWorkflowInfo(
                    namespace=info.namespace,
                    id=info.workflow_id,
                    run_id=info.workflow_run_id,
                    type=info.workflow_type,
                )
            else:
                target = StorageDriverActivityInfo(
                    namespace=info.namespace,
                    id=info.activity_id or None,
                    run_id=info.activity_run_id or None,
                    type=info.activity_type or None,
                )
        selected = self.backend_selector(
            StreamSelectContext(id=stream_id, target=target)
        )
        if not any(selected is backend for backend in self.backends):
            raise _errors.StreamConfigError(
                f"The backend_selector returned {selected!r} for the Stream {stream_id}, "
                "which is not one of the backends of the Stream config"
            )
        return selected

    def _named(self, name: str) -> StreamBackend:
        for backend in self.backends:
            if backend.backend_name == name:
                return backend
        raise _errors.StreamConfigError(
            f"The client has no Stream backend {name}. It has "
            f"{', '.join(b.backend_name for b in self.backends)}."
        )


def configure_client(
    config: temporalio.client.ClientConfig,
) -> temporalio.client.ClientConfig:
    """Apply the plugin configuration of each backend of the client's Stream config."""
    stream_config = config.get("stream_config")
    if not isinstance(stream_config, StreamConfig):
        raise TypeError(f"{stream_config!r} is not a StreamConfig")
    for backend in stream_config.backends:
        config = backend._configure_client(config)
    return config


def configure_worker(
    stream_config: StreamConfig, config: temporalio.worker.WorkerConfig
) -> temporalio.worker.WorkerConfig:
    """Apply the plugin configuration of the client's one backend to a Worker.

    Raises:
        StreamConfigError: The client has more than one backend.
    """
    if len(stream_config.backends) > 1:
        raise _errors.StreamConfigError(
            "The Worker's client has more than one Stream backend. A Worker serves only one "
            "backend, so give each Worker a client with one backend."
        )
    for backend in stream_config.backends:
        config = backend._configure_worker(config)
    return config


def configure_replayer(
    config: temporalio.worker.ReplayerConfig,
) -> temporalio.worker.ReplayerConfig:
    """Apply the plugin configuration of the Replayer's one backend.

    Raises:
        StreamConfigError: The Stream config has more than one backend.
    """
    stream_config = config.get("stream_config")
    if not isinstance(stream_config, StreamConfig):
        raise TypeError(f"{stream_config!r} is not a StreamConfig")
    if len(stream_config.backends) > 1:
        raise _errors.StreamConfigError(
            "The Replayer's Stream config has more than one backend. A Replayer serves only "
            "one backend, as a Worker does."
        )
    for backend in stream_config.backends:
        config = backend._configure_replayer(config)
    return config


def _client_config(client: temporalio.client.Client) -> StreamConfig:
    stream_config = client.config(active_config=True).get("stream_config")
    if stream_config is None:
        raise _errors.StreamConfigError(
            "The client has no Stream backend. Register one with "
            "Client.connect(..., stream_config=StreamConfig(backends=[...]))."
        )
    return stream_config


def select_backend(
    client: temporalio.client.Client, stream_id: str
) -> tuple[str, BackendCalls]:
    """The name and the calls of the backend of a create or a get by id.

    Raises:
        StreamConfigError: The client has no backend, or the selector returned a backend that
            is not registered.
    """
    backend = _client_config(client)._select(stream_id)
    return backend.backend_name, backend._backend_for(client)


def ref_backend(
    client: temporalio.client.Client, stream_id: str, name: str | None
) -> tuple[str, BackendCalls]:
    """The name and the calls of the backend that a ref names.

    A ref with no backend name goes through the selector, as a get by id does.

    Raises:
        StreamConfigError: The client has no backend of that name.
    """
    if not name:
        return select_backend(client, stream_id)
    backend = _client_config(client)._named(name)
    return backend.backend_name, backend._backend_for(client)
