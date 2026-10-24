"""The base of the Stream plugins."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import temporalio.client
import temporalio.worker
from temporalio.common.streams import _errors
from temporalio.common.streams._backend import StreamBackend
from temporalio.plugin import SimplePlugin


class StreamPlugin(SimplePlugin):
    """Base of the Stream plugins. Each registers one backend under its name.

    Put a Stream plugin on the client. A Worker takes its client's plugins. With more than one
    Stream plugin, exactly one has ``default=True``.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        name: str,
        *,
        backend_name: str,
        default: bool,
        stream_backend: StreamBackend | None = None,
        **options: Any,
    ) -> None:
        """Create the plugin. ``options`` go to :py:class:`temporalio.plugin.SimplePlugin`.

        A plugin with no ``stream_backend`` gives each client its own in
        :py:meth:`backend_for`.
        """
        self._backend_name = backend_name
        self._default = default
        self._stream_backend = stream_backend
        super().__init__(name, **options)

    @property
    def backend_name(self) -> str:
        """The name that Stream handles carry for this backend."""
        return self._backend_name

    @property
    def default(self) -> bool:
        """Whether a client with more than one Stream plugin uses this one by default."""
        return self._default

    def backend_for(
        self,
        client: temporalio.client.Client,  # pyright: ignore[reportUnusedParameter]
    ) -> StreamBackend:
        """The backend that carries the calls of ``client`` and of its Workers."""
        if self._stream_backend is None:
            raise NotImplementedError(f"{type(self).__name__} gives no backend")
        return self._stream_backend

    def configure_client(
        self, config: temporalio.client.ClientConfig
    ) -> temporalio.client.ClientConfig:
        """Check the default backend, then add the plugin's parts to the client."""
        check_default(config.get("plugins", []), "client")
        return super().configure_client(config)

    def configure_worker(
        self, config: temporalio.worker.WorkerConfig
    ) -> temporalio.worker.WorkerConfig:
        """Check the default backend, then add the plugin's parts to the Worker."""
        client = config.get("client")
        client_plugins = client.config()["plugins"] if client is not None else []
        check_default([*client_plugins, *config.get("plugins", [])], "Worker")
        return super().configure_worker(config)


def check_default(plugins: Iterable[object], owner: str) -> None:
    """Check that one Stream plugin is the default.

    The default backend is part of Workflow code, since a replayed create uses it. So the
    default must not follow the order of the plugin list.

    Raises:
        StreamConfigError: More than one Stream plugin, and not exactly one with
            ``default=True``.
    """
    stream_plugins = list(
        {id(p): p for p in plugins if isinstance(p, StreamPlugin)}.values()
    )
    if len(stream_plugins) < 2:
        return
    defaults = [p.backend_name for p in stream_plugins if p.default]
    if len(defaults) != 1:
        names = ", ".join(p.backend_name for p in stream_plugins)
        marked = f"{', '.join(defaults)} have" if defaults else "none has"
        raise _errors.StreamConfigError(
            f"The {owner} has the Stream plugins {names}, and {marked} default=True. "
            "Mark exactly one with default=True."
        )


def client_backend(
    client: temporalio.client.Client, name: str | None, *, only: bool = False
) -> tuple[str, StreamBackend]:
    """The name and the backend of a client's Stream plugin. None picks the default plugin,
    which is the only one or the one with ``default=True``. With ``only``, None needs a client
    with one backend, since a handle with no backend name may be of any of them.

    Raises:
        StreamConfigError: The client has no Stream plugin, none of that name, or more than
            one where ``only`` needs one.
    """
    backends = [
        (plugin.backend_name, plugin.backend_for(client), plugin.default)
        for plugin in client.config()["plugins"]
        if isinstance(plugin, StreamPlugin)
    ]
    if not backends:
        raise _errors.StreamConfigError(
            "The client has no Stream plugin, such as RedisStreamPlugin"
        )
    if not name and only and len(backends) > 1:
        raise _errors.StreamConfigError(
            "The handle names no backend, and the client has more than one"
        )
    if not name:
        defaults = [entry for entry in backends if entry[2]] or backends
        return defaults[0][0], defaults[0][1]
    for backend_name, backend, _ in backends:
        if backend_name == name:
            return backend_name, backend
    raise _errors.StreamConfigError(
        f"The client has no Stream backend {name}. It has "
        f"{', '.join(entry[0] for entry in backends)}."
    )
