"""The base of the Stream plugins."""

from __future__ import annotations

from typing import Any

import temporalio.client
from temporalio.contrib.streams import _errors
from temporalio.contrib.streams._backend import StreamBackend
from temporalio.plugin import SimplePlugin


class StreamPlugin(SimplePlugin):
    """Base of the Stream plugins. Each registers one backend under its name.

    Put a Stream plugin on the client. A Worker takes its client's plugins.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        name: str,
        *,
        backend_name: str,
        default: bool,
        stream_backend: StreamBackend,
        **options: Any,
    ) -> None:
        """Create the plugin. ``options`` go to :py:class:`temporalio.plugin.SimplePlugin`."""
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

    @property
    def stream_backend(self) -> StreamBackend:
        """The backend that carries the calls."""
        return self._stream_backend


def client_backend(
    client: temporalio.client.Client, name: str | None, *, only: bool = False
) -> tuple[str, StreamBackend]:
    """The name and the backend of a client's Stream plugin. None picks the plugin marked
    as the default, or else the first one. With ``only``, None needs a client with one
    backend, since a handle with no backend name may be of any of them.

    Raises:
        StreamConfigError: The client has no Stream plugin, none of that name, or more than
            one where ``only`` needs one.
    """
    backends = [
        (plugin.backend_name, plugin.stream_backend, plugin.default)
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
        return defaults[0][0], defaults[0][1].for_client(client)
    for backend_name, backend, _ in backends:
        if backend_name == name:
            return backend_name, backend.for_client(client)
    raise _errors.StreamConfigError(
        f"The client has no Stream backend {name}. It has "
        f"{', '.join(entry[0] for entry in backends)}."
    )
