"""Native extensions that a Worker passes to Core."""

from __future__ import annotations

import temporalio.bridge.extension
import temporalio.runtime


class WorkerExtension:
    """A native extension that the SDK's bridge build links, which a Worker passes to Core.

    Core calls the extension at fixed points of each Workflow Task. A plugin builds the extension
    and passes it in the Worker's ``extension`` option. The Worker must use the extension's
    runtime.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self,
        name: str,
        config: bytes = b"",
        *,
        runtime: temporalio.runtime.Runtime | None = None,
    ) -> None:
        """Build the extension with a name that the bridge build knows.

        Args:
            name: The name of the extension in the bridge build.
            config: The extension's encoded config. Each extension has its own format.
            runtime: The runtime the extension runs on. Defaults to
                :py:meth:`temporalio.runtime.Runtime.default`.

        Raises:
            ValueError: The bridge build has no extension with this name, or the config is not
                valid.
        """
        self._runtime = runtime or temporalio.runtime.Runtime.default()
        self._bridge_extension = temporalio.bridge.extension.WorkerExtension(
            self._runtime._core_runtime, name, config
        )

    @property
    def name(self) -> str:
        """Name the extension gives itself."""
        return self._bridge_extension.name

    @property
    def runtime(self) -> temporalio.runtime.Runtime:
        """Runtime the extension runs on."""
        return self._runtime
