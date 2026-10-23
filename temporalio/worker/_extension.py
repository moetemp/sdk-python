"""Native extensions that a Worker passes to Core."""

from __future__ import annotations

import temporalio.bridge.extension
import temporalio.exceptions
import temporalio.runtime
import temporalio.service


class WorkerExtension:
    """A native extension that the SDK's bridge build links, which a Worker passes to Core.

    A plugin builds the extension, passes it in the Worker's ``extension`` option and calls its
    methods. Core keeps it with the Worker and does not call it. The Worker must use the
    extension's runtime.

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

    def call(self, method: str, input: bytes) -> bytes:
        """Call a method of the extension and wait for its output.

        This blocks the thread, so call it only for methods that the extension documents as
        fast.

        Args:
            method: The method name. Each extension has its own methods.
            input: The encoded input.

        Returns:
            The encoded output.

        Raises:
            ExtensionError: The extension refused the call.
        """
        try:
            return self._bridge_extension.call(method, input)
        except temporalio.bridge.extension.ExtensionCallError as err:
            raise ExtensionError._from_bridge(err) from None

    async def call_async(self, method: str, input: bytes) -> bytes:
        """Call a method of the extension on its runtime and await the output.

        Args:
            method: The method name. Each extension has its own methods.
            input: The encoded input.

        Returns:
            The encoded output.

        Raises:
            ExtensionError: The extension refused the call.
        """
        try:
            return await self._bridge_extension.call_async(method, input)
        except temporalio.bridge.extension.ExtensionCallError as err:
            raise ExtensionError._from_bridge(err) from None


class ExtensionError(temporalio.exceptions.TemporalError):
    """Error that a :py:class:`WorkerExtension` call raises.

    It has the fields of a gRPC status, so callers can treat it like a failed service call.

    .. warning::
        This API is experimental.
    """

    def __init__(
        self, message: str, code: temporalio.service.RPCStatusCode, details: bytes
    ) -> None:
        """Create an extension error."""
        super().__init__(message)
        self._message = message
        self._code = code
        self._details = details

    @staticmethod
    def _from_bridge(err: Exception) -> ExtensionError:
        code, message, details = err.args
        return ExtensionError(message, temporalio.service.RPCStatusCode(code), details)

    @property
    def message(self) -> str:
        """Message from the extension."""
        return self._message

    @property
    def code(self) -> temporalio.service.RPCStatusCode:
        """Status code of the error, as in gRPC."""
        return self._code

    @property
    def details(self) -> bytes:
        """Encoded error details. Each extension has its own format. Empty if none."""
        return self._details
