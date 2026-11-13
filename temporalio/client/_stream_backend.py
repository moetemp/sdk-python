from __future__ import annotations

from abc import ABC, abstractmethod


class StreamBackend(ABC):
    """A store that answers the Stream operations of a client.

    A client with a backend sends each Stream operation to it instead of the server.

    .. warning::
        This API is experimental and subject to change.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the backend."""
        ...

    @abstractmethod
    async def call(self, operation: str, request: bytes) -> bytes:
        """Answer one Stream operation.

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
