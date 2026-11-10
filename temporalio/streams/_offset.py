"""The opaque offset of a Record in a Stream."""

from __future__ import annotations

from typing import Any

from typing_extensions import Self


class StreamOffset:
    """An opaque, ordered position of a Record in one Stream.

    A read gives it on each :py:class:`Record`, an append returns the offset of its last Record,
    and :py:meth:`get_offset` returns the newest one. Code compares two offsets of the same Stream
    with ``<``, ``<=``, ``>``, ``>=`` and ``==``, and resumes a read from one with
    ``read(offset=...)``. It has no meaning across Streams, and no arithmetic. To store or send
    one, use :py:meth:`__str__` and :py:meth:`parse`.

    .. warning::
        This API is experimental.
    """

    __slots__ = ("_offset",)

    def __init__(self, offset: bytes) -> None:
        """Wrap a wire offset. Build one with :py:meth:`parse`, or take one from a call."""
        self._offset = offset

    @classmethod
    def _from_wire(cls, offset: bytes) -> Self:
        """The offset of a wire bytes, for the read and append plumbing."""
        return cls(offset)

    @property
    def _wire(self) -> bytes:
        """The wire bytes, for the read and append plumbing."""
        return self._offset

    @classmethod
    def parse(cls, text: str) -> StreamOffset:
        """The offset of a string that :py:meth:`__str__` wrote.

        Raises:
            ValueError: The string is not a Stream offset.
        """
        try:
            return cls(bytes.fromhex(text))
        except ValueError:
            raise ValueError(f"Not a Stream offset: {text!r}") from None

    def __str__(self) -> str:
        """The string form, which :py:meth:`parse` reads back."""
        return self._offset.hex()

    def __repr__(self) -> str:
        """Show the offset."""
        return f"StreamOffset({self._offset.hex()})"

    def __eq__(self, other: object) -> bool:
        """Two offsets of one Stream are equal when they are the same position."""
        if not isinstance(other, StreamOffset):
            return NotImplemented
        return self._offset == other._offset

    def __lt__(self, other: Any) -> bool:
        """Order two offsets of one Stream."""
        if not isinstance(other, StreamOffset):
            return NotImplemented
        return self._offset < other._offset

    def __le__(self, other: Any) -> bool:
        """Order two offsets of one Stream."""
        if not isinstance(other, StreamOffset):
            return NotImplemented
        return self._offset <= other._offset

    def __gt__(self, other: Any) -> bool:
        """Order two offsets of one Stream."""
        if not isinstance(other, StreamOffset):
            return NotImplemented
        return self._offset > other._offset

    def __ge__(self, other: Any) -> bool:
        """Order two offsets of one Stream."""
        if not isinstance(other, StreamOffset):
            return NotImplemented
        return self._offset >= other._offset

    def __hash__(self) -> int:
        """Hash of the position."""
        return hash(self._offset)
