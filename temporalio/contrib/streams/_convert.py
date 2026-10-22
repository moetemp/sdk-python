"""Value conversion for Stream Records outside Workflow code."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator, Sequence
from typing import Any

import temporalio.api.common.v1
import temporalio.common
import temporalio.converter
import temporalio.nexus.system


class _RawPayloadConverter(temporalio.converter.PayloadConverter):
    # The generated models carry Record bodies as RawValue, so the facade runs the user's
    # converter and codec on the bodies only, never on the outer request.
    def to_payloads(
        self, values: Sequence[Any]
    ) -> list[temporalio.api.common.v1.Payload]:
        payloads = []
        for value in values:
            if not isinstance(value, temporalio.common.RawValue):
                raise TypeError(f"expected a RawValue Record body, got {type(value)}")
            payloads.append(value.payload)
        return payloads

    def from_payloads(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        type_hints: list[type] | None = None,
    ) -> list[Any]:
        return [temporalio.common.RawValue(payload) for payload in payloads]


@contextlib.contextmanager
def raw_bodies(
    failure_converter: temporalio.converter.FailureConverter,
) -> Iterator[None]:
    """Convert generated models with Record bodies kept as RawValue."""
    with temporalio.nexus.system._user_converter_context(
        temporalio.nexus.system._SystemNexusUserConverters(
            payload_converter=_RawPayloadConverter(),
            failure_converter=failure_converter,
        )
    ):
        yield


async def encode(
    converter: temporalio.converter.DataConverter, value: Any
) -> temporalio.common.RawValue:
    """Convert and encode one value for a Record body."""
    [payload] = await converter.encode([value])
    return temporalio.common.RawValue(payload)


async def decode(
    converter: temporalio.converter.DataConverter, body: Any, type_hint: Any
) -> Any:
    """Decode a Record body that a read gave as RawValue."""
    if not isinstance(body, temporalio.common.RawValue):
        return body
    [value] = await converter.decode([body.payload], [type_hint] if type_hint else None)
    return value
