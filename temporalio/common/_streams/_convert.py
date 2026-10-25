"""Value conversion for Stream Records outside Workflow code."""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from typing import Any

import temporalio.api.common.v1
import temporalio.common
import temporalio.converter
import temporalio.exceptions


class _RawPayloadConverter(temporalio.converter.PayloadConverter):
    # The generated client runs the codec on Record bodies and keeps them as RawValue, so the
    # handle converts each body with its Topic's type.
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


def raw(
    converter: temporalio.converter.DataConverter,
) -> temporalio.converter.DataConverter:
    """The converter with Record bodies kept as RawValue. Its codec still runs on them."""
    return dataclasses.replace(converter, payload_converter_class=_RawPayloadConverter)


def failure(
    converter: temporalio.converter.DataConverter, failure: BaseException
) -> BaseException:
    """Convert the RawValue payloads of a read's close failure with the user's converter."""
    if (
        not isinstance(failure, temporalio.exceptions.FailureError)
        or not failure.failure
    ):
        return failure
    return converter.failure_converter.from_failure(
        failure.failure, converter.payload_converter
    )


def decode(
    converter: temporalio.converter.DataConverter, body: Any, type_hint: Any
) -> Any:
    """Convert a Record body that a read gave as RawValue, after the codec."""
    if not isinstance(body, temporalio.common.RawValue):
        return body
    [value] = converter.payload_converter.from_payloads(
        [body.payload], [type_hint] if type_hint else None
    )
    return value
