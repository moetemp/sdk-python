"""Positions-only Workflow reads: History keeps what a read read, not the records.

A reader with ``replay_from_store=True``, the default, reads through the same Local
Activity, but the Activity's result, and so its marker, holds the read's
range and Core's digest of its records. Before Workflow code sees the
result, the Worker turns it back into the batch. It takes the batch the
Activity cached in this process, or else asks Core's ``ReadRange`` for the
same range, which checks it against the digest. So Workflow code gets the
same records in both modes, and no I/O runs in Workflow code.

The step runs as part of the client's payload codec, on decode. The store
plugin wraps the codec the client has, and every other payload passes
through to it.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import temporalio.workflow
from temporalio.api.common.v1 import Payload, Payloads
from temporalio.bridge.proto.streams import ReadRangeRequest, ReadResponse
from temporalio.contrib.streams._body import decode_body
from temporalio.contrib.streams._errors import (
    StreamReplayMismatchError,
    StreamUnsupportedError,
)
from temporalio.converter import (
    DataConverter,
    PayloadCodec,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)

if TYPE_CHECKING:
    from temporalio.contrib.streams._plugin import StreamStorePlugin

__all__ = ["POSITIONS_TYPE", "ReplayReadCodec", "positions_payload"]

POSITIONS_TYPE = b"temporal.io/stream-read-positions"
"""The ``messageType`` of a positions-only read's result."""

_RESPONSE_TYPE = ReadResponse.DESCRIPTOR.full_name.encode()
_READ_RUN_ID_KEY = "temporal.io/stream-read-run-id"
_READ_BY_RUN_ID_KEY = "temporal.io/stream-read-by-run-id"
# Batches the read Activity read in this process, so the live Workflow Task
# turns its result back without a second store read.
_CACHE_SIZE = 256


class BatchCache:
    """The newest batches positions-only reads read in this process."""

    def __init__(self, size: int = _CACHE_SIZE) -> None:
        """Keep up to ``size`` batches."""
        self._size = size
        self._batches: OrderedDict[bytes, bytes] = OrderedDict()

    @staticmethod
    def key(range: ReadRangeRequest) -> bytes:
        """The key of one read: its holder, its range and its digest."""
        return b"\0".join(
            [
                range.floor_holder.encode(),
                range.after.encode(),
                range.to.encode(),
                range.records_digest,
            ]
        )

    def put(self, range: ReadRangeRequest, response: ReadResponse) -> None:
        """Keep ``response``, whose bodies are already decoded."""
        key = self.key(range)
        self._batches[key] = response.SerializeToString()
        self._batches.move_to_end(key)
        while len(self._batches) > self._size:
            self._batches.popitem(last=False)

    def take(self, range: ReadRangeRequest) -> ReadResponse | None:
        """The batch of ``range``, which stays cached for a later replay."""
        data = self._batches.get(self.key(range))
        return ReadResponse.FromString(data) if data is not None else None

    def clear(self) -> None:
        """Drop every batch, so the next decode reads from the store."""
        self._batches.clear()


def positions_payload(
    range: ReadRangeRequest,
    answer: ReadResponse,
    read_run_id: str,
    read_by: str | None = None,
) -> Payload:
    """The result a positions-only read records: its range, and its answer without records.

    ``read_by`` is the run that read, which a reset run tells from its own.
    """
    without_records = ReadResponse()
    without_records.CopyFrom(answer)
    del without_records.records[:]
    payload = Payload(
        metadata={
            "encoding": b"binary/protobuf",
            "messageType": POSITIONS_TYPE,
            _READ_RUN_ID_KEY: read_run_id.encode(),
        },
        data=Payloads(
            payloads=[
                Payload(data=range.SerializeToString()),
                Payload(data=without_records.SerializeToString()),
            ]
        ).SerializeToString(),
    )
    if read_by is not None:
        payload.metadata[_READ_BY_RUN_ID_KEY] = read_by.encode()
    return payload


def batch_payload(
    response: ReadResponse, read_run_id: str, read_by: str | None = None
) -> Payload:
    """The result a recorded read records, and what a positions-only one turns back into."""
    payload = Payload(
        metadata={
            "encoding": b"binary/protobuf",
            "messageType": _RESPONSE_TYPE,
            _READ_RUN_ID_KEY: read_run_id.encode(),
        },
        data=response.SerializeToString(),
    )
    if read_by is not None:
        payload.metadata[_READ_BY_RUN_ID_KEY] = read_by.encode()
    return payload


def is_positions(payload: Payload) -> bool:
    """Whether ``payload`` is a positions-only read's result that no codec turned back."""
    return payload.metadata.get("messageType") == POSITIONS_TYPE


class ReplayReadCodec(PayloadCodec, WithSerializationContext):
    """The client's codec, with the positions-only reads turned back into batches on decode."""

    def __init__(
        self,
        plugin: StreamStorePlugin,
        inner: PayloadCodec | None,
        body_converter: DataConverter,
    ) -> None:
        """Wrap ``inner``. ``body_converter`` decodes record bodies, as the read Activity does."""
        self._plugin = plugin
        self._inner = inner
        self._body_converter = body_converter

    @property
    def inner(self) -> PayloadCodec | None:
        """The codec this one wraps."""
        return self._inner

    def with_context(self, context: SerializationContext) -> ReplayReadCodec:
        """Pass the context to the wrapped codec."""
        inner = self._inner
        if isinstance(inner, WithSerializationContext):
            inner = inner.with_context(context)
        return ReplayReadCodec(self._plugin, inner, self._body_converter)

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Encode with the wrapped codec."""
        if self._inner is None:
            return list(payloads)
        return await self._inner.encode(payloads)

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        """Decode with the wrapped codec, then turn positions-only reads back into batches.

        Raises:
            temporalio.workflow.NondeterminismError: The store holds other
                records in the range than the read found.
            StreamReplayRangeLostError: The store no longer holds the range.
                The Workflow Task fails and is retried.
            StreamUnsupportedError: No client connected the stream store in
                this process.
        """
        decoded = (
            list(payloads)
            if self._inner is None
            else await self._inner.decode(payloads)
        )
        return [await self._batch(p) if is_positions(p) else p for p in decoded]

    async def _batch(self, payload: Payload) -> Payload:
        parts = Payloads.FromString(payload.data).payloads
        range = ReadRangeRequest.FromString(parts[0].data)
        answer = ReadResponse.FromString(parts[1].data)
        read_run_id = payload.metadata.get(_READ_RUN_ID_KEY, b"").decode()
        read_by = payload.metadata.get(_READ_BY_RUN_ID_KEY)
        read_by_text = read_by.decode() if read_by is not None else None
        if range.stored_records == 0:
            # A read with no stored record has no record at all, since a
            # SUPERSEDED record comes only before a stored one.
            return batch_payload(answer, read_run_id, read_by_text)
        cached = self._plugin._batches.take(range)
        if cached is not None:
            return batch_payload(cached, read_run_id, read_by_text)
        records = await self._read_range(range)
        answer.records.extend(records.records)
        return batch_payload(answer, read_run_id, read_by_text)

    async def _read_range(self, range: ReadRangeRequest) -> ReadResponse:
        # Imported here, since the plugin module imports this one.
        from temporalio.contrib.streams._plugin import call

        service = self._plugin._service
        if service is None:
            raise StreamUnsupportedError(
                "a stream read with replay_from_store=True (the default) replays "
                "from the store, and no client connected the stream store in this "
                "process; connect a client with the store, or pass "
                "replay_from_store=False to record each batch in History"
            )
        try:
            response = await call(service.read_range(range))
        except StreamReplayMismatchError as error:
            raise temporalio.workflow.NondeterminismError(
                f"a positions-only stream read replayed other records: {error}"
            ) from error
        converter = self._body_converter.with_context(
            WorkflowSerializationContext(
                namespace=range.stream.namespace, workflow_id=range.stream.workflow_id
            )
        )
        for record in response.records:
            if record.HasField("stored") and record.stored.HasField("body"):
                body = await decode_body(converter, record.stored.body)
                record.stored.body.CopyFrom(body)
        return response


def wrap_codec(plugin: StreamStorePlugin, converter: Any) -> DataConverter:
    """``converter`` with its codec wrapped, once, by a :class:`ReplayReadCodec`."""
    if isinstance(converter.payload_codec, ReplayReadCodec):
        return converter
    import dataclasses

    return dataclasses.replace(
        converter,
        payload_codec=ReplayReadCodec(plugin, converter.payload_codec, converter),
    )
