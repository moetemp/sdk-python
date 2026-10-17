"""What a store can do beyond the ordered log that every store keeps."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import temporalio.bridge.proto.streams
from temporalio.bridge.proto.streams import StreamFailureKind
from temporalio.bridge.streams import StreamCallFailure, StreamStore

__all__ = ["StoreCapabilities"]


@dataclass(frozen=True)
class StoreCapabilities:
    """The capabilities of a store, as the stream runtime reports them.

    Every store keeps an ordered log per topic, and the stream runtime applies the same
    reader rules on every store. A capability changes what a producer is
    told and how much the store keeps. Without one, the stream runtime takes a fallback,
    with a weaker guarantee.

    As a mask for :class:`temporalio.contrib.streams.memory.MemoryStreams`,
    a field set to ``False`` turns that capability off. A mask can't turn
    on what the store doesn't have. A nonzero limit in a mask replaces the
    store's own, so a test can split batches early.
    """

    producer_state: bool = True
    """The store dedupes a producer's retry and refuses appends after a close.

    Without it, a retry can land a second copy, which readers drop, and an
    append that races a close can answer success, which no reader delivers.
    """
    conditional_tail: bool = True
    """An append can require the log's next position."""
    conditional_fence: bool = True
    """An append can require the stream's fence token."""
    kept_last: bool = True
    """The store knows a log's last position after retention dropped it all.

    Without it, a cursor whose record retention dropped reads as expired,
    also when nothing after it was lost.
    """
    holds: bool = True
    """A reader can hold records against retention."""
    change_feed: bool = True
    """One consumer can read every append across all logs."""
    cheap_logs: bool = True
    """The store keeps one log per topic of each run chain."""
    delete_owner: bool = True
    """The store deletes every log of one owner.

    Without it, ``delete_workflow_streams`` raises
    :class:`temporalio.contrib.streams.StreamUnsupportedError`.
    """
    topic_listing: bool = True
    """The store lists the topics of one run chain."""
    long_poll: bool = True
    """A read can wait in the store for the next record."""
    dense_positions: bool = True
    """Positions in a log have no gaps."""
    replay_horizon: timedelta | None = None
    """How long the store keeps every record for sure, or ``None`` if it doesn't say."""
    max_record_bytes: int = 0
    """The largest record the store takes, with its envelope.

    A larger record fails the append with ``ValueError``.
    """
    max_batch_bytes: int = 0
    """The most record bytes one append takes."""

    @staticmethod
    def _from_proto(proto: Any) -> StoreCapabilities:
        return StoreCapabilities(
            producer_state=proto.producer_state,
            conditional_tail=proto.conditional_tail,
            conditional_fence=proto.conditional_fence,
            kept_last=proto.kept_last,
            holds=proto.holds,
            change_feed=proto.change_feed,
            cheap_logs=proto.cheap_logs,
            delete_owner=proto.delete_owner,
            topic_listing=proto.topic_listing,
            long_poll=proto.long_poll,
            dense_positions=proto.dense_positions,
            replay_horizon=proto.replay_horizon.ToTimedelta()
            if proto.HasField("replay_horizon")
            else None,
            max_record_bytes=proto.max_record_bytes,
            max_batch_bytes=proto.max_batch_bytes,
        )

    def _to_proto(self) -> Any:
        """The mask as the stream runtime takes it.

        Raises:
            ValueError: The Core in this SDK can't mask a store.
        """
        proto_type = getattr(temporalio.bridge.proto.streams, "StoreCapabilities", None)
        if proto_type is None:
            raise ValueError("this SDK's Core can't mask a store's capabilities")
        proto = proto_type(
            producer_state=self.producer_state,
            conditional_tail=self.conditional_tail,
            conditional_fence=self.conditional_fence,
            kept_last=self.kept_last,
            holds=self.holds,
            change_feed=self.change_feed,
            cheap_logs=self.cheap_logs,
            delete_owner=self.delete_owner,
            topic_listing=self.topic_listing,
            long_poll=self.long_poll,
            dense_positions=self.dense_positions,
            max_record_bytes=self.max_record_bytes,
            max_batch_bytes=self.max_batch_bytes,
        )
        if self.replay_horizon is not None:
            proto.replay_horizon.FromTimedelta(self.replay_horizon)
        return proto


# What a Core from before DescribeStore serves. Its Redis and memory stores have
# every capability that Core knew of, and none had a conditional append.
_BEFORE_DESCRIBE = StoreCapabilities(conditional_tail=False, conditional_fence=False)


async def describe(store: StreamStore) -> StoreCapabilities:
    """What the stream runtime reports for ``store``.

    Raises:
        StreamCallFailure: the stream runtime failed the call.
    """
    proto = temporalio.bridge.proto.streams
    request = getattr(proto, "DescribeStoreRequest", None)
    response = getattr(proto, "DescribeStoreResponse", None)
    if request is None or response is None:
        return _BEFORE_DESCRIBE
    try:
        described = await store.call("DescribeStore", request(), response)
    except StreamCallFailure as failure:
        if failure.failure.kind == StreamFailureKind.STREAM_FAILURE_KIND_UNSUPPORTED:
            return _BEFORE_DESCRIBE
        raise
    return StoreCapabilities._from_proto(described.capabilities)
