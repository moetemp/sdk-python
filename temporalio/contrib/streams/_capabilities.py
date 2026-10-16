"""What a store can do beyond the ordered log that every store keeps."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio.bridge.proto.streams import StoreCapabilities as StoreCapabilitiesProto

__all__ = ["StoreCapabilities"]


@dataclass(frozen=True)
class StoreCapabilities:
    """The capabilities of a store, as Core reports them.

    Every store keeps an ordered log per topic, and Core applies the same
    reader rules on every store. A capability changes what a producer is
    told and how much the store keeps. Without one, Core takes a fallback,
    with a weaker guarantee that ``docs/store-capabilities.md`` names.

    As a mask for :class:`temporalio.contrib.streams.memory.MemoryStreams`,
    a field set to ``False`` turns that capability off. A mask can't turn
    on what the store doesn't have.
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
    """How long the store keeps every record for sure, or ``None`` when it says nothing."""
    max_record_bytes: int = 0
    """The largest record the store takes, or 0 for the store's own limit."""
    max_batch_bytes: int = 0
    """The largest batch the store takes, or 0 for the store's own limit."""

    @staticmethod
    def _from_proto(proto: StoreCapabilitiesProto) -> StoreCapabilities:
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

    def _to_proto(self) -> StoreCapabilitiesProto:
        proto = StoreCapabilitiesProto(
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
