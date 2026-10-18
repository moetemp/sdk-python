"""The stream store of the ``temporalio-streams`` crate, reached in process.

The crate serves the stream service against one store per connect call. Python sends each call
as a serialized request and gets the serialized response back, so the typed calls in
:mod:`temporalio.bridge.streams_generated` are generated from the crate's protos.
"""

from __future__ import annotations

from typing import TypeVar

import google.protobuf.message

import temporalio.api.workflowservice.v1
import temporalio.bridge.client
import temporalio.bridge.proto.streams
import temporalio.bridge.temporal_sdk_bridge

ProtoMessage = TypeVar("ProtoMessage", bound=google.protobuf.message.Message)

STREAM_OWNER_HEADER = "__temporal_stream_owner"
"""The header that marks an Activity as the owner of its own stream.

When the scheduling Workflow sees a marked Activity end, the Worker passes
the resolution in its publish call, and the stream runtime closes the
Activity's streams. Only the key counts, since a header codec may encode the
value.
"""


class StreamCallFailure(Exception):
    """A stream call that failed, with the stream runtime's account of why."""

    def __init__(self, failure: temporalio.bridge.proto.streams.StreamFailure) -> None:
        """Wrap the stream runtime's failure."""
        super().__init__(failure.message)
        self.failure = failure


class StreamStore:
    """The stream store the stream runtime serves to this process."""

    @staticmethod
    async def connect(
        client: temporalio.bridge.client.Client,
        config: temporalio.bridge.proto.streams.StreamStoreConfig,
    ) -> StreamStore:
        """Connect to the store ``config`` names.

        The stream runtime asks ``client``'s server about streams' owners.

        Raises:
            StreamCallFailure: The stream runtime could not reach the store.
        """
        try:
            ref = await temporalio.bridge.temporal_sdk_bridge.connect_stream_store(
                client._ref, config.SerializeToString()
            )
        except temporalio.bridge.temporal_sdk_bridge.StreamFailureError as error:
            raise _call_failure(error) from None
        return StreamStore(ref)

    def __init__(
        self, ref: temporalio.bridge.temporal_sdk_bridge.StreamStoreRef
    ) -> None:
        """Wrap the bridge's store reference."""
        self.ref = ref

    async def call(
        self,
        rpc: str,
        req: google.protobuf.message.Message,
        resp_type: type[ProtoMessage],
    ) -> ProtoMessage:
        """Make one stream service call and decode its answer.

        Raises:
            StreamCallFailure: The call failed.
        """
        try:
            data = await self.ref.call(rpc, req.SerializeToString())
        except temporalio.bridge.temporal_sdk_bridge.StreamFailureError as error:
            raise _call_failure(error) from None
        resp = resp_type()
        resp.ParseFromString(data)
        return resp


async def execute_sync_nexus_operation(
    client: temporalio.bridge.client.Client,
    req: temporalio.api.workflowservice.v1.ExecuteSyncNexusOperationRequest,
) -> temporalio.api.workflowservice.v1.ExecuteSyncNexusOperationResponse:
    """Run one synchronous Nexus operation through ``client``'s server.

    Core's client has no stub for the RPC, so the stream runtime calls it by
    its method path.

    Raises:
        temporalio.bridge.client.RPCError: The call failed.
    """
    data = await temporalio.bridge.temporal_sdk_bridge.execute_sync_nexus_operation(
        client._ref, req.SerializeToString()
    )
    resp = temporalio.api.workflowservice.v1.ExecuteSyncNexusOperationResponse()
    resp.ParseFromString(data)
    return resp


def _call_failure(
    error: temporalio.bridge.temporal_sdk_bridge.StreamFailureError,
) -> StreamCallFailure:
    # The bridge raises with the serialized failure as its only argument.
    failure = temporalio.bridge.proto.streams.StreamFailure()
    failure.ParseFromString(error.args[0])
    return StreamCallFailure(failure)
