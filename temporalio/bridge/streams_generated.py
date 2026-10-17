# Generated file. DO NOT EDIT
"""Generated calls for Core's in-process StreamService."""

from __future__ import annotations

from typing import TYPE_CHECKING

import temporalio.bridge.proto.streams

if TYPE_CHECKING:
    from temporalio.bridge.streams import StreamStore


class StreamService:
    """Calls for the StreamService, which Core dispatches in process."""

    def __init__(self, store: StreamStore) -> None:
        """Initialize service with the provided store."""
        self._store = store

    async def append(
        self, req: temporalio.bridge.proto.streams.AppendRequest
    ) -> temporalio.bridge.proto.streams.AppendResponse:
        """Invokes the StreamService.append call."""
        return await self._store.call(
            "Append", req, temporalio.bridge.proto.streams.AppendResponse
        )

    async def close(
        self, req: temporalio.bridge.proto.streams.CloseRequest
    ) -> temporalio.bridge.proto.streams.CloseResponse:
        """Invokes the StreamService.close call."""
        return await self._store.call(
            "Close", req, temporalio.bridge.proto.streams.CloseResponse
        )

    async def delete_owner(
        self, req: temporalio.bridge.proto.streams.DeleteOwnerRequest
    ) -> temporalio.bridge.proto.streams.DeleteOwnerResponse:
        """Invokes the StreamService.delete_owner call."""
        return await self._store.call(
            "DeleteOwner", req, temporalio.bridge.proto.streams.DeleteOwnerResponse
        )

    async def describe_store(
        self, req: temporalio.bridge.proto.streams.DescribeStoreRequest
    ) -> temporalio.bridge.proto.streams.DescribeStoreResponse:
        """Invokes the StreamService.describe_store call."""
        return await self._store.call(
            "DescribeStore", req, temporalio.bridge.proto.streams.DescribeStoreResponse
        )

    async def latest(
        self, req: temporalio.bridge.proto.streams.LatestRequest
    ) -> temporalio.bridge.proto.streams.LatestResponse:
        """Invokes the StreamService.latest call."""
        return await self._store.call(
            "Latest", req, temporalio.bridge.proto.streams.LatestResponse
        )

    async def publish_workflow_task(
        self, req: temporalio.bridge.proto.streams.PublishWorkflowTaskRequest
    ) -> temporalio.bridge.proto.streams.PublishWorkflowTaskResponse:
        """Invokes the StreamService.publish_workflow_task call."""
        return await self._store.call(
            "PublishWorkflowTask",
            req,
            temporalio.bridge.proto.streams.PublishWorkflowTaskResponse,
        )

    async def read(
        self, req: temporalio.bridge.proto.streams.ReadRequest
    ) -> temporalio.bridge.proto.streams.ReadResponse:
        """Invokes the StreamService.read call."""
        return await self._store.call(
            "Read", req, temporalio.bridge.proto.streams.ReadResponse
        )

    async def retire_producer(
        self, req: temporalio.bridge.proto.streams.RetireProducerRequest
    ) -> temporalio.bridge.proto.streams.RetireProducerResponse:
        """Invokes the StreamService.retire_producer call."""
        return await self._store.call(
            "RetireProducer",
            req,
            temporalio.bridge.proto.streams.RetireProducerResponse,
        )

    async def run_evicted(
        self, req: temporalio.bridge.proto.streams.RunEvictedRequest
    ) -> temporalio.bridge.proto.streams.RunEvictedResponse:
        """Invokes the StreamService.run_evicted call."""
        return await self._store.call(
            "RunEvicted", req, temporalio.bridge.proto.streams.RunEvictedResponse
        )

    async def shutdown_worker(
        self, req: temporalio.bridge.proto.streams.ShutdownWorkerRequest
    ) -> temporalio.bridge.proto.streams.ShutdownWorkerResponse:
        """Invokes the StreamService.shutdown_worker call."""
        return await self._store.call(
            "ShutdownWorker",
            req,
            temporalio.bridge.proto.streams.ShutdownWorkerResponse,
        )
