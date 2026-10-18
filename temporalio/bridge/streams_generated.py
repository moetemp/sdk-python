# Generated file. DO NOT EDIT
"""Generated calls for the stream runtime's in-process StreamService."""

from __future__ import annotations

from typing import TYPE_CHECKING

import temporalio.api.workflowservice.v1
import temporalio.bridge.proto.streams

if TYPE_CHECKING:
    from temporalio.bridge.streams import StreamStore


class StreamService:
    """Calls for the StreamService, which the stream runtime serves in process."""

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

    async def describe_stream_notifier(
        self, req: temporalio.api.workflowservice.v1.DescribeStreamNotifierRequest
    ) -> temporalio.api.workflowservice.v1.DescribeStreamNotifierResponse:
        """Invokes the StreamService.describe_stream_notifier call."""
        return await self._store.call(
            "DescribeStreamNotifier",
            req,
            temporalio.api.workflowservice.v1.DescribeStreamNotifierResponse,
        )

    async def flush_notifications(
        self, req: temporalio.bridge.proto.streams.FlushNotificationsRequest
    ) -> temporalio.bridge.proto.streams.FlushNotificationsResponse:
        """Invokes the StreamService.flush_notifications call."""
        return await self._store.call(
            "FlushNotifications",
            req,
            temporalio.bridge.proto.streams.FlushNotificationsResponse,
        )

    async def follow(
        self, req: temporalio.bridge.proto.streams.FollowRequest
    ) -> temporalio.bridge.proto.streams.FollowResponse:
        """Invokes the StreamService.follow call."""
        return await self._store.call(
            "Follow", req, temporalio.bridge.proto.streams.FollowResponse
        )

    async def follow_range(
        self, req: temporalio.bridge.proto.streams.FollowRangeRequest
    ) -> temporalio.bridge.proto.streams.FollowResponse:
        """Invokes the StreamService.follow_range call."""
        return await self._store.call(
            "FollowRange", req, temporalio.bridge.proto.streams.FollowResponse
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

    async def read_range(
        self, req: temporalio.bridge.proto.streams.ReadRangeRequest
    ) -> temporalio.bridge.proto.streams.ReadResponse:
        """Invokes the StreamService.read_range call."""
        return await self._store.call(
            "ReadRange", req, temporalio.bridge.proto.streams.ReadResponse
        )

    async def release_floor(
        self, req: temporalio.bridge.proto.streams.ReleaseFloorRequest
    ) -> temporalio.bridge.proto.streams.ReleaseFloorResponse:
        """Invokes the StreamService.release_floor call."""
        return await self._store.call(
            "ReleaseFloor", req, temporalio.bridge.proto.streams.ReleaseFloorResponse
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

    async def run_stream_driver(
        self, req: temporalio.bridge.proto.streams.RunStreamDriverRequest
    ) -> temporalio.bridge.proto.streams.RunStreamDriverResponse:
        """Invokes the StreamService.run_stream_driver call."""
        return await self._store.call(
            "RunStreamDriver",
            req,
            temporalio.bridge.proto.streams.RunStreamDriverResponse,
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

    async def subscribe(
        self, req: temporalio.bridge.proto.streams.SubscribeRequest
    ) -> temporalio.bridge.proto.streams.SubscribeResponse:
        """Invokes the StreamService.subscribe call."""
        return await self._store.call(
            "Subscribe", req, temporalio.bridge.proto.streams.SubscribeResponse
        )

    async def unsubscribe(
        self, req: temporalio.bridge.proto.streams.UnsubscribeRequest
    ) -> temporalio.bridge.proto.streams.UnsubscribeResponse:
        """Invokes the StreamService.unsubscribe call."""
        return await self._store.call(
            "Unsubscribe", req, temporalio.bridge.proto.streams.UnsubscribeResponse
        )

    async def wait_for_notification(
        self, req: temporalio.bridge.proto.streams.WaitForNotificationRequest
    ) -> temporalio.bridge.proto.streams.WaitForNotificationResponse:
        """Invokes the StreamService.wait_for_notification call."""
        return await self._store.call(
            "WaitForNotification",
            req,
            temporalio.bridge.proto.streams.WaitForNotificationResponse,
        )
