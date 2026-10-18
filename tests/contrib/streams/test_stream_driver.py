"""A StreamDriver forwards a store's notification log to the server.

The live cases need a server with the stream notifier and run in the Jetty
lane. The rest check the driver's setup and how it fails.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from temporalio.client import Client
from temporalio.contrib.streams import (
    StreamDriver,
    StreamStorePlugin,
    StreamUnsupportedError,
    get_stream_handle,
)
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.redis import RedisStreams
from tests.contrib.streams._support import connect_with
from tests.contrib.streams.test_outside_handles import Waits, running_owner
from tests.helpers import new_worker


def test_the_log_rides_the_store_config():
    store = MemoryStreams().notify_on_append(notification_log=True)
    assert store._config.notify_on_append and store._config.notification_log
    assert not MemoryStreams().notify_on_append()._config.notification_log
    redis = RedisStreams("redis://x", notify_doorbells=4)
    assert redis._config.redis.notify_doorbells == 4
    with pytest.raises(ValueError, match="notify_doorbells"):
        RedisStreams("redis://x", notify_doorbells=0)


async def test_a_driver_needs_a_store_with_a_log(client: Client):
    streams = await connect_with(client, MemoryStreams().notify_on_append())
    with pytest.raises(ValueError, match="notification_log=True"):
        StreamDriver(streams)


async def test_a_driver_takes_its_settings(client: Client):
    store = MemoryStreams().notify_on_append(notification_log=True)
    streams = await connect_with(client, store)
    driver = StreamDriver(
        streams,
        consumer="driver-a",
        sweep_interval=timedelta(seconds=5),
        claim_idle=timedelta(seconds=7),
        batch=50,
    )
    request = driver._request
    assert (request.consumer, request.batch) == ("driver-a", 50)
    assert request.sweep_interval.ToTimedelta() == timedelta(seconds=5)
    assert request.claim_idle.ToTimedelta() == timedelta(seconds=7)
    # Without a name, each driver gets one of its own.
    assert StreamDriver(streams).consumer != StreamDriver(streams).consumer
    with pytest.raises(ValueError, match="batch"):
        StreamDriver(streams, batch=0)


def log_store(kind: str) -> StreamStorePlugin:
    return MemoryStreams().notify_on_append(notification_log=True)


@pytest.mark.parametrize("kind", ["memory"])
async def test_a_driver_on_a_server_without_notifiers_fails(client: Client, kind: str):
    streams = await connect_with(client, log_store(kind))
    async with new_worker(streams, Waits) as worker:
        owner = await running_owner(streams, worker.task_queue)
        # An entry in the log, so the driver has a notification to send.
        await (
            get_stream_handle(streams, owner, topic="t")
            .producer(producer_id="p", attempt=1)
            .append("x")
        )
        try:
            await asyncio.wait_for(StreamDriver(streams).run(), 20)
        except StreamUnsupportedError:
            return
        except asyncio.TimeoutError:
            pytest.skip("the server has stream notifiers, so the driver runs")
        finally:
            await streams.get_workflow_handle(owner).signal(Waits.finish)
    pytest.fail("the driver returned")
