"""Scenario 1c on Bridge: a relay across namespaces.

As n1b, but ``Delivery`` and the stream operation's handler run in namespace
``streams-b``, and the caller ``Order`` runs in ``default``. The endpoint
targets B's task queue, and only B's Worker holds B's store. ``Order``
reaches ``Delivery``'s stream through the endpoint alone. It republishes on
its own stream, in a store of its own namespace, which the backend
long-polls.

Left out: records in progress callbacks, as in n1b. The handler and the
caller share a payload codec, since B decodes each body and the read result
crosses once encoded with B's codec.

Run against the live lane's server with ``TEMPORAL_ADDRESS=127.0.0.1:7881
python -m tests.contrib.streams.samples.scenarios.n1c_cross_namespace_relay``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.streams import StreamRecord, get_stream_handle
from temporalio.contrib.streams.nexus import TemporalStreamsHandler
from temporalio.worker import Worker
from tests.contrib.streams.samples.scenarios._common import follow, values
from tests.contrib.streams.samples.scenarios._nexus import (
    NAMESPACE_B,
    NewStore,
    connect,
    endpoint,
    run,
    two_namespaces,
)
from tests.contrib.streams.samples.scenarios.n1b_parent_relay import (
    RELAY,
    Delivery,
    DeliveryHandler,
    relay,
)


@workflow.defn
class RemoteOrder:
    """Relays the stages of a delivery that runs in another namespace."""

    @workflow.run
    async def run(self, endpoint: str, delivery_id: str) -> str:
        # The handler starts the delivery in its own namespace.
        return await relay(endpoint, delivery_id)


@dataclass
class Outcome:
    relayed: list[str]
    close_result: str
    delivery_namespace: str


async def main(
    client: Client,
    task_queue: str,
    *,
    workflow_id: str = "order-n1c",
    new_store: NewStore,
) -> Outcome:
    """Run one order across namespaces and return what the backend read.

    ``client`` is a client of the ``default`` namespace of a server with the
    stream notifier and the namespace ``streams-b``.
    """
    b = await two_namespaces(client, new_store().notify_on_append())
    a = await connect(client, new_store())
    b_queue = f"{task_queue}-b"
    name = await endpoint(client, b_queue, namespace=NAMESPACE_B)
    delivery_id = f"{workflow_id}-delivery"
    async with (
        Worker(
            b,
            task_queue=b_queue,
            workflows=[Delivery],
            nexus_service_handlers=[DeliveryHandler(), TemporalStreamsHandler()],
        ),
        Worker(a, task_queue=task_queue, workflows=[RemoteOrder]),
    ):
        handle = await a.start_workflow(
            RemoteOrder.run,
            args=[name, delivery_id],
            id=workflow_id,
            task_queue=task_queue,
        )
        seen: list[StreamRecord[str]] = []
        await follow(
            "backend", get_stream_handle(a, workflow_id).read(topic=RELAY), seen
        )
        result = await asyncio.wait_for(handle.result(), 30)
        described = await b.get_workflow_handle(delivery_id).describe()
        return Outcome(
            relayed=values(seen),
            close_result=result,
            delivery_namespace=described.namespace,
        )


if __name__ == "__main__":
    run(main)
