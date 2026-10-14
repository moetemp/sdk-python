"""A Nexus operation whose start hands its caller a stream.

The handler attaches the caller's callback to the stream's notifier on the
server and puts the stream reference in the operation token. Core notifies
the notifier after each append, a Workflow's flushes included, and closing
the stream completes the operation with the close result.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest

import temporalio.nexus
from temporalio import workflow
from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import (
    DescribeStreamNotifierRequest,
)
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams import (
    RecordKind,
    StreamRef,
    StreamStorePlugin,
    close_workflow_stream,
    get_stream_handle,
    workflow_writer,
)
from temporalio.contrib.streams._plugin import store_for_client
from temporalio.contrib.streams.memory import MemoryStreams
from temporalio.contrib.streams.nexus import (
    StreamOperationHandler,
    stream_ref_from_token,
)
from temporalio.exceptions import ApplicationError, NexusOperationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from temporalio.worker._nexus import _NexusTaskCancellation
from tests.contrib.streams._support import vendored_core_nexus
from tests.helpers.nexus import make_nexus_endpoint_name

REF = StreamRef.for_workflow("owner-1", topic="tokens")


def start_context(callback_url: str | None = "temporal://system") -> Any:
    return nexusrpc.handler.StartOperationContext(
        service="ChatService",
        operation="chat",
        headers={},
        task_cancellation=_NexusTaskCancellation(),
        request_id="attach-1",
        callback_url=callback_url,
        callback_headers={"nexus-callback-token": "abc"},
    )


def cancel_context() -> Any:
    return nexusrpc.handler.CancelOperationContext(
        service="ChatService",
        operation="chat",
        headers={},
        task_cancellation=_NexusTaskCancellation(),
    )


# Core attaches and detaches the caller and mints the token. Its tests in
# crates/streams/src/nexus/operation.rs cover the attach, the token, a pinned
# ref and the chain a cancel detaches from. These cover what stays in lang.


async def test_a_start_without_a_callback_is_refused_before_it_opens_the_stream() -> (
    None
):
    opened: list[str] = []

    async def open_stream(_ctx: Any, prompt: str) -> StreamRef:
        opened.append(prompt)
        return REF

    with pytest.raises(nexusrpc.HandlerError) as raised:
        await StreamOperationHandler(open_stream).start(
            start_context(callback_url=None), "hi"
        )

    assert raised.value.type == nexusrpc.HandlerErrorType.BAD_REQUEST
    assert not opened


def _raised(error: nexusrpc.HandlerError) -> tuple[str, bool | None, str | None]:
    """The type, retryability and stream condition class a caller sees."""
    failure = error.original_failure
    details: Mapping[str, Any] = (failure.details if failure else None) or {}
    cause = details.get("cause", {}).get("applicationFailureInfo", {})
    return error.type.name, error.retryable_override, cause.get("type")


async def test_a_start_on_a_worker_without_a_store_fails_before_it_opens_the_stream(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("temporalio.nexus.client", lambda: client)
    opened: list[str] = []

    async def open_stream(_ctx: Any, prompt: str) -> StreamRef:
        opened.append(prompt)
        return REF

    with pytest.raises(nexusrpc.HandlerError) as raised:
        await StreamOperationHandler(open_stream).start(start_context(), "hi")

    # The handler's configuration is at fault, not the caller's request.
    assert raised.value.type == nexusrpc.HandlerErrorType.INTERNAL
    assert not opened


async def test_a_start_for_an_owner_that_does_not_exist_is_not_found(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client = await connect(client, MemoryStreams())
    monkeypatch.setattr("temporalio.nexus.client", lambda: store_client)

    async def open_missing(_ctx: Any, _prompt: str) -> StreamRef:
        return StreamRef.for_workflow(f"missing-{uuid.uuid4()}", topic="tokens")

    with pytest.raises(nexusrpc.HandlerError) as raised:
        await StreamOperationHandler(open_missing).start(start_context(), "hi")

    # Core's table, as the stream service's operations answer with it.
    assert _raised(raised.value) == ("NOT_FOUND", False, "StreamNotFoundError")


async def test_a_cancel_with_a_token_that_names_no_stream_is_a_bad_request(
    client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_client = await connect(client, MemoryStreams())
    monkeypatch.setattr("temporalio.nexus.client", lambda: store_client)

    async def open_stream(_ctx: Any, _prompt: str) -> StreamRef:
        return REF

    with pytest.raises(nexusrpc.HandlerError) as raised:
        await StreamOperationHandler(open_stream).cancel(
            cancel_context(), "not-a-token"
        )

    assert _raised(raised.value) == ("BAD_REQUEST", False, "ValueError")


def test_a_token_reads_as_the_shared_vectors_say() -> None:
    vectors = json.loads(
        (vendored_core_nexus() / "operation_token_vectors.json").read_text()
    )
    for vector in vectors["vectors"] + vectors["decode_only"]:
        ref = vector["ref"]
        assert stream_ref_from_token(vector["token"]) == StreamRef(
            kind=ref["kind"],
            workflow_id=ref["workflow_id"],
            chain_run_id=ref.get("chain_run_id"),
            topic=ref["topic"],
        )
    for token in vectors["refused"]:
        with pytest.raises(ValueError):
            stream_ref_from_token(token)


# Live: a caller Workflow sees the stream's progress and the close result.
# These need a server with Nexus progress and the stream notifier, and skip on
# any other server.


@nexusrpc.service
class ChatService:
    chat: nexusrpc.Operation[str, str]


@dataclass
class Observed:
    counters: list[int]
    result: str


@workflow.defn(name="StreamOperationCaller")
class StreamOperationCaller:
    def __init__(self) -> None:
        self._counters: list[int] = []

    @workflow.run
    async def run(self, endpoint: str) -> Observed:
        nexus_client = workflow.create_nexus_client(
            service=ChatService, endpoint=endpoint
        )
        handle = await nexus_client.start_operation(ChatService.chat, "hello")
        last = 0
        while (progress := await handle.progress(after_counter=last)) is not None:
            self._counters.append(progress.counter)
            last = progress.counter
        return Observed(counters=list(self._counters), result=await handle)

    @workflow.query
    def counters(self) -> list[int]:
        return list(self._counters)


@workflow.defn(name="OwnerUntilDone")
class OwnerUntilDone:
    """Owns a stream until signaled done."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


async def connect(client: Client, store: StreamStorePlugin) -> Client:
    """A client of the test server that carries ``store``."""
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        plugins=[store],
    )


async def _skip_without_notifier(client: Client, ref: StreamRef) -> None:
    try:
        await client.workflow_service.describe_stream_notifier(
            DescribeStreamNotifierRequest(
                namespace=client.namespace,
                stream_ref=StreamReference(
                    owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                    workflow_id=ref.workflow_id,
                    topic=ref.topic,
                ),
            )
        )
    except RPCError as err:
        if err.status == RPCStatusCode.UNIMPLEMENTED:
            pytest.skip(f"server has no stream notifier: {err.message}")
        # A server that keys the notifier by run chain refuses the probe's
        # empty run id, which still shows it has the notifier.
        if err.status not in (
            RPCStatusCode.NOT_FOUND,
            RPCStatusCode.INVALID_ARGUMENT,
        ):
            raise


async def _wait_for(predicate: Any, attempts: int = 100) -> Any:
    for _ in range(attempts):
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.1)
    return None


def _store_for(backing: str) -> StreamStorePlugin:
    if backing == "memory":
        return MemoryStreams().notify_on_append()
    url = os.environ.get("STREAMS_REDIS_URL")
    if not url:
        pytest.skip("set STREAMS_REDIS_URL to run the Redis cases")
    from temporalio.contrib.streams.redis import RedisStreams

    return RedisStreams(url, key_prefix=f"test-{uuid.uuid4().hex}").notify_on_append()


async def _values(client: Client, ref: StreamRef) -> list[Any]:
    return [
        record.value
        async for record in get_stream_handle(client, ref).read()
        if record.kind is RecordKind.DATA
    ]


async def test_a_caller_sees_appends_as_progress_and_the_close_as_the_result(
    client: Client, env: WorkflowEnvironment
) -> None:
    ref = StreamRef.for_workflow(f"owner-{uuid.uuid4()}", topic="tokens")
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-operation-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    store = MemoryStreams().notify_on_append()
    streams_client = await connect(client, store)
    started = asyncio.Event()

    async def open_owned_stream(_ctx: Any, _prompt: str) -> StreamRef:
        # The notifier is keyed by the owner's run chain, so the owner runs.
        await streams_client.start_workflow(
            OwnerUntilDone.run, id=ref.workflow_id, task_queue=task_queue
        )
        started.set()
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(open_owned_stream)

    async with Worker(
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, OwnerUntilDone],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        handle = await streams_client.start_workflow(
            StreamOperationCaller.run,
            endpoint,
            id=f"stream-operation-caller-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        await asyncio.wait_for(started.wait(), 10)
        # Progress that reaches the caller before its started event is dropped
        # (DD-43), so the first append waits for the operation to be started.

        async def operation_started() -> bool:
            history = await handle.fetch_history()
            return any(
                event.HasField("nexus_operation_started_event_attributes")
                for event in history.events
            )

        assert await _wait_for(operation_started)
        producer = get_stream_handle(streams_client, ref).producer(
            producer_id="writer", attempt=1
        )
        await producer.append("a")

        async def first_progress() -> list[int]:
            return await handle.query(StreamOperationCaller.counters)

        assert await _wait_for(first_progress), "the caller never saw progress"
        await producer.append("b", "c")
        await store.close_stream(streams_client, ref, "done")
        await store.flush_notifications()

        observed = await asyncio.wait_for(handle.result(), 20)
        await streams_client.get_workflow_handle(ref.workflow_id).signal(
            OwnerUntilDone.done
        )

    assert observed.result == "done"
    assert observed.counters, observed
    assert observed.counters == sorted(set(observed.counters)), observed


TOKENS_TOPIC = "tokens"


@workflow.defn(name="StreamOwnerThatPublishes")
class StreamOwnerThatPublishes:
    """Publishes its tokens on its own stream, then waits to be told to end."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self, tokens: list[str]) -> None:
        writer = workflow_writer(TOKENS_TOPIC)
        for token in tokens:
            writer.publish(token)
            await workflow.sleep(timedelta(milliseconds=200))
        await writer.finish()
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


async def test_a_workflows_own_publishes_reach_the_caller_as_progress(
    client: Client, env: WorkflowEnvironment
) -> None:
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-owner-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    # The caller waits on progress and never reads, so it never arms the stream.
    store = MemoryStreams().notify_on_append(every_append=True)
    streams_client = await connect(client, store)

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            StreamOwnerThatPublishes.run,
            ["a", "b", "c"],
            id=owner_id,
            task_queue=task_queue,
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, StreamOwnerThatPublishes],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        caller = await streams_client.start_workflow(
            StreamOperationCaller.run,
            endpoint,
            id=f"stream-owner-caller-{uuid.uuid4()}",
            task_queue=task_queue,
        )

        # A Workflow's records are notified once the Local Activity of their
        # flush lands them.
        async def several_progresses() -> list[int] | None:
            counters = await caller.query(StreamOperationCaller.counters)
            return counters if len(counters) >= 2 else None

        assert await _wait_for(several_progresses), "the caller saw too little progress"
        await streams_client.get_workflow_handle(owner_id).signal(
            StreamOwnerThatPublishes.done
        )
        await store.close_stream(streams_client, ref, "3 tokens")
        observed = await asyncio.wait_for(caller.result(), 20)

    assert observed.result == "3 tokens"
    assert observed.counters == sorted(set(observed.counters)), observed


@workflow.defn(name="StreamOwnerThatCloses")
class StreamOwnerThatCloses:
    """Publishes its tokens on its own stream, then closes it from Workflow code."""

    @workflow.run
    async def run(self, tokens: list[str]) -> None:
        writer = workflow_writer(TOKENS_TOPIC)
        for token in tokens:
            writer.publish(token)
            await workflow.sleep(timedelta(milliseconds=200))
        await close_workflow_stream(TOKENS_TOPIC, result=f"{len(tokens)} tokens")
        # A second close of the topic keeps the first outcome.
        await close_workflow_stream(TOKENS_TOPIC, result="again")


async def test_a_workflow_closes_its_own_stream(
    client: Client, env: WorkflowEnvironment
) -> None:
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-closer-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    store = MemoryStreams().notify_on_append()
    streams_client = await connect(client, store)

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            StreamOwnerThatCloses.run,
            ["a", "b", "c"],
            id=owner_id,
            task_queue=task_queue,
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, StreamOwnerThatCloses],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        observed = await asyncio.wait_for(
            streams_client.execute_workflow(
                StreamOperationCaller.run,
                endpoint,
                id=f"stream-closer-caller-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            30,
        )
        owner_result = await streams_client.get_workflow_handle(owner_id).result()

    assert observed.result == "3 tokens"
    assert owner_result is None


@workflow.defn(name="OwnerThatClosesInOneTask")
class OwnerThatClosesInOneTask:
    """Publishes its tokens and closes its stream with no wait between, then
    keeps running, so only the close can end a read."""

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self, count: int) -> None:
        writer = workflow_writer(TOKENS_TOPIC)
        for n in range(count):
            writer.publish(f"t{n}")
        await close_workflow_stream(TOKENS_TOPIC, result=f"{count} tokens")
        await workflow.wait_condition(lambda: self._done)

    @workflow.signal
    def done(self) -> None:
        self._done = True


@pytest.mark.parametrize("backing", ["memory", "redis"])
async def test_a_close_right_after_the_last_publishes_comes_after_them(
    client: Client, env: WorkflowEnvironment, backing: str
) -> None:
    store = _store_for(backing)
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-closer-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    streams_client = await connect(client, store)

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            OwnerThatClosesInOneTask.run, 50, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, OwnerThatClosesInOneTask],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        observed = await asyncio.wait_for(
            streams_client.execute_workflow(
                StreamOperationCaller.run,
                endpoint,
                id=f"stream-closer-caller-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            30,
        )
        # The operation completed, so the store is closed and holds every
        # record: a read gets them all and ends while the owner still runs.
        values = await asyncio.wait_for(_values(streams_client, ref), 10)
        owner = streams_client.get_workflow_handle(owner_id)
        await owner.signal(OwnerThatClosesInOneTask.done)
        await owner.result()
        history = await owner.fetch_history()

    assert observed.result == "50 tokens"
    assert values == [f"t{n}" for n in range(50)]
    # The flushes and the close are Local Activity markers, so a replay needs
    # no store.
    await Replayer(
        workflows=[OwnerThatClosesInOneTask], plugins=[MemoryStreams()]
    ).replay_workflow(history)


async def test_a_worker_without_notifications_still_delivers_the_close_result(
    client: Client, env: WorkflowEnvironment
) -> None:
    # Notifications only gate progress. A close reaches the notifier whatever
    # the store config says, so the caller completes with its result.
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-closer-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    streams_client = await connect(client, MemoryStreams())

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            OwnerThatClosesInOneTask.run, 3, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        streams_client,
        task_queue=task_queue,
        workflows=[StreamOperationCaller, OwnerThatClosesInOneTask],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        observed = await asyncio.wait_for(
            streams_client.execute_workflow(
                StreamOperationCaller.run,
                endpoint,
                id=f"stream-closer-caller-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            30,
        )
        owner = streams_client.get_workflow_handle(owner_id)
        await owner.signal(OwnerThatClosesInOneTask.done)
        await owner.result()

    assert observed.result == "3 tokens"


# A close with an error, and an owner that ends without completing, fail the
# caller's operation as a Workflow-run operation fails (DD-107).


@workflow.defn(name="StreamFailureCaller")
class StreamFailureCaller:
    """Starts the operation and says how it ended."""

    @workflow.run
    async def run(self, endpoint: str) -> list[str]:
        nexus_client = workflow.create_nexus_client(
            service=ChatService, endpoint=endpoint
        )
        handle = await nexus_client.start_operation(ChatService.chat, "hello")
        try:
            result = await handle
        except NexusOperationError as err:
            cause = err.cause
            return [
                type(cause).__name__,
                getattr(cause, "type", None) or "",
                getattr(cause, "message", str(cause)),
            ]
        return ["succeeded", str(result)]


@workflow.defn(name="OwnerThatEndsAsTold")
class OwnerThatEndsAsTold:
    """Publishes once, then ends as its signal says: fail, close with an error, or finish."""

    def __init__(self) -> None:
        self._how: str | None = None

    @workflow.run
    async def run(self) -> None:
        workflow_writer(TOKENS_TOPIC).publish("token")
        await workflow.wait_condition(lambda: self._how is not None)
        if self._how == "fail":
            raise ApplicationError(
                "the owner gave up", type="OwnerGaveUp", non_retryable=True
            )
        if self._how == "close-with-error":
            await close_workflow_stream(
                TOKENS_TOPIC,
                error=ApplicationError("the agent gave up", type="AgentGaveUp"),
            )
            await workflow.wait_condition(lambda: self._how == "finish")

    @workflow.signal
    def end(self, how: str) -> None:
        self._how = how


async def _caller_outcome(
    client: Client,
    env: WorkflowEnvironment,
    end_owner: Callable[[Client, StreamRef], Awaitable[None]],
) -> list[str]:
    """Runs a caller against a stream of an `OwnerThatEndsAsTold`, ends the owner with
    `end_owner` once the caller's operation started, and returns how the operation ended.
    """
    owner_id = f"owner-{uuid.uuid4()}"
    ref = StreamRef.for_workflow(owner_id, topic=TOKENS_TOPIC)
    await _skip_without_notifier(client, ref)
    task_queue = f"stream-failure-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)
    streams_client = await connect(client, MemoryStreams())

    async def start_owner(_ctx: Any, _prompt: str) -> StreamRef:
        await temporalio.nexus.client().start_workflow(
            OwnerThatEndsAsTold.run, id=owner_id, task_queue=task_queue
        )
        return ref

    @nexusrpc.handler.service_handler(service=ChatService)
    class ChatServiceHandler:
        @nexusrpc.handler.operation_handler
        def chat(self) -> nexusrpc.handler.OperationHandler[str, str]:
            return StreamOperationHandler(start_owner)

    async with Worker(
        streams_client,
        task_queue=task_queue,
        workflows=[StreamFailureCaller, OwnerThatEndsAsTold],
        nexus_service_handlers=[ChatServiceHandler()],
    ):
        handle = await streams_client.start_workflow(
            StreamFailureCaller.run,
            endpoint,
            id=f"stream-failure-caller-{uuid.uuid4()}",
            task_queue=task_queue,
        )

        async def operation_started() -> bool:
            history = await handle.fetch_history()
            return any(
                event.HasField("nexus_operation_started_event_attributes")
                for event in history.events
            )

        assert await _wait_for(operation_started)
        await end_owner(streams_client, ref)
        outcome = await asyncio.wait_for(handle.result(), 30)
        owner = streams_client.get_workflow_handle(owner_id)
        if (await owner.describe()).status == WorkflowExecutionStatus.RUNNING:
            await owner.signal(OwnerThatEndsAsTold.end, "finish")
    return outcome


async def test_live_a_failed_owner_fails_the_operation(
    client: Client, env: WorkflowEnvironment
) -> None:
    async def fail(c: Client, ref: StreamRef) -> None:
        await c.get_workflow_handle(ref.workflow_id).signal(
            OwnerThatEndsAsTold.end, "fail"
        )

    outcome = await _caller_outcome(client, env, fail)
    # No Worker closes the stream when its owner fails. The notifier's owner
    # check does, and it reads no History, so it has no failure of the run's.
    assert outcome == [
        "ApplicationError",
        "StreamOwnerFailed",
        "the owner Workflow failed",
    ]


async def test_live_a_close_with_an_error_fails_the_operation(
    client: Client, env: WorkflowEnvironment
) -> None:
    async def close_with_error(c: Client, ref: StreamRef) -> None:
        await c.get_workflow_handle(ref.workflow_id).signal(
            OwnerThatEndsAsTold.end, "close-with-error"
        )

    outcome = await _caller_outcome(client, env, close_with_error)
    assert outcome == ["ApplicationError", "AgentGaveUp", "the agent gave up"]


async def test_live_an_outside_close_with_an_error_fails_the_operation(
    client: Client, env: WorkflowEnvironment
) -> None:
    async def close_from_outside(c: Client, ref: StreamRef) -> None:
        store = store_for_client(c)
        await store.close_stream(
            c, ref, error=ApplicationError("the moderator stopped it", type="Moderated")
        )

    outcome = await _caller_outcome(client, env, close_from_outside)
    assert outcome == ["ApplicationError", "Moderated", "the moderator stopped it"]


async def test_live_a_terminated_owner_fails_the_operation(
    client: Client, env: WorkflowEnvironment
) -> None:
    # No Worker runs a final task for a terminated owner, so the server's
    # owner check closes the stream, every streamnotifier.ownerCheckInterval.
    async def terminate(c: Client, ref: StreamRef) -> None:
        await c.get_workflow_handle(ref.workflow_id).terminate("stop")

    outcome = await _caller_outcome(client, env, terminate)
    assert outcome[0] == "TerminatedError", outcome
