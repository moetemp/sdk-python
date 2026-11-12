"""The chat server. It starts or opens chats, and serves their Streams as Server-Sent Events.

It runs the sessions' Worker in its own process, so the in-memory backend works with no Redis.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import os
import re
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.sse import EventSourceResponse, ServerSentEvent
from pydantic import BaseModel

from temporalio import streams
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    Stream,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHandle,
    WorkflowUpdateFailedError,
)
from temporalio.common import WorkflowIDReusePolicy
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.converter import DataConverter
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.streams.superseded import Superseded, with_superseded
from temporalio.worker import Worker

from .activities import ModelActivities
from .models import pick_model
from .shared import (
    SessionInfo,
    SessionInput,
    SessionStreams,
    SharedTurn,
    SourceProducer,
    Token,
    Turn,
    UserMessage,
    tokens_id,
    turns_id,
)
from .tools import ToolActivities
from .workflow import ChatSession, Researcher

LOBBY_ID = "streams-chat-app-lobby"
INDEX = Path(__file__).parent / "static" / "index.html"
SESSION_ID = r"[a-z0-9][a-z0-9-]{0,39}"
SHARE_ID = rf"({SESSION_ID})-snapshot-([0-9a-f]*)"
# An older version of the app wrote snapshots of plain Turns under these ids.
OLD_SHARE_ID = rf"{SESSION_ID}-share-\d+"
# A sub-agent's Stream ids, which it picks from its Workflow ID and its run id.
AGENT_STREAM_ID = rf"{SESSION_ID}-agent-\d+-\d+-\d+-[0-9a-f]{{8}}-(turns|tokens)"
PAYLOADS = DataConverter.default.payload_converter
PRODUCER_METADATA = PAYLOADS.to_payload(streams.Producer(id="x")).metadata


@dataclasses.dataclass
class Chat:
    client: Client
    lobby: streams.StreamRef[SessionInfo]
    task_queue: str
    backend: str
    provider: str
    model: str


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    redis_url = os.environ.get("CHAT_APP_REDIS_URL")
    backend: streams.StreamBackend
    if redis_url:
        backend = RedisStreamBackend(redis_url)
    else:
        backend = MemoryStreamBackend()
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        stream_config=streams.StreamConfig(backends=[backend]),
    )
    # Each server start sends this create, so it gets the lobby that an earlier start made.
    lobby = await client.create_stream(
        LOBBY_ID,
        payload_type=SessionInfo,
        id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
    )
    model = pick_model()
    task_queue = "streams-chat-app"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ChatSession, Researcher],
        activities=[ModelActivities(model).call_model, ToolActivities().run_tool],
    ):
        app.state.chat = Chat(
            client,
            lobby.ref,
            task_queue,
            "redis" if redis_url else "memory",
            model.provider,
            model.name,
        )
        yield


app = FastAPI(lifespan=lifespan)


def chat_of(request: Request) -> Chat:
    return request.app.state.chat


ChatDep = Annotated[Chat, Depends(chat_of)]


def session_of(session_id: str, chat: ChatDep) -> WorkflowHandle[ChatSession, None]:
    if not re.fullmatch(SESSION_ID, session_id):
        raise HTTPException(400, "A chat name has lowercase letters, digits and dashes")
    return chat.client.get_workflow_handle_for(ChatSession.run, session_id)


SessionDep = Annotated[WorkflowHandle[ChatSession, None], Depends(session_of)]


async def streams_of(session: SessionDep) -> SessionStreams:
    try:
        return await session.query(ChatSession.streams)
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, f"There is no chat {session.id}") from err
        raise


StreamsDep = Annotated[SessionStreams, Depends(streams_of)]


def snapshot_of(share_id: str, chat: ChatDep) -> Stream[SharedTurn]:
    # A share link names a Stream id, so the server reads only the ids that it writes.
    if not re.fullmatch(SHARE_ID, share_id):
        raise HTTPException(404, f"There is no snapshot {share_id}")
    return chat.client.get_stream(share_id, payload_type=SharedTurn)


SnapshotDep = Annotated[Stream[SharedTurn], Depends(snapshot_of)]


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(INDEX)


@app.get("/share/{share_id}")
async def share_page() -> FileResponse:
    return FileResponse(INDEX)


class OpenRequest(BaseModel):
    session_id: str | None = None


@app.post("/api/sessions")
async def open_session(body: OpenRequest, chat: ChatDep) -> dict[str, Any]:
    session = session_of(body.session_id or f"chat-{uuid.uuid4().hex[:6]}", chat)
    try:
        # An ended chat keeps its id, so its page opens it to read.
        await chat.client.start_workflow(
            ChatSession.run,
            SessionInput(session.id),
            id=session.id,
            task_queue=chat.task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
        # A read of a Stream that nobody created fails, so the server creates the chat's
        # Streams before it hands out their ids. The Workflow's init creates them too, so both
        # use USE_EXISTING.
        await chat.client.create_stream(
            turns_id(session.id),
            payload_type=Turn,
            id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
        )
        await chat.client.create_stream(
            tokens_id(session.id),
            payload_type=Token,
            id_conflict_policy=streams.StreamIDConflictPolicy.USE_EXISTING,
        )
        started = datetime.now(timezone.utc).isoformat(timespec="seconds")
        await chat.client.get_stream(chat.lobby).append(
            SessionInfo(session.id, started)
        )
    except WorkflowAlreadyStartedError:
        pass
    refs = await session.query(ChatSession.streams)
    status = (await session.describe()).status
    if status == WorkflowExecutionStatus.FAILED:
        raise HTTPException(409, await failure_of(session))
    return {
        "session_id": session.id,
        "ended": status != WorkflowExecutionStatus.RUNNING,
        "turns": refs.turns.id,
        "tokens": refs.tokens.id,
        "backend": chat.backend,
        "provider": chat.provider,
        "model": chat.model,
    }


async def failure_of(session: WorkflowHandle[ChatSession, None]) -> str:
    try:
        await session.result()
    except WorkflowFailureError as err:
        return f"The chat {session.id} failed with {err.cause}"
    return f"The chat {session.id} failed"


class SendRequest(BaseModel):
    text: str
    fail_first_attempt: bool = False


@app.post("/api/sessions/{session_id}/messages")
async def send(body: SendRequest, session: SessionDep) -> dict[str, Any]:
    try:
        offset = await session.execute_update(
            ChatSession.send, UserMessage(body.text, body.fail_first_attempt)
        )
    except WorkflowUpdateFailedError as err:
        raise HTTPException(409, str(err.cause)) from err
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(409, f"The chat {session.id} has ended") from err
        raise
    return {"offset": str(offset)}


@app.post("/api/sessions/{session_id}/add")
async def add(body: SendRequest, session: SessionDep) -> dict[str, Any]:
    try:
        await session.signal(ChatSession.add_message, UserMessage(body.text))
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(409, f"The chat {session.id} has ended") from err
        raise
    return {}


@app.post("/api/sessions/{session_id}/end")
async def end(session: SessionDep) -> dict[str, Any]:
    try:
        await session.signal(ChatSession.end)
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(409, f"The chat {session.id} has ended") from err
        raise
    return {}


@app.post("/api/sessions/{session_id}/share")
async def share(session_id: str, refs: StreamsDep, chat: ChatDep) -> dict[str, Any]:
    turns = chat.client.get_stream(refs.turns.id, payload_type=Turn)
    # The snapshot holds the chat up to this offset, which names it.
    upto = await turns.get_offset()
    share_id = f"{session_id}-snapshot-{upto}"
    shown: dict[str, SharedTurn] = {}
    # with_superseded crosses out a failed attempt's Records, so the snapshot copies only what a
    # live reader would show.
    async with contextlib.aclosing(
        with_superseded(turns.read(wait_new_records=False))
    ) as read:
        async for item in read:
            if item.offset > upto:
                break
            if isinstance(item, Superseded):
                for offset in item.withdrawn_offsets:
                    del shown[str(offset)]
            elif isinstance(item, streams.Record):
                shown[str(item.offset)] = SharedTurn(
                    item.data, str(item.offset), source_producer(item.producer)
                )
    try:
        # A strict create, since a share id names one snapshot.
        async with await chat.client.create_stream(
            share_id, payload_type=SharedTurn
        ) as snapshot:
            await snapshot.append(*shown.values())
    except streams.StreamError as err:
        # CLOSED: this point of the chat has a snapshot, and its block closed it.
        # ALREADY_EXISTS: another request writes this snapshot now, and readers wait for its close.
        if err.reason not in (
            streams.StreamErrorReason.STREAM_ERROR_REASON_CLOSED,
            streams.StreamErrorReason.STREAM_ERROR_REASON_ALREADY_EXISTS,
        ):
            raise
    return {"share_id": share_id, "url": f"/share/{share_id}"}


@app.get("/api/shares/{share_id}")
async def share_info(share_id: str, chat: ChatDep) -> dict[str, Any]:
    if re.fullmatch(OLD_SHARE_ID, share_id):
        # Its Records have no origin and don't decode as SharedTurn, so the page asks for a share.
        return {"share_id": share_id, "outdated": True}
    snapshot = snapshot_of(share_id, chat)
    try:
        last_offset = await snapshot.get_offset()
    except streams.StreamError as err:
        if err.reason != streams.StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND:
            raise
        raise HTTPException(404, f"There is no snapshot {share_id}") from err
    # The id names the chat and the offset that the snapshot goes up to.
    named = re.fullmatch(SHARE_ID, share_id)
    assert named is not None
    session_id, upto = named.groups()
    return {
        "share_id": share_id,
        "session_id": session_id,
        "upto": upto,
        "last_offset": str(last_offset),
    }


@app.get("/api/shares/{share_id}/events", response_class=EventSourceResponse)
async def share_events(snapshot: SnapshotDep) -> AsyncIterable[ServerSentEvent]:
    # A snapshot is closed, so the read ends after its last Turn.
    async for event in events("turns", snapshot.read()):
        yield event


@app.get("/api/agents/events", response_class=EventSourceResponse)
async def agent_events(
    turns: str, tokens: str, chat: ChatDep
) -> AsyncIterable[ServerSentEvent]:
    # A page names a sub-agent's Streams by the ids in its Turn, so the server reads them by id.
    if not (
        re.fullmatch(AGENT_STREAM_ID, turns) and re.fullmatch(AGENT_STREAM_ID, tokens)
    ):
        raise HTTPException(404, "There is no such sub-agent")
    turns_read = chat.client.get_stream(turns, payload_type=Turn).read()
    tokens_read = chat.client.get_stream(tokens, payload_type=Token).read()
    merged = merge(events("sub-turns", turns_read), events("sub-tokens", tokens_read))
    async for event in merged:
        yield event


@app.get("/api/lobby", response_class=EventSourceResponse)
async def lobby_events(chat: ChatDep) -> AsyncIterable[ServerSentEvent]:
    async for event in events("lobby", lobby_read(chat)):
        yield event


# A browser opens at most six connections to a host, so a tab reads all its Streams over one.
@app.get("/api/sessions/{session_id}/events", response_class=EventSourceResponse)
async def session_events(
    refs: StreamsDep,
    chat: ChatDep,
    turns: str | None = None,
    tokens: str | None = None,
    kept: str | None = None,
) -> AsyncIterable[ServerSentEvent]:
    # The tab sends the offsets that it saved, so a reload or a reconnect resumes there.
    turns_stream = chat.client.get_stream(refs.turns.id, payload_type=Turn)
    turns_read = turns_stream.read(
        offset=streams.StreamOffset.parse(turns) if turns else None
    )
    tokens_stream = chat.client.get_stream(refs.tokens.id, payload_type=Token)
    # A tab that joins reads only new tokens, since the turns Stream has the earlier replies.
    tokens_read = tokens_stream.read(
        offset=streams.StreamOffset.parse(tokens) if tokens else streams.END
    )
    merged = merge(
        events("lobby", lobby_read(chat)),
        events("turns", turns_read),
        events("tokens", tokens_read, kept_producers(kept), supersede=True),
    )
    async for event in merged:
        yield event


def lobby_read(chat: Chat) -> streams.Reader[SessionInfo]:
    lobby = chat.client.get_stream(chat.lobby.id, payload_type=SessionInfo)
    # Every chat, oldest first, then each new one.
    return lobby.read()


async def merge(
    *sources: AsyncIterator[ServerSentEvent],
) -> AsyncIterator[ServerSentEvent]:
    # A bound makes a fast read wait for the connection, as a read of its own would.
    queue: asyncio.Queue[ServerSentEvent] = asyncio.Queue(maxsize=100)

    async def pump(source: AsyncIterator[ServerSentEvent]) -> None:
        async for event in source:
            await queue.put(event)

    tasks = [asyncio.create_task(pump(source)) for source in sources]
    try:
        while True:
            yield await queue.get()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def events(
    name: str,
    read: streams.Reader[Any],
    kept: Sequence[streams.Producer] = (),
    *,
    supersede: bool = False,
) -> AsyncIterator[ServerSentEvent]:
    # The tokens read opts into with_superseded, which crosses out a failed attempt's tokens from
    # the Producer epochs. The base read's last_offset is where a later read resumes.
    items = with_superseded(read) if supersede else read
    try:
        async for item in items:
            # The close ends the read, which the "end" event marks.
            if isinstance(item, streams.Closed):
                continue
            data = {"stream": name, **item_json(item, kept)}
            # Where a later read of this Stream resumes, which the tab saves.
            data["last_offset"] = (
                str(read.last_offset) if read.last_offset is not None else None
            )
            yield ServerSentEvent(event="item", data=data)
        yield ServerSentEvent(event="end", data={"stream": name})
    except Exception as err:
        failure = {"stream": name, "error": type(err).__name__, "message": str(err)}
        yield ServerSentEvent(event="failed", data=failure)


def item_json(
    item: streams.Record[Any] | Superseded, kept: Sequence[streams.Producer]
) -> dict[str, Any]:
    # Offsets are past the range of a JavaScript number, so they go as strings.
    if isinstance(item, Superseded):
        return {
            "kind": "superseded",
            "offset": str(item.offset),
            "producer": producer_json(item.producer),
            "withdrawn_offsets": [str(o) for o in item.withdrawn_offsets],
            # A tab sends the Producers of the Records that it kept from before a reload.
            "withdrawn_kept": [i for i, p in enumerate(kept) if item.withdraws(p)],
        }
    return {
        "kind": "record",
        "offset": str(item.offset),
        "producer": producer_json(item.producer),
        "data": data_json(item.data),
    }


def plain(value: Any) -> Any:
    """A value with each ref in it as JSON, such as the refs of a sub-agent in a Turn."""
    if isinstance(value, streams.StreamRef):
        return {"id": value.id, "backend": value.backend}
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


def data_json(value: Any) -> dict[str, Any]:
    # A SharedTurn already holds its source offset as the opaque offset's string form.
    return plain(dataclasses.asdict(value))


def producer_json(producer: streams.Producer | None) -> dict[str, Any] | None:
    if producer is None:
        return None
    kind, attempt = epoch_of(producer)
    return {
        "id": producer.id,
        "kind": kind,
        "attempt": attempt,
        # The tab sends it back after a reload, so the server can check its kept Records.
        "wire": json.loads(PAYLOADS.to_payload(producer).data),
    }


def source_producer(producer: streams.Producer | None) -> SourceProducer | None:
    if producer is None:
        return None
    kind, attempt = epoch_of(producer)
    return SourceProducer(producer.id, kind, attempt)


def epoch_of(producer: streams.Producer) -> tuple[str, int | None]:
    match producer.kind:
        case streams.ProducerKindWorkflow():
            return "workflow", None
        case streams.ProducerKindLocalActivity(value=local):
            return "local activity", local.epoch.attempt if local.epoch else None
        case (
            streams.ProducerKindWorkflowActivity(value=act)
            | streams.ProducerKindStandaloneActivity(value=act)
        ):
            return "activity", act.epoch.attempt if act.epoch else None
        case streams.ProducerKindClient():
            return "client", None
        case _:
            return "unknown", None


def kept_producers(kept: str | None) -> list[streams.Producer]:
    if not kept:
        return []
    return [
        PAYLOADS.from_payload(
            Payload(metadata=PRODUCER_METADATA, data=json.dumps(wire).encode()),
            streams.Producer,
        )
        for wire in json.loads(kept)
    ]
