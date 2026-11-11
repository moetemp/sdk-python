"""Runs the chat session on the scripted model, on the memory Store and on Redis."""

import uuid

import httpx

from temporalio import streams
from temporalio.client import Client, WorkflowHandle
from temporalio.streams.superseded import Superseded, with_superseded
from temporalio.worker import Worker
from tests.contrib.streams.examples.chat_app import server
from tests.contrib.streams.examples.chat_app.activities import (
    FAIL_AFTER,
    ModelActivities,
)
from tests.contrib.streams.examples.chat_app.models.fake import FakeModel
from tests.contrib.streams.examples.chat_app.shared import (
    ROUNDS_PER_RUN,
    SessionInfo,
    SessionInput,
    SessionStreams,
    SharedTurn,
    Token,
    Turn,
    UserMessage,
)
from tests.contrib.streams.examples.chat_app.tools import ToolActivities
from tests.contrib.streams.examples.chat_app.workflow import ChatSession, Researcher
from tests.helpers import new_worker


def chat_worker(client: Client, work_scale: float = 0) -> Worker:
    return new_worker(
        client,
        ChatSession,
        Researcher,
        activities=[
            ModelActivities(FakeModel(delay=0)).call_model,
            ToolActivities(work_scale).run_tool,
        ],
    )


async def start(client: Client, worker: Worker) -> WorkflowHandle[ChatSession, None]:
    session_id = f"chat-{uuid.uuid4()}"
    return await client.start_workflow(
        ChatSession.run,
        SessionInput(session_id),
        id=session_id,
        task_queue=worker.task_queue,
    )


async def read_until_answer(
    read: streams.Reader[Turn], number: int
) -> list[streams.Record[Turn]]:
    records = []
    async for item in read:
        if isinstance(item, streams.Record):
            records.append(item)
            if item.data.number == number and item.data.final:
                break
    return records


async def answer(
    client: Client, worker: Worker, text: str
) -> tuple[WorkflowHandle[ChatSession, None], list[streams.Record[Turn]]]:
    """Starts a session, sends one message, and reads its turns up to the answer."""
    handle = await start(client, worker)
    refs = await handle.query(ChatSession.streams)
    await handle.execute_update(ChatSession.send, UserMessage(text))
    read = client.get_stream(
        refs.turns.id,
        payload_type=Turn,
    ).read()
    records = await read_until_answer(read, 1)
    await read.aclose()
    return handle, records


async def test_turn_and_its_tokens(stream_client: Client):
    async with chat_worker(stream_client) as worker:
        handle = await start(stream_client, worker)
        refs = await handle.query(ChatSession.streams)
        offset = await handle.execute_update(ChatSession.send, UserMessage("hello"))
        await handle.signal(ChatSession.end)
        await handle.result()
    # The session closed both Streams, so each read ends.
    turns = stream_client.get_stream(
        refs.turns.id,
        payload_type=Turn,
    )
    records = [r async for r in turns.read() if isinstance(r, streams.Record)]
    tokens = stream_client.get_stream(
        refs.tokens.id,
        payload_type=Token,
    )
    deltas = [r.data async for r in tokens.read() if isinstance(r, streams.Record)]
    system, user, assistant, ended = records
    assert system.data == Turn("system", "Run 1 of the session started", 0)
    assert user.data == Turn("user", "hello", 1) and str(user.offset) == offset
    assert assistant.data.text.startswith('You said "hello".')
    assert (assistant.data.round, assistant.data.final) == (1, True)
    assert "".join(d.text for d in deltas) == assistant.data.text
    assert {(d.number, d.round) for d in deltas} == {(1, 1)}
    assert ended.data == Turn("system", "The chat ended", 1)


async def test_tool_rounds(stream_client: Client):
    async with chat_worker(stream_client) as worker:
        handle, records = await answer(
            stream_client,
            worker,
            "Roll two dice, then add them up with the calculator.",
        )
        await handle.signal(ChatSession.end)
    turns = [r.data for r in records[1:]]
    assert [(t.role, t.round, t.tool) for t in turns] == [
        ("user", 0, None),
        ("assistant", 1, None),
        ("tool_call", 1, "roll_dice"),
        ("tool_progress", 1, "roll_dice"),
        ("tool_result", 1, "roll_dice"),
        ("assistant", 2, None),
        ("tool_call", 2, "calculator"),
        ("tool_progress", 2, "calculator"),
        ("tool_result", 2, "calculator"),
        ("assistant", 3, None),
    ]
    assert turns[-1].final and not any(t.final for t in turns[:-1])


async def test_parallel_tools_interleave_their_turns(stream_client: Client):
    async with chat_worker(stream_client, work_scale=1) as worker:
        handle, records = await answer(
            stream_client, worker, "What time is it, and what is 12 * 34?"
        )
        await handle.signal(ChatSession.end)
    calls = [r.data.tool for r in records if r.data.role == "tool_call"]
    assert calls == ["clock", "calculator"]
    # Each tool Activity is its own Producer. Its Turns keep their order, and the two tools'
    # Turns interleave.
    written = [r for r in records if r.data.role in ("tool_progress", "tool_result")]
    assert [(r.data.tool, r.data.role) for r in written] == [
        ("clock", "tool_progress"),
        ("calculator", "tool_progress"),
        ("clock", "tool_result"),
        ("calculator", "tool_result"),
    ] or [(r.data.tool, r.data.role) for r in written] == [
        ("calculator", "tool_progress"),
        ("clock", "tool_progress"),
        ("clock", "tool_result"),
        ("calculator", "tool_result"),
    ]
    assert len({r.producer.id for r in written if r.producer}) == 2
    assert records[-1].data.text.endswith("12 * 34 = 408. ")


async def test_message_during_a_run_joins_the_next_round(stream_client: Client):
    async with chat_worker(stream_client, work_scale=1) as worker:
        handle = await start(stream_client, worker)
        refs = await handle.query(ChatSession.streams)
        await handle.execute_update(
            ChatSession.send, UserMessage("Search the docs for retention.")
        )
        read = stream_client.get_stream(
            refs.turns.id,
            payload_type=Turn,
        ).read()
        records = []
        async for item in read:
            if isinstance(item, streams.Record):
                records.append(item.data)
                if item.data.role == "tool_call":
                    # The search runs for a while, and the user writes during it.
                    await handle.signal(
                        ChatSession.add_message, UserMessage("How long?")
                    )
                    break
        records += [r.data for r in await read_until_answer(read, 1)]
        await read.aclose()
        await handle.signal(ChatSession.end)
    assert Turn("user", "How long?", 1) in records
    searches = [t.text for t in records if t.role == "tool_call"]
    assert searches == [
        "search_docs(Search the docs for retention.)",
        "search_docs(How long?)",
    ]
    assert records[-1].final and records[-1].round == 3


async def test_sub_agent_writes_its_own_streams(stream_client: Client):
    async with chat_worker(stream_client) as worker:
        handle, records = await answer(
            stream_client, worker, "Use the research tool to look into continue-as-new."
        )
        await handle.signal(ChatSession.end)
    roles = [r.data.role for r in records[1:]]
    assert roles == [
        "user",
        "assistant",
        "tool_call",
        "agent",
        "tool_result",
        "assistant",
    ]
    [link] = [r.data for r in records if r.data.role == "agent"]
    assert link.agent is not None
    # The sub-agent closed its Streams, so each read ends.
    child = stream_client.get_stream(
        link.agent.turns.id,
        payload_type=Turn,
    )
    child_turns = [r.data async for r in child.read() if isinstance(r, streams.Record)]
    assert [(t.role, t.tool) for t in child_turns] == [
        ("user", None),
        ("assistant", None),
        ("tool_call", "search_docs"),
        ("tool_progress", "search_docs"),
        ("tool_result", "search_docs"),
        ("assistant", None),
    ]
    tokens = stream_client.get_stream(
        link.agent.tokens.id,
        payload_type=Token,
    )
    assert [r async for r in tokens.read() if isinstance(r, streams.Record)]
    assert records[-1].data.text.startswith("The sub-agent reports back.")


async def failed_attempt(
    client: Client,
) -> tuple[SessionStreams, list[streams.Record[Token] | Superseded | streams.Closed]]:
    """The refs of a session whose one round failed its first attempt, and the token items."""
    async with chat_worker(client) as worker:
        handle = await start(client, worker)
        refs = await handle.query(ChatSession.streams)
        await handle.execute_update(
            ChatSession.send, UserMessage("hello", fail_first_attempt=True)
        )
        await handle.signal(ChatSession.end)
        await handle.result()
    tokens = client.get_stream(
        refs.tokens.id,
        payload_type=Token,
    )
    return refs, [item async for item in with_superseded(tokens.read())]


async def test_retry_withdraws_the_failed_attempt(stream_client: Client):
    _, items = await failed_attempt(stream_client)
    [superseded] = [i for i in items if isinstance(i, Superseded)]
    records = [i for i in items if isinstance(i, streams.Record)]
    withdrawn = [r for r in records if r.offset in superseded.withdrawn_offsets]
    kept = [r for r in records if r.offset not in superseded.withdrawn_offsets]
    assert len(withdrawn) == FAIL_AFTER
    assert all(superseded.withdraws(r) for r in withdrawn)
    assert not any(superseded.withdraws(r) for r in kept)
    # The retry streamed the whole round again.
    assert "".join(r.data.text for r in kept).startswith(
        "".join(r.data.text for r in withdrawn)
    )


async def test_resume_from_a_saved_offset(stream_client: Client):
    async with chat_worker(stream_client) as worker:
        handle = await start(stream_client, worker)
        refs = await handle.query(ChatSession.streams)
        turns = stream_client.get_stream(
            refs.turns.id,
            payload_type=Turn,
        )
        await handle.execute_update(ChatSession.send, UserMessage("message 1"))
        read = turns.read()
        await read_until_answer(read, 1)
        saved = read.last_offset
        await read.aclose()
        # Each of these answers takes one round, so the last one comes from the run after
        # continue-as-new.
        last = ROUNDS_PER_RUN + 1
        for number in range(2, last + 1):
            await handle.execute_update(
                ChatSession.send, UserMessage(f"message {number}")
            )
        resumed = turns.read(offset=saved)
        later = [r.data for r in await read_until_answer(resumed, last)]
        await resumed.aclose()
        assert await handle.query(ChatSession.streams) == refs
        await handle.signal(ChatSession.end)
        await handle.result()
    assert later[0] == Turn("user", "message 2", 2)
    assert [t.number for t in later if t.final] == list(range(2, last + 1))
    assert any(t.text == "Run 2 of the session started" for t in later)


async def test_share_again_is_a_strict_create_of_an_existing_id(stream_client: Client):
    async with chat_worker(stream_client) as worker:
        lobby = await stream_client.create_stream(
            f"lobby-{uuid.uuid4()}", payload_type=SessionInfo
        )
        server.app.state.chat = server.Chat(
            stream_client, lobby.ref, worker.task_queue, "test", "fake", "scripted"
        )
        transport = httpx.ASGITransport(app=server.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://app"
        ) as http:
            session_id = (await http.post("/api/sessions", json={})).json()[
                "session_id"
            ]
            await http.post(f"/api/sessions/{session_id}/messages", json={"text": "hi"})
            handle = stream_client.get_workflow_handle(session_id)
            refs = await handle.query(ChatSession.streams)
            turns = stream_client.get_stream(
                refs.turns.id,
                payload_type=Turn,
            )
            read = turns.read()
            await read_until_answer(read, 1)
            await read.aclose()
            chat = [
                r
                async for r in turns.read(wait_new_records=False)
                if isinstance(r, streams.Record)
            ]
            first = (await http.post(f"/api/sessions/{session_id}/share")).json()
            # The same point of the chat gives the same id, whose snapshot is closed.
            again = (await http.post(f"/api/sessions/{session_id}/share")).json()
            info = await http.get(f"/api/shares/{first['share_id']}")
            missing = await http.get(f"/api/shares/{session_id}-snapshot-1")
            outdated = await http.get(f"/api/shares/{session_id}-share-1")
            await http.post(f"/api/sessions/{session_id}/end")
        await handle.result()
    assert again == first
    assert info.status_code == 200 and missing.status_code == 404
    assert info.json()["session_id"] == session_id
    assert outdated.json()["outdated"] is True
    # The read ends at the snapshot's close. Each copy names its Turn's offset in the chat.
    snapshot = stream_client.get_stream(first["share_id"], payload_type=SharedTurn)
    copies = [r.data async for r in snapshot.read() if isinstance(r, streams.Record)]
    assert [c.turn for c in copies] == [r.data for r in chat]
    assert [c.source_offset for c in copies] == [str(r.offset) for r in chat]
    assert {c.source_producer.kind for c in copies if c.source_producer} == {
        "local activity"
    }
