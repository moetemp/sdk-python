"""The values that the chat Workflows, the Activities and the server pass around."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from temporalio import streams

# Low, so that the demo shows a continue-as-new after a few answers.
ROUNDS_PER_RUN = 6

# A model that keeps calling tools stops here, so a turn always ends.
MAX_ROUNDS = 8


@dataclass
class SubAgent:
    """The refs of a sub-agent's own Streams, which a Turn carries as data."""

    id: str
    turns: streams.StreamRef[Turn]
    tokens: streams.StreamRef[Token]


@dataclass
class Turn:
    """A Record of a turns Stream.

    ``number`` is the user turn that it belongs to, and ``round`` the model round in that turn.
    """

    role: Literal[
        "system",
        "user",
        "assistant",
        "tool_call",
        "tool_progress",
        "tool_result",
        "agent",
        "error",
    ]
    text: str
    number: int
    round: int = 0
    # The round's text is the answer, and the turn is done.
    final: bool = False
    tool: str | None = None
    call_id: str | None = None
    agent: SubAgent | None = None


@dataclass
class Token:
    """A Record of a tokens Stream, one model delta of a round."""

    number: int
    text: str
    round: int = 1


@dataclass
class ToolCall:
    id: str
    name: str
    args: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    call_id: str
    name: str
    text: str
    error: bool = False


@dataclass
class Message:
    """One message of a model's history."""

    role: Literal["user", "assistant", "tool"]
    text: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    results: list[ToolResult] = field(default_factory=list)
    # A provider's own blocks of an assistant message, such as Claude's thinking blocks, which go
    # back to the provider unchanged.
    raw: list[dict[str, Any]] | None = None


@dataclass
class ModelReply:
    text: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    raw: list[dict[str, Any]] | None = None


@dataclass
class UserMessage:
    text: str
    # The demo toggle. The model Activity fails its first attempt partway through the round.
    fail_first_attempt: bool = False


@dataclass
class Waiting:
    number: int
    message: UserMessage


@dataclass
class SessionStreams:
    """The refs of a session's Streams. They are data, so a query hands them out."""

    turns: streams.StreamRef[Turn]
    tokens: streams.StreamRef[Token]


@dataclass
class SessionInput:
    session_id: str
    history: list[Message] = field(default_factory=list)
    # The run before continue-as-new sets the rest.
    streams: SessionStreams | None = None
    run: int = 1
    number: int = 0
    waiting: list[Waiting] = field(default_factory=list)


@dataclass
class ModelInput:
    messages: list[Message]
    number: int
    round: int
    tokens: streams.StreamRef[Token]
    tools: list[str]
    fail_first_attempt: bool = False


@dataclass
class ToolInput:
    call: ToolCall
    number: int
    round: int
    turns: streams.StreamRef[Turn]


@dataclass
class ResearchInput:
    topic: str
    call: ToolCall
    number: int
    round: int
    # The parent's turns Stream, where the sub-agent writes its link and its result.
    parent_turns: streams.StreamRef[Turn]


@dataclass
class SourceProducer:
    """The Producer of a copied Record in its first Stream."""

    id: str
    kind: str
    attempt: int | None = None


@dataclass
class SharedTurn:
    """A Record of a snapshot, a copy of a Turn and where the Turn was in the chat's turns Stream.

    The snapshot is a new Stream that the server writes, so its own offsets and Producer differ.
    The source offset is the opaque offset's string form, which a reader parses back.
    """

    turn: Turn
    source_offset: str
    source_producer: SourceProducer | None = None


@dataclass
class SessionInfo:
    """A Record of the lobby Stream, which lists the chats."""

    session_id: str
    started: str


def turns_id(session_id: str) -> str:
    return f"{session_id}-turns"


def tokens_id(session_id: str) -> str:
    return f"{session_id}-tokens"
