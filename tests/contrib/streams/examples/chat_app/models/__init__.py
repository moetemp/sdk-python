"""The model providers. Each one streams one model round for a history and a set of tools."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any, Protocol

from ..shared import Message, ToolCall

SYSTEM_PROMPT = (
    "You are the assistant in a demo of Temporal Streams. Use the tools when they help, and "
    "keep each answer to one short paragraph."
)


@dataclass
class RawContent:
    """A provider's own blocks of the round, which go back to it in the next round's history."""

    blocks: list[dict[str, Any]]


RoundEvent = str | ToolCall | RawContent


class Model(Protocol):
    provider: str
    name: str

    def stream(
        self, messages: list[Message], tools: list[str]
    ) -> AsyncGenerator[RoundEvent, None]:
        """Yield the round's text deltas, then its tool calls, then optionally its raw blocks.

        A refused or failed round raises ``ApplicationError``.
        """
        ...


def pick_model() -> Model:
    """The provider of ``CHAT_MODEL_PROVIDER``, else the first one whose key is set.

    This code checks only whether a key variable is set. Each SDK reads its key itself.
    """
    provider = os.environ.get("CHAT_MODEL_PROVIDER") or (
        "claude"
        if "ANTHROPIC_API_KEY" in os.environ
        else "openai"
        if "OPENAI_API_KEY" in os.environ
        else "fake"
    )
    # Each provider imports its SDK, so a demo with the fake model needs neither SDK.
    if provider == "claude":
        from .claude import ClaudeModel

        return ClaudeModel(os.environ.get("ANTHROPIC_MODEL", "claude-opus-5-5"))
    if provider == "openai":
        from .openai_chat import OpenAIModel

        return OpenAIModel(os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
    if provider == "fake":
        from .fake import FakeModel

        return FakeModel()
    raise ValueError(
        f"CHAT_MODEL_PROVIDER is {provider!r}, and it takes claude, openai or fake"
    )
