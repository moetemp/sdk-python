"""OpenAI chat completions, one streamed round with tool calls."""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any, cast

import openai

from temporalio.exceptions import ApplicationError

from ..shared import Message, ToolCall
from ..tools import TOOL_SPECS
from . import SYSTEM_PROMPT, RoundEvent


def _chat(messages: list[Message]) -> list[dict[str, Any]]:
    chat: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    for m in messages:
        if m.role == "user":
            chat.append({"role": "user", "content": m.text})
        elif m.role == "tool":
            chat += [
                {"role": "tool", "tool_call_id": r.call_id, "content": r.text}
                for r in m.results
            ]
        else:
            message: dict[str, Any] = {"role": "assistant", "content": m.text or None}
            if m.tool_calls:
                message["tool_calls"] = [
                    {
                        "id": c.id,
                        "type": "function",
                        "function": {"name": c.name, "arguments": json.dumps(c.args)},
                    }
                    for c in m.tool_calls
                ]
            chat.append(message)
    return chat


class OpenAIModel:
    """Streams a round from the OpenAI API. The client reads ``OPENAI_API_KEY`` itself."""

    provider = "openai"

    def __init__(self, name: str) -> None:
        self.name = name
        self._client = openai.AsyncOpenAI()

    async def stream(
        self, messages: list[Message], tools: list[str]
    ) -> AsyncGenerator[RoundEvent, None]:
        specs = [
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": TOOL_SPECS[name]["description"],
                    "parameters": TOOL_SPECS[name]["schema"],
                },
            }
            for name in tools
        ]
        # A tool call streams in parts, keyed by its index in the round.
        calls: dict[int, dict[str, str]] = {}
        try:
            response = await self._client.chat.completions.create(
                model=self.name,
                messages=cast(Any, _chat(messages)),
                tools=cast(Any, specs),
                stream=True,
            )
            async for chunk in response:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                if delta.content:
                    yield delta.content
                for part in delta.tool_calls or []:
                    call = calls.setdefault(
                        part.index, {"id": "", "name": "", "args": ""}
                    )
                    call["id"] = part.id or call["id"]
                    if part.function:
                        call["name"] += part.function.name or ""
                        call["args"] += part.function.arguments or ""
        except openai.APIError as err:
            # An API error's text can quote the key, and History and the turns Stream keep the
            # failure. So the failure has only the error's class, and no cause.
            raise ApplicationError(
                f"The OpenAI call failed with {type(err).__name__}",
                type=type(err).__name__,
                non_retryable=isinstance(err, openai.AuthenticationError),
            ) from None
        for index in sorted(calls):
            call = calls[index]
            try:
                args = json.loads(call["args"] or "{}")
            except ValueError:
                # The tool checks its input and reports what's missing.
                args = {}
            yield ToolCall(
                call["id"], call["name"], args if isinstance(args, dict) else {}
            )
