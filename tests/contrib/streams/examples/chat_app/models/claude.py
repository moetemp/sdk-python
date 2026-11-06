"""Claude through the Messages API, one streamed round with tool use.

The Workflow runs the agent loop, since each round and each tool is an Activity there, so this
module makes one request per round.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, cast

# The SDK's dev environment has anthropic only on some Python versions.
import anthropic  # type: ignore[import-not-found]

from temporalio.exceptions import ApplicationError

from ..shared import Message, ToolCall
from ..tools import TOOL_SPECS
from . import SYSTEM_PROMPT, RawContent, RoundEvent


def _history(messages: list[Message]) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for m in messages:
        if m.role == "user":
            history.append({"role": "user", "content": m.text})
        elif m.role == "tool":
            results = [
                {
                    "type": "tool_result",
                    "tool_use_id": r.call_id,
                    "content": r.text,
                    "is_error": r.error,
                }
                for r in m.results
            ]
            history.append({"role": "user", "content": results})
        elif m.raw is not None:
            # A round's own blocks go back unchanged, so Claude keeps its thinking.
            history.append({"role": "assistant", "content": m.raw})
        else:
            # A round of another provider, such as before a restart with a new provider.
            blocks: list[dict[str, Any]] = [{"type": "text", "text": m.text or "..."}]
            blocks += [
                {"type": "tool_use", "id": c.id, "name": c.name, "input": c.args}
                for c in m.tool_calls
            ]
            history.append({"role": "assistant", "content": blocks})
    return history


def _echo(content: list[Any]) -> list[dict[str, Any]]:
    """The blocks of a round, as the next request sends them back."""
    fallback = max(
        (i for i, b in enumerate(content) if b.type == "fallback"), default=-1
    )
    blocks: list[dict[str, Any]] = []
    for i, b in enumerate(content):
        if b.type == "text":
            blocks.append({"type": "text", "text": b.text})
        elif i < fallback:
            # Before a fallback, only text goes back, since the fallback model can't read the
            # declined model's other blocks.
            continue
        elif b.type == "thinking":
            blocks.append(
                {"type": "thinking", "thinking": b.thinking, "signature": b.signature}
            )
        elif b.type == "redacted_thinking":
            blocks.append({"type": "redacted_thinking", "data": b.data})
        elif b.type == "tool_use":
            blocks.append(
                {"type": "tool_use", "id": b.id, "name": b.name, "input": b.input}
            )
    return blocks


class ClaudeModel:
    """Streams a round from Claude. The client reads ``ANTHROPIC_API_KEY`` itself."""

    provider = "claude"

    def __init__(self, name: str) -> None:
        self.name = name
        self._client = anthropic.AsyncAnthropic()

    async def stream(
        self, messages: list[Message], tools: list[str]
    ) -> AsyncGenerator[RoundEvent, None]:
        specs = [
            {
                "name": name,
                "description": TOOL_SPECS[name]["description"],
                "input_schema": TOOL_SPECS[name]["schema"],
                # Inputs stream as they're made, and each tool checks its own input.
                "eager_input_streaming": True,
            }
            for name in tools
        ]
        try:
            async with self._client.beta.messages.stream(
                model=self.name,
                max_tokens=64000,
                system=SYSTEM_PROMPT,
                messages=cast(Any, _history(messages)),
                tools=cast(Any, specs),
                # A chat round needs little thinking, and this model can't turn thinking off.
                output_config={"effort": "low"},
                # A declined request runs again on the model that Anthropic picks for its
                # category, in the same call.
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",
            ) as stream:
                async for text in stream.text_stream:
                    yield text
                reply = await stream.get_final_message()
        except anthropic.APIStatusError as err:
            # An API error's text can quote the request, and History and the turns Stream keep
            # the failure. So the failure has only the error's class, and no cause.
            raise ApplicationError(
                f"The Claude call failed with {type(err).__name__}",
                type=type(err).__name__,
                non_retryable=err.status_code < 500 and err.status_code != 429,
            ) from None
        except anthropic.APIConnectionError as err:
            raise ApplicationError(
                f"The Claude call failed with {type(err).__name__}",
                type=type(err).__name__,
            ) from None
        except ValueError:
            # A streamed tool input that isn't JSON at all, so the attempt asks again.
            raise ApplicationError(
                "Claude sent a tool input that isn't JSON", type="InvalidToolInput"
            ) from None
        if reply.stop_reason == "refusal":
            # The whole fallback chain declined, so the text so far isn't an answer, and no
            # tool of the round runs.
            raise ApplicationError(
                "Claude declined to answer this message",
                type="Refusal",
                non_retryable=True,
            )
        calls = [b for b in reply.content if b.type == "tool_use"]
        if calls and reply.stop_reason == "max_tokens":
            # A cut-off tool input can still look complete, so no tool of the round runs.
            raise ApplicationError(
                "Claude's round was cut off in a tool call",
                type="MaxTokens",
                non_retryable=True,
            )
        for b in calls:
            yield ToolCall(b.id, b.name, b.input if isinstance(b.input, dict) else {})
        yield RawContent(_echo(list(reply.content)))
