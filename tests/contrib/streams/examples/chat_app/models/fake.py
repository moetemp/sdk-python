"""A scripted model, so the demo and the tests run with no API key.

It plans each round from the turn's first message and the tool results so far, so the scenario
prompts take the same rounds as with a real model.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncGenerator
from typing import Any

from ..shared import ROUNDS_PER_RUN, Message, ToolCall, ToolResult
from . import RoundEvent

CANNED_REPLIES = [
    "I'm the scripted model, so this demo runs with no API key. Each word that you see is a "
    "Record of the tokens Stream. The model Activity writes them through a BufferedAppender, "
    "which sends them in small batches.",
    "The Workflow publishes each turn to the turns Stream, and the Activity publishes the tokens "
    "to a Stream of their own. The browser reads both through the server, which sends each read "
    "as Server-Sent Events.",
    "Turn on the fail toggle and send a message. The first attempt fails partway through, and "
    "the retry's Superseded item withdraws the tokens of the failed attempt. Then the round "
    "streams again.",
    "Reload the page. The tab keeps its offset in each Stream, and the server resumes each read "
    "from it. A second tab is a second reader, and it reads the tokens from the end of the "
    "Stream.",
    f"After {ROUNDS_PER_RUN} model rounds, the Workflow continues as new. It forwards the refs of "
    "its Streams to the next run, so the chat keeps its Streams.",
]

Plan = tuple[str, list[tuple[str, dict[str, Any]]]]


def _turn(messages: list[Message]) -> list[Message]:
    # The turn starts after the last answer, an assistant message that calls no tool.
    start = 0
    for i, m in enumerate(messages[:-1]):
        if m.role == "assistant" and not m.tool_calls:
            start = i + 1
    return messages[start:]


def _numbers(text: str) -> list[int]:
    return [int(n) for n in re.findall(r"\d+", text)]


def plan(messages: list[Message], tools: list[str]) -> Plan:
    """The round's text and tool calls."""
    turn = _turn(messages)
    prompt = turn[0].text
    low = prompt.lower()
    results: list[ToolResult] = [r for m in turn for r in m.results]
    last = messages[-1]
    if last.role == "user" and last is not turn[0] and "search_docs" in tools:
        # A message that joined during the run gets a round of its own.
        return (
            f'You also asked "{last.text}". I\'ll search the docs for that too.',
            [("search_docs", {"query": last.text})],
        )
    if low.startswith("research "):
        topic = prompt[len("research ") :]
        if not results:
            return f"I'll search the docs for {topic}.", [
                ("search_docs", {"query": topic})
            ]
        return f"Here is what the docs say about {topic}. {results[-1].text}", []
    if "research" in low and "research" in tools:
        if not results:
            topic = "continue-as-new" if "continue" in low else prompt
            return "I'll ask a sub-agent to research that.", [
                ("research", {"topic": topic})
            ]
        return f"The sub-agent reports back. {results[-1].text}", []
    if "six" in low and ("dice" in low or "die" in low):
        rolls = [_numbers(r.text)[0] for r in results if r.name == "roll_dice"]
        if len(rolls) < 6:
            return f"Roll number {len(rolls) + 1}.", [("roll_dice", {"count": 1})]
        return f"The six rolls are {rolls}, and they add up to {sum(rolls)}.", []
    if "dice" in low:
        if not results:
            return "I'll roll two dice first.", [("roll_dice", {"count": 2})]
        if len(results) == 1:
            a, b = _numbers(results[0].text)[:2]
            return "Now I'll add them up.", [
                ("calculator", {"expression": f"{a} + {b}"})
            ]
        return f"The dice {results[0].text}, and {results[1].text}.", []
    if "clock" in low or "time" in low:
        if not results:
            expression = (
                " ".join(re.findall(r"\d+\s*[*+/-]\s*\d+", prompt)[:1]) or "12 * 34"
            )
            return "I'll ask the clock and the calculator at the same time.", [
                ("clock", {}),
                ("calculator", {"expression": expression}),
            ]
        by_name = {r.name: r.text for r in results}
        return f"It's {by_name['clock']}, and {by_name['calculator']}.", []
    if "search" in low or "docs" in low:
        if not results:
            return "I'll search the docs.", [("search_docs", {"query": prompt})]
        found = " ".join(r.text.split("\n")[0] for r in results)
        return f"Here is what I found. {found}", []
    asked = sum(1 for m in messages if m.role == "user")
    return (
        f'You said "{prompt}". {CANNED_REPLIES[(asked - 1) % len(CANNED_REPLIES)]}',
        [],
    )


class FakeModel:
    """Streams each planned round word by word."""

    provider = "fake"
    name = "scripted"

    def __init__(self, delay: float = 0.05) -> None:
        self._delay = delay

    async def stream(
        self, messages: list[Message], tools: list[str]
    ) -> AsyncGenerator[RoundEvent, None]:
        text, calls = plan(messages, tools)
        for word in text.split(" "):
            await asyncio.sleep(self._delay)
            yield word + " "
        for i, (name, args) in enumerate(calls):
            yield ToolCall(f"call_{len(messages)}_{i}", name, args)
