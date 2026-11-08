"""The agent's tools. Each one is local and makes no network call.

``research`` starts a sub-agent, so the Workflow runs it as a child Workflow. The others run in
the ``run_tool`` Activity, which writes the tool's Turns to the chat's turns Stream itself.
"""

from __future__ import annotations

import ast
import asyncio
import operator
import random
from datetime import datetime, timezone
from typing import Any

from temporalio import activity

from . import docs
from .shared import ToolInput, ToolResult, Turn

TOOL_SPECS: dict[str, dict[str, Any]] = {
    "calculator": {
        "description": "Work out an arithmetic expression, such as '12 * 34'. Call it for any "
        "sum or product instead of working it out yourself.",
        "schema": {
            "type": "object",
            "properties": {"expression": {"type": "string"}},
            "required": ["expression"],
        },
    },
    "clock": {
        "description": "Get the current time in UTC. Call it when the user asks for the time.",
        "schema": {"type": "object", "properties": {}},
    },
    "roll_dice": {
        "description": "Roll dice and get each value. Call it when the user asks for a roll.",
        "schema": {
            "type": "object",
            "properties": {
                "count": {"type": "integer", "minimum": 1, "maximum": 6},
                "sides": {"type": "integer", "minimum": 2, "maximum": 100},
            },
            "required": ["count"],
        },
    },
    "search_docs": {
        "description": "Search a short text about Temporal Streams. Call it for any question "
        "about Streams, Records, offsets, Retention or withdrawal.",
        "schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
    "research": {
        "description": "Start a sub-agent that researches a topic about Temporal Streams in the "
        "docs, and get its summary. Call it when the user asks for research or a sub-agent.",
        "schema": {
            "type": "object",
            "properties": {"topic": {"type": "string"}},
            "required": ["topic"],
        },
    },
}

# How long each tool works, so that tools that run at the same time interleave their Turns.
WORK_SECONDS = {"calculator": 0.6, "clock": 0.3, "roll_dice": 0.4, "search_docs": 0.8}

_OPERATORS: dict[type, Any] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
}


def _evaluate(node: ast.AST) -> float:
    # Only numbers and arithmetic, since the expression comes from a model.
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.UnaryOp) and type(node.op) in _OPERATORS:
        return _OPERATORS[type(node.op)](_evaluate(node.operand))
    if isinstance(node, ast.BinOp) and type(node.op) in _OPERATORS:
        left, right = _evaluate(node.left), _evaluate(node.right)
        if isinstance(node.op, ast.Pow) and abs(right) > 10:
            raise ValueError("the exponent is too large")
        return _OPERATORS[type(node.op)](left, right)
    raise ValueError("only numbers and + - * / // % ** are allowed")


def calculator(args: dict[str, Any]) -> str:
    expression = args.get("expression")
    if not isinstance(expression, str):
        raise ValueError("expression must be a string")
    value = _evaluate(ast.parse(expression, mode="eval").body)
    return f"{expression} = {value:g}"


def clock(_args: dict[str, Any]) -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def roll_dice(args: dict[str, Any]) -> str:
    count, sides = args.get("count", 1), args.get("sides", 6)
    if not isinstance(count, int) or not 1 <= count <= 6:
        raise ValueError("count must be an integer from 1 to 6")
    if not isinstance(sides, int) or not 2 <= sides <= 100:
        raise ValueError("sides must be an integer from 2 to 100")
    # Seeded by the call, so a retry of the attempt rolls the same values.
    info = activity.info()
    rolls = random.Random(f"{info.workflow_id}:{info.activity_id}").choices(
        range(1, sides + 1), k=count
    )
    return "rolled " + ", ".join(str(r) for r in rolls)


def search_docs(args: dict[str, Any]) -> str:
    query = args.get("query")
    if not isinstance(query, str):
        raise ValueError("query must be a string")
    found = docs.search(query)
    if not found:
        return "The docs have nothing on that."
    return "\n".join(f"{title}. {text}" for title, text in found)


TOOLS = {
    "calculator": calculator,
    "clock": clock,
    "roll_dice": roll_dice,
    "search_docs": search_docs,
}


def describe(name: str, args: dict[str, Any]) -> str:
    """A short form of a call, such as ``calculator(12 * 34)``."""
    return f"{name}({', '.join(str(v) for v in args.values())})"


class ToolActivities:
    def __init__(self, work_scale: float = 1.0) -> None:
        self._work_scale = work_scale

    @activity.defn
    async def run_tool(self, input: ToolInput) -> ToolResult:
        call = input.call
        # A Stream from activity.client() writes as this attempt, so each tool is a Producer.
        turns = activity.client().get_stream(input.turns)

        def turn(role: Any, text: str) -> Turn:
            return Turn(
                role, text, input.number, input.round, tool=call.name, call_id=call.id
            )

        await turns.append(turn("tool_progress", f"{call.name} started"))
        await asyncio.sleep(WORK_SECONDS.get(call.name, 0) * self._work_scale)
        tool = TOOLS.get(call.name)
        try:
            if tool is None:
                raise ValueError(f"there is no tool {call.name}")
            # A streamed tool input can be cut off or malformed, so each tool checks its input.
            result = ToolResult(call.id, call.name, tool(call.args))
        except (ValueError, SyntaxError, ZeroDivisionError) as err:
            result = ToolResult(call.id, call.name, f"error: {err}", error=True)
        await turns.append(turn("tool_result", result.text))
        return result
