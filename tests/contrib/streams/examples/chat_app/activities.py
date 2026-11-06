"""The model Activity, which streams one round's token deltas to a tokens Stream."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from temporalio import activity
from temporalio.client import BufferedPublisher
from temporalio.exceptions import ApplicationError

from .models import Model, RawContent
from .shared import ModelInput, ModelReply, Token, ToolCall

# How many tokens the first attempt streams before the demo toggle fails it.
FAIL_AFTER = 8


@contextlib.asynccontextmanager
async def heartbeats(every: float = 2) -> AsyncIterator[None]:
    # A model can think for a while before its first delta, so the beat doesn't wait for them.
    async def beat() -> None:
        while True:
            activity.heartbeat()
            await asyncio.sleep(every)

    task = asyncio.create_task(beat())
    try:
        yield
    finally:
        task.cancel()


class ModelActivities:
    def __init__(self, model: Model) -> None:
        self._model = model

    @activity.defn
    async def call_model(self, input: ModelInput) -> ModelReply:
        # A Stream from activity.client() writes as this attempt, so a retry withdraws its Records.
        tokens = activity.client().to_stream(input.tokens)
        fail = input.fail_first_attempt and activity.info().attempt == 1
        reply = ModelReply("")
        text: list[str] = []
        async with heartbeats(), BufferedPublisher(tokens) as publisher:
            streamed = self._model.stream(input.messages, input.tools)
            async with contextlib.aclosing(streamed) as events:
                async for event in events:
                    if isinstance(event, ToolCall):
                        reply.tool_calls.append(event)
                    elif isinstance(event, RawContent):
                        reply.raw = event.blocks
                    else:
                        publisher.publish(Token(input.number, event, input.round))
                        text.append(event)
                        if fail and len(text) == FAIL_AFTER:
                            break
            if fail:
                # The block end would write the tokens too. The flush shows the call, and that
                # the attempt fails only after readers have its tokens.
                await publisher.flush()
                raise ApplicationError("The fail toggle failed this attempt")
        reply.text = "".join(text)
        return reply
