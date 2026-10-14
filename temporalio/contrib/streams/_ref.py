"""A reference to one stream that crosses a process boundary as data.

A handle is bound to a client and its store, so it cannot be a Workflow
argument or an Activity result. A :class:`StreamRef` can: it names the owner
and the topic, nothing more, and whoever receives it opens the stream on the
store its own client carries. It carries no cursor, because a position
belongs to a reader, and no store name.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any, Literal

from temporalio.contrib.streams._topic import (
    DEFAULT_TOPIC,
    StreamTopic,
    resolve_topic,
)

__all__ = ["StreamOwnerKind", "StreamRef"]

StreamOwnerKind = Literal["workflow"]
"""The owner kinds this release opens. Only a Workflow owns a stream.

:attr:`StreamRef.kind` is a plain string, so a ref that names an owner kind
a later release adds still reaches the ref's own check when it is decoded,
instead of failing the converter's type check.
"""


@dataclass(frozen=True)
class StreamRef:
    """One stream, named by its owner and its topic.

    ``kind`` says what owns the stream; only ``"workflow"`` exists in this
    release. A Workflow ref carries ``workflow_id``. The stream belongs to
    the Workflow's run chain, and a read on it follows the chain across
    Continue-as-New and ends only when the chain ends. ``topic`` is the topic
    a handle opened from the ref addresses when a call names none.

    ``chain_run_id`` picks which chain: the one that holds that run. A
    Workflow id can start a new chain once the old one closed, and without
    ``chain_run_id`` a ref names the newest chain. It never stops a read at
    that run's end.

    The default data converter carries it as JSON, so it can be a Workflow
    argument or an Activity result.
    """

    kind: str
    workflow_id: str
    topic: str = DEFAULT_TOPIC
    chain_run_id: str | None = None

    def __post_init__(self) -> None:
        """Refuse a ref this release cannot open.

        Raises:
            ValueError: ``kind`` is not ``"workflow"``, or ``workflow_id`` or
                ``topic`` is empty.
        """
        if self.kind != "workflow":
            raise ValueError(f"unknown StreamRef kind {self.kind!r}")
        if not self.workflow_id:
            raise ValueError("a Workflow StreamRef needs a workflow_id")
        if not self.topic:
            raise ValueError("a StreamRef needs a topic name")

    @classmethod
    def for_workflow(
        cls,
        workflow_id: str,
        *,
        topic: str | StreamTopic[Any] | None = None,
        chain_run_id: str | None = None,
    ) -> StreamRef:
        """A ref to ``topic`` of ``workflow_id``'s stream.

        Without ``topic`` it names
        :data:`temporalio.contrib.streams.DEFAULT_TOPIC`. Without
        ``chain_run_id`` it names the Workflow id's newest chain, and with
        one the chain that holds that run.
        """
        name, _ = resolve_topic(topic)
        return cls("workflow", workflow_id, topic=name, chain_run_id=chain_run_id)

    def with_topic(self, topic: str | StreamTopic[Any] | None) -> StreamRef:
        """The same owner, naming ``topic`` instead.

        ``None`` names :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        """
        name, _ = resolve_topic(topic)
        return dataclasses.replace(self, topic=name)
