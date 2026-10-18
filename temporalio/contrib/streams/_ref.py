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

from temporalio.api.enums.v1 import StreamOwnerKind as _ApiOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.bridge.proto.streams import StreamAddress
from temporalio.bridge.proto.streams import StreamOwnerKind as _OwnerKind
from temporalio.contrib.streams._errors import StreamUnsupportedError
from temporalio.contrib.streams._topic import (
    DEFAULT_TOPIC,
    StreamTopic,
    resolve_topic,
)
from temporalio.converter import (
    ActivitySerializationContext,
    SerializationContext,
    WorkflowSerializationContext,
)

__all__ = ["StreamOwnerKind", "StreamRef"]

StreamOwnerKind = Literal["workflow", "activity", "workflow_activity"]
"""The owner kinds this release opens.

A Workflow owns a stream through its run chain. A standalone Activity owns a
stream through one run. An Activity that a Workflow scheduled owns a stream
through its scheduling run. :attr:`StreamRef.kind` is a plain string, so a ref
that names an owner kind a later release adds still decodes, and is refused
where a handle opens.
"""

_KINDS = ("workflow", "activity", "workflow_activity")


@dataclass(frozen=True)
class StreamRef:
    """One stream, named by its owner and its topic.

    ``kind`` says what owns the stream. A ``"workflow"`` ref carries
    ``workflow_id``. The stream belongs to the Workflow's run chain, and a
    read on it follows the chain across Continue-as-New and ends only when
    the chain ends. An ``"activity"`` ref carries ``activity_id`` and an
    empty ``workflow_id``. The stream belongs to one run of a standalone
    Activity, and a read on it ends when that run ends. A
    ``"workflow_activity"`` ref carries ``workflow_id``, ``activity_id`` and
    ``chain_run_id``. The stream belongs to the Activity with that id that
    the run ``chain_run_id`` of the Workflow scheduled, and it ends when that
    run sees the Activity end. ``topic`` is the topic a handle opened from
    the ref addresses when a call names none.

    ``chain_run_id`` picks which chain: the one that holds that run. A
    Workflow id can start a new chain once the old one closed, and without
    ``chain_run_id`` a ref names the newest chain. It never stops a read at
    that run's end. For an Activity ref it is the Activity run. Without it
    the ref names the newest run of the Activity id when a read starts. For a
    Workflow Activity ref it is the scheduling run, and it is required.

    The default data converter carries it as JSON, so it can be a Workflow
    argument or an Activity result.
    """

    kind: str
    workflow_id: str
    topic: str = DEFAULT_TOPIC
    chain_run_id: str | None = None
    activity_id: str | None = None

    def __post_init__(self) -> None:
        """Refuse a ref that names no stream.

        Any ``kind`` decodes, since a ref of a later release can arrive as
        Workflow input, where raising would fail the Workflow Task on every
        retry. :meth:`_require_supported` refuses it where a handle opens.

        Raises:
            ValueError: ``kind`` or ``topic`` is empty, a Workflow ref has no
                ``workflow_id`` or has an ``activity_id``, an Activity ref
                has no ``activity_id`` or has a ``workflow_id``, or a
                Workflow Activity ref lacks one of ``workflow_id``,
                ``activity_id`` and ``chain_run_id``.
        """
        if not self.kind:
            raise ValueError("a StreamRef needs a kind")
        if self.kind == "workflow":
            if not self.workflow_id:
                raise ValueError("a Workflow StreamRef needs a workflow_id")
            if self.activity_id:
                raise ValueError("a Workflow StreamRef has no activity_id")
        if self.kind == "activity":
            if not self.activity_id:
                raise ValueError("an Activity StreamRef needs an activity_id")
            if self.workflow_id:
                raise ValueError("an Activity StreamRef has no workflow_id")
        if self.kind == "workflow_activity":
            for name in ("workflow_id", "activity_id", "chain_run_id"):
                if not getattr(self, name):
                    raise ValueError(f"a Workflow Activity StreamRef needs its {name}")
        if not self.topic:
            raise ValueError("a StreamRef needs a topic name")

    def _require_supported(self) -> None:
        """Refuse to open a stream whose owner kind this release lacks.

        Raises:
            StreamUnsupportedError: ``kind`` is not ``"workflow"``,
                ``"activity"`` or ``"workflow_activity"``.
        """
        if self.kind not in _KINDS:
            raise StreamUnsupportedError(
                f"streams owned by kind {self.kind!r} are not supported in this "
                "release; only streams owned by a Workflow or an Activity are"
            )

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

    @classmethod
    def for_activity(
        cls,
        activity_id: str,
        *,
        run_id: str | None = None,
        topic: str | StreamTopic[Any] | None = None,
    ) -> StreamRef:
        """A ref to ``topic`` of the stream of standalone Activity ``activity_id``.

        Without ``topic`` it names
        :data:`temporalio.contrib.streams.DEFAULT_TOPIC`. Without ``run_id``
        it names the newest run of the Activity id when a read starts, and
        the read stays on that run.
        """
        name, _ = resolve_topic(topic)
        return cls(
            "activity", "", topic=name, chain_run_id=run_id, activity_id=activity_id
        )

    @classmethod
    def for_workflow_activity(
        cls,
        workflow_id: str,
        *,
        run_id: str,
        activity_id: str,
        topic: str | StreamTopic[Any] | None = None,
    ) -> StreamRef:
        """A ref to ``topic`` of the stream of Activity ``activity_id`` of a Workflow run.

        ``run_id`` is the run that scheduled the Activity. An Activity id
        names one Activity only within a run, so it is required. Without
        ``topic`` it names :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        """
        name, _ = resolve_topic(topic)
        return cls(
            "workflow_activity",
            workflow_id,
            topic=name,
            chain_run_id=run_id,
            activity_id=activity_id,
        )

    def with_topic(self, topic: str | StreamTopic[Any] | None) -> StreamRef:
        """The same owner, naming ``topic`` instead.

        ``None`` names :data:`temporalio.contrib.streams.DEFAULT_TOPIC`.
        """
        name, _ = resolve_topic(topic)
        return dataclasses.replace(self, topic=name)


def stream_address(
    namespace: str,
    ref: StreamRef,
    *,
    topic: str | None = None,
    run_id: str | None = None,
) -> StreamAddress:
    """The stream runtime's address of ``ref``'s stream, on ``topic`` or the ref's topic.

    ``run_id`` replaces the ref's ``chain_run_id``. An empty one names the
    owner's newest chain or run.
    """
    address = StreamAddress(
        namespace=namespace,
        run_id=(ref.chain_run_id or "") if run_id is None else run_id,
        topic=ref.topic if topic is None else topic,
    )
    if ref.kind == "activity":
        address.owner_kind = _OwnerKind.STREAM_OWNER_KIND_ACTIVITY
        address.activity_id = ref.activity_id or ""
    elif ref.kind == "workflow_activity":
        address.owner_kind = _OwnerKind.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY
        address.workflow_id = ref.workflow_id
        address.activity_id = ref.activity_id or ""
    else:
        address.owner_kind = _OwnerKind.STREAM_OWNER_KIND_WORKFLOW
        address.workflow_id = ref.workflow_id
    return address


def from_address(address: StreamAddress) -> StreamRef:
    """The ref of the stream the stream runtime's ``address`` names."""
    if address.owner_kind == _OwnerKind.STREAM_OWNER_KIND_ACTIVITY:
        return StreamRef.for_activity(
            address.activity_id, run_id=address.run_id or None, topic=address.topic
        )
    if address.owner_kind == _OwnerKind.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY:
        return StreamRef.for_workflow_activity(
            address.workflow_id,
            run_id=address.run_id,
            activity_id=address.activity_id,
            topic=address.topic,
        )
    return StreamRef.for_workflow(
        address.workflow_id, topic=address.topic, chain_run_id=address.run_id or None
    )


def owner_context(
    namespace: str, workflow_id: str, activity_id: str | None
) -> SerializationContext:
    """The codec context of every record and close of one owner's streams.

    An Activity owner gets only its namespace, its id and the id of the
    Workflow that scheduled it, if any, since a reader can build no more from
    the ref.
    """
    if activity_id:
        return ActivitySerializationContext(
            namespace=namespace,
            activity_id=activity_id,
            activity_type=None,
            activity_task_queue=None,
            workflow_id=workflow_id or None,
            workflow_type=None,
            is_local=False,
        )
    return WorkflowSerializationContext(namespace=namespace, workflow_id=workflow_id)


_API_KINDS = {
    "workflow": _ApiOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
    "activity": _ApiOwnerKind.STREAM_OWNER_KIND_ACTIVITY,
    "workflow_activity": _ApiOwnerKind.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY,
}


def to_reference(ref: StreamRef) -> StreamReference:
    """``ref`` as the API's stream reference, which a link record stores."""
    if ref.kind not in _API_KINDS:
        ref._require_supported()
    return StreamReference(
        owner_kind=_API_KINDS[ref.kind],
        workflow_id=ref.workflow_id,
        run_id=ref.chain_run_id or "",
        topic=ref.topic,
        activity_id=ref.activity_id or "",
    )


def from_reference(reference: StreamReference) -> StreamRef:
    """The ref an API stream reference names.

    An owner kind this release lacks decodes as ``"kind_<number>"``, which a
    handle refuses where it opens.

    Raises:
        ValueError: The reference names no stream.
    """
    kind = next(
        (k for k, v in _API_KINDS.items() if v == reference.owner_kind),
        f"kind_{int(reference.owner_kind)}",
    )
    return StreamRef(
        kind,
        reference.workflow_id,
        topic=reference.topic,
        chain_run_id=reference.run_id or None,
        activity_id=reference.activity_id or None,
    )
