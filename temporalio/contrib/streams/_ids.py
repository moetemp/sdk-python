"""Stream and Producer ids. They must match the temporalio-streams crate byte for byte."""

from __future__ import annotations

import urllib.parse


def encode_part(text: str) -> str:
    """Percent-encode every byte but the unreserved ones."""
    return urllib.parse.quote(text, safe="")


def _join(*parts: str) -> str:
    return ":".join(encode_part(part) for part in parts)


def default_stream_id(workflow_id: str, first_run_id: str) -> str:
    """Id of a Workflow's default Stream."""
    return _join(workflow_id, first_run_id)


def scoped_stream_id(workflow_id: str, first_run_id: str, name: str) -> str:
    """Id of a Stream that a Workflow creates with a name."""
    return _join(workflow_id, first_run_id, name)


def workflow_producer_id(run_id: str) -> str:
    """Producer id of a Workflow run."""
    return f"wf:{_join(run_id)}"


def local_activity_producer_id(run_id: str, call_key: str) -> str:
    """Producer id of a Local Activity call."""
    return f"la:{_join(run_id, call_key)}"


def activity_producer_id(run_id: str, activity_id: str, schedule_key: str) -> str:
    """Producer id of an Activity that a Workflow scheduled."""
    return f"act:{_join(run_id, activity_id, schedule_key)}"


def standalone_activity_producer_id(activity_id: str, activity_run_id: str) -> str:
    """Producer id of a Standalone Activity."""
    return f"sa:{_join(activity_id, activity_run_id)}"


def client_producer_id(identity: str, process_key: str) -> str:
    """Producer id of a client process."""
    return f"client:{_join(identity, process_key)}"
