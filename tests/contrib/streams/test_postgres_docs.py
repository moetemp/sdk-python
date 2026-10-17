"""The PostgreSQL docs this package ships stay in step with the crate's store."""

from __future__ import annotations

import re
from pathlib import Path

from tests.contrib.streams._support import streams_crate_dir

_ROOT = Path(__file__).resolve().parents[3]
_PYTHON_DOC = _ROOT / "temporalio/contrib/streams/docs/postgres-guarantees.md"
# The grants the crate's role test lane runs the store with.
_CORE_DOC = streams_crate_dir() / "docs/postgres.md"


def grant_lines(doc: Path) -> list[str]:
    return re.findall(r"^GRANT .*$", doc.read_text(), re.MULTILINE)


def test_the_documented_grants_are_cores():
    # A role built from an older copy of the grants fails calls once Core's
    # store needs another right.
    assert grant_lines(_PYTHON_DOC)
    assert grant_lines(_PYTHON_DOC) == grant_lines(_CORE_DOC)
