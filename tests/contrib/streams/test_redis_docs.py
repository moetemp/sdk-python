"""The Redis docs this package ships stay in step with the crate's store."""

from __future__ import annotations

import re
from pathlib import Path

from tests.contrib.streams._support import streams_crate_dir

_ROOT = Path(__file__).resolve().parents[3]
_PYTHON_DOC = _ROOT / "temporalio/contrib/streams/docs/redis-guarantees.md"
# The line the crate's ACL test lane builds its Redis user from.
_CORE_DOC = streams_crate_dir() / "docs/redis.md"


def acl_line(doc: Path) -> str:
    (line,) = re.findall(r"^ACL SETUSER .*$", doc.read_text(), re.MULTILINE)
    return line.strip()


def test_the_documented_acl_line_is_cores():
    # A user built from an older copy of the line fails appends with NOPERM
    # once the crate's store needs another command.
    assert acl_line(_PYTHON_DOC) == acl_line(_CORE_DOC)
