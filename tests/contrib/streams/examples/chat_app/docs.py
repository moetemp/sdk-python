"""A small text about Streams, which the ``search_docs`` tool searches."""

DOCS = [
    (
        "Streams",
        "A Stream is an ordered log of Records. Workflows, Activities and clients write to it, "
        "and clients and Activities read it. Each Stream has an id that its creator picks.",
    ),
    (
        "Records and offsets",
        "Each Record has an offset. Offsets are ordered and unique, with gaps. A reader saves "
        "the offset after the last Record that it showed, and a later read resumes from there.",
    ),
    (
        "Producers",
        "Each Record names its Producer, the writer that wrote it. A Workflow writes through "
        "Local Activities, an Activity writes as its attempt, and a client writes as itself. The "
        "Records of one Producer keep the order of its writes. Records of two Producers "
        "interleave in the order that the Store gets them, and every reader sees that order.",
    ),
    (
        "Superseded and withdrawal",
        "When an Activity attempt fails and its retry writes, a Superseded item withdraws the "
        "Records of the failed attempt. A live reader crosses them out with withdrawn_offsets. "
        "A reader that kept Records from an earlier read checks them with withdraws().",
    ),
    (
        "Retention",
        "An open Stream keeps every Record until its close, or until one last-write TTL after "
        "its last write when the backend sets one. A closed Stream stays readable for one "
        "Retention after its close. Token deltas matter only to "
        "live readers, so a backend that stores a chat's tokens can keep a short Retention.",
    ),
    (
        "Close",
        "A run's end doesn't close a Stream. Code closes it with close() or at the end of an "
        "async with block. Readers get the Records before the close, then their read ends.",
    ),
    (
        "Workflow Streams",
        "A Workflow creates standalone Streams under ids of its choice. Each run of a chain "
        "sends the same create, so a retry run gets the Stream that an earlier run made. A "
        "Workflow can create a Stream at run time, such as a sub-agent that names its Streams "
        "after its own Workflow ID.",
    ),
    (
        "Continue-as-new",
        "A run that continues as new forwards the refs of its Streams to the next run. The next "
        "run gets the Streams with get_stream and creates nothing, so readers see one Stream.",
    ),
    (
        "Reading",
        "A read starts after an offset, or at END for new Records only. A read of a Stream that "
        "nobody created fails with NOT_FOUND, so the code that hands out a Stream's id creates "
        "it first.",
    ),
]


def search(query: str, count: int = 2) -> list[tuple[str, str]]:
    """The sections that share the most words with the query."""
    words = {w.strip(".,?!").lower() for w in query.split() if len(w) > 3}

    def score(section: tuple[str, str]) -> int:
        text = f"{section[0]} {section[1]}".lower()
        return sum(1 for w in words if w in text)

    ranked = sorted(DOCS, key=score, reverse=True)
    return [s for s in ranked[:count] if score(s) > 0]
