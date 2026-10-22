# Examples for the Brook streams API proposal

Each file is a complete program for one section of the proposal. `test_examples.py` runs each one end to end on the memory Store and on Redis.

| Proposal section | File |
|---|---|
| A UI backend reads a Workflow's Stream | [`ui_backend_reads.py`](ui_backend_reads.py) |
| A client resumes a scoped Stream from an offset | [`resume_from_offset.py`](resume_from_offset.py) |
| A client creates a Stream and passes it to a Workflow | [`client_creates_stream.py`](client_creates_stream.py) |
| An agent Workflow publishes its turns | [`agent_publishes_turns.py`](agent_publishes_turns.py) |
| An Activity streams tokens to the Workflow's Stream | [`activity_streams_tokens.py`](activity_streams_tokens.py) |
| A Workflow scopes a Stream to one turn and closes it | [`turn_scoped_stream.py`](turn_scoped_stream.py) |
| Worker setup | [`worker_setup.py`](worker_setup.py) |

The proposal's Nexus examples are a later stage, so they have no file here.
