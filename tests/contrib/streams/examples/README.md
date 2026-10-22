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

## Jetty v2

These add Workflow reads, the native backend, and two backends in one Worker to the same API. `test_examples.py` runs each one on three lanes. The memory and Redis lanes use a memory Store with the backend name `native` in place of the server. The native lane uses `NativeStreamPlugin` against a server with the Stream service, and it runs only when `TEMPORAL_STREAMS_NATIVE_TARGET` is set.

| Section | File |
|---|---|
| One Worker with Redis and native, one Stream on each | [`two_backends.py`](two_backends.py) |
| A Workflow reads another Workflow's Stream | [`workflow_reader.py`](workflow_reader.py) |
| Old Streams stay on Redis, new ones go to native | [`migrate_redis_to_native.py`](migrate_redis_to_native.py) |
