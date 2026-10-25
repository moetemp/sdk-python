# Examples for the Brook streams API proposal

Each file is a complete program for one section of the proposal. `test_examples.py` runs each one
end to end on the memory Store and on Redis.

| Proposal section | File |
|---|---|
| A UI backend reads a Workflow's Stream | [`ui_backend_reads.py`](ui_backend_reads.py) |
| A client resumes a scoped Stream from an offset | [`resume_from_offset.py`](resume_from_offset.py) |
| A client creates a Stream and passes it to a Workflow | [`client_creates_stream.py`](client_creates_stream.py) |
| An agent Workflow publishes its turns | [`agent_publishes_turns.py`](agent_publishes_turns.py) |
| An Activity streams tokens to the Workflow's Stream | [`activity_streams_tokens.py`](activity_streams_tokens.py) |
| A Workflow scopes a Stream to one turn and closes it | [`turn_scoped_stream.py`](turn_scoped_stream.py) |
| Worker setup | [`worker_setup.py`](worker_setup.py) |

The proposal's Nexus examples aren't in this directory.

## Jetty v2

These add Workflow reads, the native backend, two backends in one Worker, and Standalone
Activity Streams to the same API. Every client has the native backend with no registration, and
it's the default unless a registered backend has `default=True`. `test_examples.py` runs the
Workflow reader on the memory Store, on Redis and on the native server. The others need the
native server, with Redis or a memory Store named `redis` in place of Redis. The native runs
happen only when `TEMPORAL_STREAMS_NATIVE_TARGET` is set.

| Section | File |
|---|---|
| One Worker with Redis and native, one Stream on each | [`two_backends.py`](two_backends.py) |
| A Workflow reads another Workflow's Stream | [`workflow_reader.py`](workflow_reader.py) |
| Old Streams stay on Redis, and new ones are created on native | [`migrate_redis_to_native.py`](migrate_redis_to_native.py) |
| A Standalone Activity streams its progress | [`standalone_activity_stream.py`](standalone_activity_stream.py) |
