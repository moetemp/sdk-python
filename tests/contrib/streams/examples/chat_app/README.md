# Streams chat app

A small ChatGPT-like app that uses most parts of the Streams API. Each chat is a Workflow with an
agent that answers in rounds. A round is a model call, which can call tools, and the tools' results
go to the next round. Each round streams its token deltas to a tokens Stream, and the turns Stream
gets a Turn for each round, each tool call and each tool result. A FastAPI server reads the Streams
and sends them to the browser as Server-Sent Events. The page shows the chat, and a log of each
Stream item with its offset, its Producer and its kind.

## Run it

From the repo root, with the SDK's dev environment.

```sh
uv sync --all-extras
poe build-develop

# In two other shells. Redis is optional, see below.
temporal server start-dev --port 7245 --ui-port 8245
redis-server --port 6681 --save "" --appendonly no

export TEMPORAL_ADDRESS=localhost:7245
export CHAT_APP_REDIS_URL=redis://127.0.0.1:6681
uv run uvicorn tests.contrib.streams.examples.chat_app.server:app --port 8010 \
  --timeout-graceful-shutdown 1
```

Then open <http://localhost:8010>. Each open tab holds a connection, so without
`--timeout-graceful-shutdown` a stop of the server waits for the tabs to close.

The server runs the Worker in its own process. Without `CHAT_APP_REDIS_URL`, it uses the
in-memory backend, and the Streams go away when the server stops.

### Models

The page header shows the provider and the model. `CHAT_MODEL_PROVIDER` picks the provider. It
takes `claude`, `openai` or `fake`. Without it, the server takes Claude if `ANTHROPIC_API_KEY` is
set, else OpenAI if `OPENAI_API_KEY` is set, else the scripted model.

- **Claude.** Export `ANTHROPIC_API_KEY`. The model is `claude-opus-5-5`, and `ANTHROPIC_MODEL`
  picks another. Each round is one streamed request with tool use and low effort. The next
  round sends back the round's blocks unchanged, so Claude keeps its thinking. A round that is
  refused, or cut off in a tool call, runs no tool. The call opts in to the server-side refusal
  fallback (`fallbacks="default"`), so a declined request runs again on the model that Anthropic
  picks for it. If the whole chain declines, the page shows a short notice instead of the reply.
  With `CHAT_MODEL_PROVIDER=claude`, the `anthropic` SDK can also use other credentials, such as
  a profile from `ant auth login`.
- **OpenAI.** Export `OPENAI_API_KEY`. The model is `gpt-4o-mini`, and `OPENAI_MODEL` picks
  another. Each round is one streamed request with tool calls.
- **Scripted.** It plans each round from the prompt and the tool results so far, so the
  scenarios take the same rounds with no key. The tests use it.

The tools are local and make no network call. They are `calculator`, `clock`, `roll_dice`,
`search_docs`, which searches a short text about Streams in [`docs.py`](docs.py), and `research`,
which starts a sub-agent. Each tool works for a moment, so tools that run at the same time
interleave their Turns.

Each provider gets only the system prompt and the chat's messages. The app reads no key itself,
and a failed call keeps only the error's class, since an API error's text can quote the request.

To run the app in its own environment instead, build it from [`pyproject.toml`](pyproject.toml).
It takes the SDK from this repo, so the build needs the Stream runtime.

```sh
MATURIN_PEP517_ARGS="--features streams" uv sync --project tests/contrib/streams/examples/chat_app
uv run --project tests/contrib/streams/examples/chat_app \
  uvicorn tests.contrib.streams.examples.chat_app.server:app --port 8010 \
  --timeout-graceful-shutdown 1
```

## Scenarios

The bar above the composer has a preset prompt for each case. "Run all" plays them one after
another in one chat, so a new viewer sees each case.

- **Tool rounds.** Three model rounds, with `roll_dice` and then `calculator` between them. Each
  round streams its own tokens, and each tool gets a card with its Turns.
- **Parallel tools.** One round calls `clock` and `calculator`, and the two tool Activities write
  to the turns Stream at the same time. The log shows two Activity Producers. Each Producer's
  Turns keep their order, and the two interleave in the order that the Store got them, which
  every reader sees.
- **Failed attempt.** The first attempt of the round fails after a few tokens. One second later,
  the retry's `Superseded` item crosses them out, and the round streams again.
- **Message during a run.** While `search_docs` runs, the page sends a second message. The
  `add_message` Signal appends its Turn at once with `append_nowait`, and the message joins
  the next round of the same turn. While a turn runs, the composer sends this way too.
- **Sub-agent.** `research` starts a child Workflow. It creates its own Streams, under ids with
  its run id that the parent can't know, and writes a Turn with their refs to the parent's turns
  Stream. The page opens a panel that reads the child's Streams by those ids.
- **Continue-as-new.** Six rolls in six rounds, then the answer. After six rounds, the Workflow
  continues as new between turns, and the log shows the next run on the same Streams.
- **Share.** A snapshot of the chat so far.

## What to try

- Send a message, and watch the rounds stream token by token. Each token is a row in the log.
- Turn on "Fail the first attempt of the next reply", and send a message. The model Activity fails
  after a few tokens. One second later, the retry's `Superseded` item crosses out the failed
  attempt's tokens, and the round streams again.
- Stop the server in the middle of a reply, and start it again with Redis. The attempt misses its
  heartbeats, and its retry withdraws its tokens. The tab reconnects, and the server checks the
  tokens that the tab kept with `withdraws()`, so the page crosses them out.
- Reload the page, also in the middle of a reply. The tab keeps its place in each Stream and
  resumes from it.
- Click "Open a second reader". The new tab reads the turns from the start and the tokens from
  the end.
- Click "Share a snapshot". The server copies the chat into a new Stream and closes it. The link
  reads that Stream to its end. The snapshot's offsets and Producer are its own, so each Record
  is a `SharedTurn`, which keeps the Turn's offset and Producer in the chat. The log shows both.
  A link from an older version of the app, whose id has `-share-` and not `-snapshot-`, asks you
  to share the chat again.
- Click "End chat". The Workflow closes its Streams, and each read ends.

## Where each feature is

- **`create_stream` with `USE_EXISTING` in a Workflow.** [`ChatSession.__init__`](workflow.py)
  gets the turns and the tokens Stream with `workflow.create_stream_nowait(...,
  id_conflict_policy=StreamIDConflictPolicy.USE_EXISTING)`. A chat id can come back, such as in a
  restarted session, and the run then gets the chat's Streams.
- **`create_stream` with `USE_EXISTING` in a client.** The server's [`lifespan`](server.py) gets
  the lobby Stream, which lists the chats. Each server start runs it.
- **Strict create.** [`share`](server.py) creates each snapshot with
  `client.create_stream(payload_type=SharedTurn)`, since a share id names one snapshot. A second
  share of the same point raises a `StreamError` with reason `STREAM_ERROR_REASON_CLOSED`, since
  that snapshot is closed. A share that runs at the same time raises a `StreamError` with reason
  `STREAM_ERROR_REASON_ALREADY_EXISTS`. Both give the same link.
- **Standalone Streams that a Workflow creates.** `<chat>-turns` and `<chat>-tokens`, with ids
  that the Workflow picks.
- **Refs as data.** The `streams` query gives `SessionStreams`. `ModelInput.tokens` takes a ref to
  the Activity, and `SessionInput.streams` takes both refs through continue-as-new.
- **`get_stream`.** `workflow.get_stream(ref)` in the next run,
  `activity.client().get_stream(ref)` in the Activity, and `client.get_stream(id,
  payload_type=Turn)` in the server.
- **`client.get_stream` by id.** The share links, which name a snapshot's Stream id.
- **Awaited `append`.** The `send` Update returns the user Turn's offset, which the page shows
  under the composer. The server's lobby append is awaited too.
- **`append_nowait`.** `@workflow.init` appends "Run N of the session started", and the
  `add_message` Signal appends the user's Turn, since neither can await.
- **An agent loop.** [`Agent.answer`](workflow.py) runs the rounds of an answer, for a chat and
  for a sub-agent. Each round is the model Activity, and the round's tools run at the same time.
- **Activities that write Turns.** [`run_tool`](tools.py) writes its progress and its result to
  the turns Stream, from a ref in its input. Each attempt of each tool is its own Producer.
- **A child Workflow's own Streams.** [`Researcher`](workflow.py) creates its Streams at run time,
  writes to its parent's turns Stream through `workflow.get_stream`, and puts its own refs in a
  Turn. A ref is data, also inside a Record.
- **`BufferedAppender` and `flush`.** [`call_model`](activities.py) batches the deltas, and
  flushes before the toggle fails the attempt.
- **`read(offset=...)`.** [`session_events`](server.py) resumes each read at the offset that
  the tab saved.
- **`streams.END`.** A tab that joins reads only new tokens, since the turns Stream has the
  earlier replies.
- **`read(last=...)`.** The lobby sidebar reads the newest 20 chats, then each new one.
- **`create_wait`.** A new chat's tab can start to read before the init's creates.
- **Resume from `next_offset`.** Each item that the server sends carries its read's
  `next_offset`. The tab saves it, and a reload or a reconnect sends it back.
- **Opaque offsets.** A `StreamOffset` is an opaque position. The server passes each one to the
  browser as its string form, and parses a resume offset back with `StreamOffset.parse`.
- **`Superseded` withdrawal.** The page uses `withdrawn_offsets` for tokens that the read gave,
  and the server uses `withdraws()` for tokens that the tab kept from an earlier read.
- **Close with `async with`.** The share snapshot's block in the server, and the sub-agent's
  Streams in `Researcher`.
- **`close()`.** `ChatSession.run` closes both Streams when the chat ends, since a run's end
  doesn't close a Stream.
- **`get_offset`.** The snapshot id names the turns Stream's offset. The share page gets the
  snapshot's offset, and an unknown snapshot gives a `StreamError` with reason
  `STREAM_ERROR_REASON_NOT_FOUND`.
- **Error reasons.** `STREAM_ERROR_REASON_CLOSED` and `STREAM_ERROR_REASON_ALREADY_EXISTS` in
  `share`, and `STREAM_ERROR_REASON_NOT_FOUND` in `share_info`. Both catch `StreamError` and check
  `reason`.
- **Payload type.** `payload_type=Turn`, `payload_type=Token`, `payload_type=SharedTurn` and
  `payload_type=SessionInfo` on the creates and the id-form gets.
- **`client.get_stream` by id for a sub-agent.** The server reads a sub-agent's Streams by the
  ids in its Turn, for the page's panel.
- **In-memory backend.** The tests run on it and on Redis. The server uses it when
  `CHAT_APP_REDIS_URL` isn't set.
- **Producers.** The log shows each item's Producer. The Workflow writes through Local
  Activities, each model or tool attempt is an Activity Producer, a sub-agent writes through its
  own Local Activities, and the server writes as a client.

The app doesn't use `streams.producer()`, `SyncStream`, `close_nowait` or the `FENCED` reason.

## Coming

These are approved but not built yet, so the app doesn't use them.

- A `closed` item with a close status. The log would show why a Stream closed.
- `max_record_age` on a read.

The Python facade has no public `describe` yet, so the app uses only `get_offset`.

## Files

- [`shared.py`](shared.py) has the Records and the inputs that the Workflow, the Activity and the
  server share.
- [`workflow.py`](workflow.py) has the agent loop, the `ChatSession` Workflow and the
  `Researcher` sub-agent.
- [`activities.py`](activities.py) has the model Activity.
- [`tools.py`](tools.py) has the tools and their Activity, and [`docs.py`](docs.py) the text
  that `search_docs` searches.
- [`models/`](models/__init__.py) has one module for each provider, behind one interface.
- [`server.py`](server.py) has the FastAPI server and its Server-Sent Events.
- [`static/index.html`](static/index.html) is the page, with plain JavaScript and no build step.
- [`test_chat_app.py`](test_chat_app.py) tests the agent on the scripted model and the server's
  Stream code, on the memory Store and on Redis.

Run the tests from the repo root.

```sh
uv run pytest tests/contrib/streams/examples/chat_app
```
