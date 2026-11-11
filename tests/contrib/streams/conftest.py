import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest

from temporalio.client import Client
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend
from temporalio.streams import StreamBackend, StreamConfig


def _free_port() -> int:
    # The OS gives a port that is free now, so the scratch server takes no port of another
    # service.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# One scratch server serves every test module, and each test uses its own key prefix. To use a
# known port, set TEMPORAL_STREAMS_REDIS_URL to your own server.
@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    given = os.environ.get("TEMPORAL_STREAMS_REDIS_URL")
    if given:
        yield given
        return
    server = shutil.which("redis-server")
    if server is None:
        # CI sets this where it installs Redis, so the Redis lane can't skip there unseen.
        if os.environ.get("TEMPORAL_STREAMS_REQUIRE_REDIS"):
            pytest.fail("redis-server is not installed")
        pytest.skip("redis-server is not installed")
    port = _free_port()
    process = subprocess.Popen(
        [server, "--port", str(port), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
    )
    try:
        for _ in range(100):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
                break
            except OSError:
                time.sleep(0.05)
        yield f"redis://127.0.0.1:{port}"
    finally:
        process.terminate()
        process.wait()


@pytest.fixture(params=["memory", "redis"])
async def stream_client(
    client: Client, request: pytest.FixtureRequest
) -> AsyncIterator[Client]:
    # Every live test runs on the memory Store and on Redis, since both must have the same
    # rules. The client has one backend, so every Stream goes to it.
    backend: StreamBackend
    if request.param == "memory":
        backend = MemoryStreamBackend()
    else:
        redis_url = request.getfixturevalue("redis_url")
        backend = RedisStreamBackend(redis_url, key_prefix=f"t-{uuid.uuid4()}")
    config = client.config()
    config["stream_config"] = StreamConfig(backends=[backend])
    yield Client(**config)
