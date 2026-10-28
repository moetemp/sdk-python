import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest

from temporalio.client import Client
from temporalio.common import StreamBackend
from temporalio.contrib.stream import RedisStreamBackend
from temporalio.contrib.stream.testing import MemoryStreamBackend

# Ports that other local services own.
_RESERVED = {7333, 7861, 7931, 6431, 7101, 7102, 7103}


def _free_port() -> int:
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in _RESERVED:
            return port


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
    # rules. The backend is the default, so a create with no backend name uses it.
    backend: StreamBackend
    if request.param == "memory":
        backend = MemoryStreamBackend(default=True)
    else:
        redis_url = request.getfixturevalue("redis_url")
        backend = RedisStreamBackend(
            redis_url, key_prefix=f"t-{uuid.uuid4()}", default=True
        )
    config = client.config()
    config["stream_backends"] = [backend]
    yield Client(**config)
