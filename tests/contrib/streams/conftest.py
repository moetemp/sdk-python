import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest

from temporalio.client import Client
from temporalio.common.streams import StreamPlugin
from temporalio.common.streams.redis import RedisStreamPlugin
from temporalio.common.streams.testing import MemoryStreamPlugin

# Ports that other local services own.
_RESERVED = {7333, 7861, 7931, 6431, 7101, 7102, 7103}


def _free_port() -> int:
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in _RESERVED:
            return port


# One scratch server serves every test module, and each test takes its own key prefix. A run
# that must use a known port gives its own server in TEMPORAL_STREAMS_REDIS_URL.
@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    given = os.environ.get("TEMPORAL_STREAMS_REDIS_URL")
    if given:
        yield given
        return
    server = shutil.which("redis-server")
    if server is None:
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
    # Every live test runs on the memory Store and on Redis, since both must keep one set of
    # rules.
    plugin: StreamPlugin
    if request.param == "memory":
        plugin = MemoryStreamPlugin()
    else:
        redis_url = request.getfixturevalue("redis_url")
        plugin = RedisStreamPlugin(redis_url, key_prefix=f"t-{uuid.uuid4()}")
    config = client.config()
    config["plugins"] = [plugin]
    yield Client(**config)
