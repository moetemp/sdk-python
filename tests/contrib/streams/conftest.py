import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator

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


# A server that serves the Stream service, such as a dev server of moetemp/temporal with the
# native Stream backend. The native lane runs only when it is set.
NATIVE_TARGET = os.environ.get("TEMPORAL_STREAMS_NATIVE_TARGET", "")
NATIVE_NAMESPACE = os.environ.get("TEMPORAL_STREAMS_NATIVE_NAMESPACE", "default")


async def _brook_client(
    client: Client, request: pytest.FixtureRequest, lane: str
) -> Client:
    # The memory Store or Redis, as the default, so a create with no backend name uses it.
    backend: StreamBackend
    if lane == "memory":
        backend = MemoryStreamBackend(default=True)
    else:
        redis_url = request.getfixturevalue("redis_url")
        backend = RedisStreamBackend(
            redis_url, key_prefix=f"t-{uuid.uuid4()}", default=True
        )
    config = client.config()
    config["stream_backends"] = [backend]
    return Client(**config)


@pytest.fixture(params=["memory", "redis", *(["native"] if NATIVE_TARGET else [])])
async def stream_client(client: Client, request: pytest.FixtureRequest) -> Client:
    # Every live test runs on the memory Store and on Redis, since both must keep one set of
    # rules, and on the native server when one is given. The native backend is every
    # client's with no registration, and the default when no backend has default=True.
    if request.param == "native":
        return await Client.connect(NATIVE_TARGET, namespace=NATIVE_NAMESPACE)
    return await _brook_client(client, request, request.param)


@pytest.fixture(params=["memory", "redis"])
async def redis_client(client: Client, request: pytest.FixtureRequest) -> Client:
    # A test of the Redis calls, such as temporalio.contrib.stream.create, which the memory
    # Store keeps the rules of.
    return await _brook_client(client, request, request.param)


@pytest.fixture
async def native_client() -> Client:
    if not NATIVE_TARGET:
        pytest.skip(
            "TEMPORAL_STREAMS_NATIVE_TARGET names no server with the Stream service"
        )
    return await Client.connect(NATIVE_TARGET, namespace=NATIVE_NAMESPACE)
