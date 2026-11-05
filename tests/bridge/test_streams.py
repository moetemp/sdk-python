import pytest

import temporalio.bridge.streams
from temporalio.runtime import Runtime
from temporalio.service import RPCStatusCode


def _stream_runtime(
    memory: bool, config: bytes = b""
) -> temporalio.bridge.streams.StreamRuntime:
    return temporalio.bridge.streams.StreamRuntime(
        Runtime.default()._core_runtime, memory, config
    )


async def test_memory_runtime_takes_calls_by_method_name():
    runtime = _stream_runtime(True)
    # The args are the gRPC code, the message and the encoded details, as in RPCError.
    with pytest.raises(temporalio.bridge.streams.StreamCallError) as err:
        await runtime.call("NoSuchMethod", b"")
    code, _, details = err.value.args
    assert code == RPCStatusCode.UNIMPLEMENTED
    # StreamErrorDetails with the reason STREAM_ERROR_REASON_UNSUPPORTED (14).
    assert details == b"\x08\x0e"


def test_runtime_refuses_a_config_that_does_not_decode():
    with pytest.raises(ValueError, match="config"):
        _stream_runtime(False, b"\xff\xff")
