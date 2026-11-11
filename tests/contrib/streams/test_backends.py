from typing import Any, cast

import pytest

from temporalio import streams, workflow
from temporalio.client import Client
from temporalio.plugin import SimplePlugin
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, WorkerConfig
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
from tests.helpers import new_worker


class _Backend(streams.StreamBackend):
    # Registration makes no Stream call, so this backend needs no Store.
    def __init__(self, name: str = "test") -> None:
        super().__init__(f"test-streams-{name}", backend_name=name)


class _KeepsTheRunner(_Backend):
    runner: object = None

    def _configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        self.runner = config.get("workflow_runner")
        return super()._configure_worker(config)


@workflow.defn
class Idle:
    @workflow.run
    async def run(self) -> None:
        pass


def _client(client: Client, stream_config: streams.StreamConfig) -> Client:
    config = client.config()
    config["stream_config"] = stream_config
    return Client(**config)


def test_config_needs_a_backend():
    with pytest.raises(streams.StreamConfigError, match="no backend"):
        streams.StreamConfig(backends=[])


def test_config_refuses_two_backends_with_one_name():
    with pytest.raises(streams.StreamConfigError, match="two backends with the name a"):
        streams.StreamConfig(
            backends=[_Backend("a"), _Backend("a")],
            backend_selector=lambda ctx: _Backend("a"),
        )


def test_config_with_two_backends_needs_a_selector():
    with pytest.raises(streams.StreamConfigError, match="no backend_selector"):
        streams.StreamConfig(backends=[_Backend("a"), _Backend("b")])


def test_config_refuses_an_entry_that_is_not_a_backend():
    with pytest.raises(TypeError, match="not a StreamBackend"):
        streams.StreamConfig(backends=cast(Any, [object()]))


async def test_selector_that_returns_an_unregistered_backend_raises(client: Client):
    # A backend with the same name is still not one of the registered instances.
    two = _client(
        client,
        streams.StreamConfig(
            backends=[_Backend("a"), _Backend("b")],
            backend_selector=lambda ctx: _Backend("b"),
        ),
    )
    with pytest.raises(streams.StreamConfigError, match="not one of the backends"):
        await two.create_stream("chat-1", payload_type=str)
    with pytest.raises(streams.StreamConfigError, match="not one of the backends"):
        two.get_stream("chat-1", payload_type=str)


async def test_worker_on_a_client_with_two_backends_raises(client: Client):
    a, b = _Backend("a"), _Backend("b")
    two = _client(
        client, streams.StreamConfig(backends=[a, b], backend_selector=lambda ctx: a)
    )
    with pytest.raises(streams.StreamConfigError, match="client with one backend"):
        new_worker(two, Idle)


async def test_backend_in_plugins_raises(client: Client):
    # The connect refuses it before it connects, so the unreachable target doesn't matter.
    with pytest.raises(streams.StreamConfigError, match="not a plugin"):
        await Client.connect("localhost:1", plugins=[_Backend()])
    config = client.config()
    config["plugins"] = [_Backend()]
    with pytest.raises(streams.StreamConfigError, match="not a plugin"):
        Client(**config)
    with pytest.raises(streams.StreamConfigError, match="not a plugin"):
        new_worker(client, Idle, plugins=[_Backend()])
    with pytest.raises(streams.StreamConfigError, match="not a plugin"):
        Replayer(workflows=[Idle], plugins=[_Backend()])


def test_replayer_serves_one_backend():
    a, b = _Backend("a"), _Backend("b")
    with pytest.raises(streams.StreamConfigError, match="one backend"):
        Replayer(
            workflows=[Idle],
            stream_config=streams.StreamConfig(
                backends=[a, b], backend_selector=lambda ctx: a
            ),
        )
    one = streams.StreamConfig(backends=[a])
    replayer = Replayer(workflows=[Idle], stream_config=one)
    assert replayer.config(active_config=True).get("stream_config") is one


async def test_worker_configures_backends_after_its_plugins(client: Client):
    # A plugin can replace the Workflow runner, and the backend must see the runner in use.
    backend = _KeepsTheRunner()
    runner = SandboxedWorkflowRunner()
    plugin = SimplePlugin("replaces-runner", workflow_runner=runner)
    new_worker(
        _client(client, streams.StreamConfig(backends=[backend])),
        Idle,
        plugins=[plugin],
    )
    assert backend.runner is runner


async def test_workflow_environment_gives_the_config_to_its_client():
    config = streams.StreamConfig(backends=[_Backend()])
    async with await WorkflowEnvironment.start_local(stream_config=config) as env:
        assert env.client.config()["stream_config"] is config
