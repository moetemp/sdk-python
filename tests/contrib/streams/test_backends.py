from typing import Any, cast

import pytest

from temporalio import common, workflow
from temporalio.client import Client
from temporalio.plugin import SimplePlugin
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, WorkerConfig
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
from tests.helpers import new_worker


class _Backend(common.StreamBackend):
    # Registration makes no Stream call, so this backend needs no Store.
    def __init__(self, name: str = "test", *, default: bool = False) -> None:
        super().__init__(f"test-streams-{name}", backend_name=name, default=default)


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


async def test_client_checks_its_backends(client: Client):
    config = client.config()
    config["stream_backends"] = [_Backend("a"), _Backend("b", default=True)]
    two = Client(**config)
    names = [b.backend_name for b in two.config(active_config=True)["stream_backends"]]
    assert names == ["a", "b"]
    # The default is part of Workflow code, so it must not depend on the order of the list.
    for backends in (
        [_Backend("a"), _Backend("b")],
        [_Backend("a", default=True), _Backend("b", default=True)],
    ):
        config["stream_backends"] = backends
        with pytest.raises(common.StreamConfigError, match="exactly one"):
            Client(**config)
    config["stream_backends"] = [_Backend(), _Backend()]
    with pytest.raises(common.StreamConfigError, match="two Stream backends"):
        Client(**config)
    config["stream_backends"] = cast(Any, [object()])
    with pytest.raises(TypeError, match="not a StreamBackend"):
        Client(**config)
    # A generator of backends works too, and a copy of the client keeps them.
    config["stream_backends"] = cast(Any, (b for b in [_Backend()]))
    copied = Client(**Client(**config).config())
    assert len(copied.config(active_config=True)["stream_backends"]) == 1


async def test_backend_in_plugins_raises(client: Client):
    # The connect refuses it before it connects, so the unreachable target doesn't matter.
    with pytest.raises(common.StreamConfigError, match="not a plugin"):
        await Client.connect("localhost:1", plugins=[_Backend()])
    config = client.config()
    config["plugins"] = [_Backend()]
    with pytest.raises(common.StreamConfigError, match="not a plugin"):
        Client(**config)
    with pytest.raises(common.StreamConfigError, match="not a plugin"):
        new_worker(client, Idle, plugins=[_Backend()])
    with pytest.raises(common.StreamConfigError, match="not a plugin"):
        Replayer(workflows=[Idle], plugins=[_Backend()])


def test_replayer_checks_its_backends():
    with pytest.raises(common.StreamConfigError, match="two Stream backends"):
        Replayer(workflows=[Idle], stream_backends=[_Backend(), _Backend()])
    replayer = Replayer(workflows=[Idle], stream_backends=[_Backend()])
    assert len(replayer.config(active_config=True).get("stream_backends", ())) == 1


async def test_worker_configures_backends_after_its_plugins(client: Client):
    # A plugin can replace the Workflow runner, and the backend must see the runner in use.
    backend = _KeepsTheRunner(default=True)
    config = client.config()
    config["stream_backends"] = [backend]
    runner = SandboxedWorkflowRunner()
    plugin = SimplePlugin("replaces-runner", workflow_runner=runner)
    new_worker(Client(**config), Idle, plugins=[plugin])
    assert backend.runner is runner


async def test_workflow_environment_gives_backends_to_its_client():
    backend = _Backend()
    async with await WorkflowEnvironment.start_local(stream_backends=[backend]) as env:
        assert list(env.client.config()["stream_backends"]) == [backend]
