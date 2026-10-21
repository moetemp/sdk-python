from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import temporalio.converter
import temporalio.workflow
from temporalio.client import WorkflowHistory

if TYPE_CHECKING:
    from temporalio.worker import (
        Replayer,
        ReplayerConfig,
        Worker,
        WorkerConfig,
        WorkflowReplayResult,
    )


@dataclass(frozen=True)
class WorkflowActivationCompleteInput:
    """Input for :py:meth:`Plugin.on_workflow_activation_complete`.

    .. warning::
        This API is experimental.
    """

    run_id: str
    """Run ID of the Workflow."""

    info: temporalio.workflow.Info
    """Info of the Workflow."""

    is_replaying: bool
    """True if the Workflow replayed History in this Activation."""

    items: Sequence[Any]
    """Values that Workflow code gave to :py:func:`temporalio.workflow.send_to_plugins` in this
    Activation, in order. Values given during replay are not here, unless they were given with
    ``during_replay=True``."""

    data_converter: temporalio.converter.DataConverter
    """Data converter of the Worker, with the Workflow's serialization context."""


class Plugin(abc.ABC):
    """Base class for worker plugins that can intercept and modify worker behavior.

    Plugins allow customization of worker creation and execution processes
    through a chain of responsibility pattern. Each plugin can modify the worker
    configuration or intercept worker execution.
    """

    def name(self) -> str:
        """Get the qualified name of this plugin. Can be overridden if desired to provide a more appropriate name.

        Returns:
            The fully qualified name of the plugin class (module.classname).
        """
        return type(self).__module__ + "." + type(self).__qualname__

    @abc.abstractmethod
    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Hook called when creating a worker to allow modification of configuration.

        This method is called during worker creation and allows plugins to modify
        the worker configuration before the worker is fully initialized. Plugins
        can modify task queue names, adjust concurrency settings, add interceptors,
        or change other worker settings.

        Args:
            config: The worker configuration dictionary to potentially modify.

        Returns:
            The modified worker configuration.
        """

    @abc.abstractmethod
    async def run_worker(
        self, worker: Worker, next: Callable[[Worker], Awaitable[None]]
    ) -> None:
        """Hook called when running a worker to allow interception of execution.

        This method is called when the worker is started and allows plugins to
        intercept or wrap the worker execution. Plugins can add monitoring,
        custom lifecycle management, or other execution-time behavior.

        Args:
            worker: The worker instance to run.
            next: Callable to continue the worker execution.
        """

    async def on_workflow_activation_complete(
        self,
        input: WorkflowActivationCompleteInput,  # pyright: ignore[reportUnusedParameter]
    ) -> None:
        """Hook called at the end of each successful Workflow Activation on a Worker.

        The Worker calls it outside the sandbox, after it encodes the Activation's completion and
        before it sends the completion to Core. So the hook can run the payload codec and call
        native code. An exception fails the Workflow Task. The :py:class:`Replayer` never calls
        it. The default does nothing.

        .. warning::
            This API is experimental.

        Args:
            input: The Activation's values and context.
        """
        pass

    @abc.abstractmethod
    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Hook called when creating a replayer to allow modification of configuration.

        This should be used to configure anything in ReplayerConfig needed to make execution match
        the worker and client config. This could include interceptors, DataConverter, workflows, and more.

        Args:
            config: The replayer configuration dictionary to potentially modify.

        Returns:
            The modified replayer configuration.
        """

    @abc.abstractmethod
    def run_replayer(
        self,
        replayer: Replayer,
        histories: AsyncIterator[WorkflowHistory],
        next: Callable[
            [Replayer, AsyncIterator[WorkflowHistory]],
            AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]],
        ],
    ) -> AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]]:
        """Hook called when running a replayer to allow interception of execution."""
