//! Worker extensions that this bridge build links, so Python can pass one to Core by its name.

use std::sync::Arc;

use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use temporalio_client::Connection;
use temporalio_sdk_core::{
    ExtensionError, WorkerExtension, WorkflowTaskCompletionInfo, WorkflowTaskFailedInfo,
    WorkflowTaskInterceptError, WorkflowTaskInterceptor,
};

use crate::runtime;

// Its args are the gRPC code, the message and the encoded details, as with `RPCError`.
pyo3::create_exception!(temporal_sdk_bridge, ExtensionCallError, PyException);

/// An extension that a Worker config can carry. Python keeps it to reach the same instance that
/// Core calls.
#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
pub struct WorkerExtensionRef {
    pub(crate) extension: Arc<dyn WorkerExtension>,
    runtime: runtime::Runtime,
}

#[pymethods]
impl WorkerExtensionRef {
    #[getter]
    fn name(&self) -> String {
        self.extension.name().to_string()
    }

    /// Calls `method` and waits for its output. Workflow code can't await, so this is for code
    /// that runs between the activation and its completion.
    fn call(&self, py: Python<'_>, method: &str, input: Vec<u8>) -> PyResult<Vec<u8>> {
        self.runtime
            .assert_same_process("call a worker extension")?;
        let extension = self.extension.clone();
        py.detach(|| extension.call(method, &input))
            .map_err(call_error)
    }

    /// Gives the extension a client's connection, as a Worker gives it at its start, so a
    /// process with no Worker can call the server through it too.
    fn attach(&self, client: &crate::client::ClientRef) {
        self.extension.attach(client.connection.clone());
    }

    fn call_async<'p>(
        &self,
        py: Python<'p>,
        method: String,
        input: Vec<u8>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.runtime
            .assert_same_process("call a worker extension")?;
        let extension = self.extension.clone();
        self.runtime.future_into_py(py, async move {
            extension
                .call_async(method, input)
                .await
                .map_err(call_error)
        })
    }
}

/// Joins extensions into one, since Core takes one extension per Worker. Core calls the hooks of
/// each part in the given order. Python keeps each part to call its methods.
pub fn combine_worker_extensions(parts: Vec<WorkerExtensionRef>) -> PyResult<WorkerExtensionRef> {
    let Some(first) = parts.first() else {
        return Err(PyValueError::new_err("No worker extension to combine"));
    };
    let runtime = first.runtime.clone();
    let parts: Vec<Arc<dyn WorkerExtension>> =
        parts.into_iter().map(|part| part.extension).collect();
    let interceptors: Vec<_> = parts
        .iter()
        .filter_map(|part| {
            part.workflow_task_interceptor()
                .map(|interceptor| (part.name().to_string(), interceptor))
        })
        .collect();
    let name = parts
        .iter()
        .map(|part| part.name())
        .collect::<Vec<_>>()
        .join("+");
    let interceptor: Option<Arc<dyn WorkflowTaskInterceptor>> = if interceptors.is_empty() {
        None
    } else {
        Some(Arc::new(CombinedInterceptor(interceptors)))
    };
    Ok(WorkerExtensionRef {
        extension: Arc::new(CombinedExtension {
            name,
            parts,
            interceptor,
        }),
        runtime,
    })
}

struct CombinedExtension {
    name: String,
    parts: Vec<Arc<dyn WorkerExtension>>,
    // Core may ask for the interceptor more than once, so it is built once.
    interceptor: Option<Arc<dyn WorkflowTaskInterceptor>>,
}

impl WorkerExtension for CombinedExtension {
    fn name(&self) -> &str {
        &self.name
    }

    fn workflow_task_interceptor(&self) -> Option<Arc<dyn WorkflowTaskInterceptor>> {
        self.interceptor.clone()
    }

    fn attach(&self, connection: Connection) {
        for part in &self.parts {
            part.attach(connection.clone());
        }
    }
}

struct CombinedInterceptor(Vec<(String, Arc<dyn WorkflowTaskInterceptor>)>);

#[async_trait::async_trait]
impl WorkflowTaskInterceptor for CombinedInterceptor {
    async fn before_complete(
        &self,
        task: &WorkflowTaskCompletionInfo,
    ) -> Result<(), WorkflowTaskInterceptError> {
        // In order, so the first failure fails the task before a later part writes anything.
        for (name, part) in &self.0 {
            part.before_complete(task).await.map_err(|err| {
                WorkflowTaskInterceptError::new(format!("{name}: {}", err.message))
            })?;
        }
        Ok(())
    }

    async fn after_complete(&self, task: &WorkflowTaskCompletionInfo, accepted: bool) {
        // Together, since Core waits on all of them within one bound.
        futures::future::join_all(
            self.0
                .iter()
                .map(|(_, part)| part.after_complete(task, accepted)),
        )
        .await;
    }

    fn on_task_failed(&self, task: &WorkflowTaskFailedInfo) {
        for (_, part) in &self.0 {
            part.on_task_failed(task);
        }
    }

    fn on_evicted(&self, run_id: &str) {
        for (_, part) in &self.0 {
            part.on_evicted(run_id);
        }
    }
}

fn call_error(err: ExtensionError) -> PyErr {
    ExtensionCallError::new_err((err.code as u32, err.message, err.details))
}

/// Builds the extension with `name` from its encoded config. Only the extensions that this build
/// links are known.
// A release build with no extension feature never reads the config.
#[cfg_attr(
    not(any(debug_assertions, feature = "streams")),
    allow(unused_variables)
)]
pub fn new_worker_extension(
    runtime_ref: &runtime::RuntimeRef,
    name: &str,
    config: Vec<u8>,
) -> PyResult<WorkerExtensionRef> {
    let runtime = runtime_ref.runtime.clone();
    runtime.assert_same_process("create a worker extension")?;
    // An extension may start tasks on the runtime that it lives on.
    let _guard = runtime.core.tokio_handle().enter();
    let extension: Arc<dyn WorkerExtension> = match name {
        #[cfg(feature = "streams")]
        temporalio_streams::EXTENSION_NAME => {
            let meter = runtime.core.telemetry().get_temporal_metric_meter();
            temporalio_streams::RedisStreamExtension::from_config_bytes(&config, meter)
                .map_err(|err| PyValueError::new_err(err.to_string()))?
        }
        #[cfg(feature = "streams")]
        temporalio_streams::MEMORY_EXTENSION_NAME => {
            let meter = runtime.core.telemetry().get_temporal_metric_meter();
            temporalio_streams::MemoryStreamExtension::from_config_bytes(&config, meter)
                .map_err(|err| PyValueError::new_err(err.to_string()))?
        }
        #[cfg(debug_assertions)]
        test_extension::NAME => Arc::new(test_extension::TestExtension::new(&config)?),
        _ => {
            return Err(PyValueError::new_err(format!(
                "This build has no worker extension named {name}"
            )))
        }
    };
    Ok(WorkerExtensionRef { extension, runtime })
}

/// An extension for the SDK's own tests. Release builds leave it out.
#[cfg(debug_assertions)]
mod test_extension {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    use pyo3::exceptions::PyValueError;
    use pyo3::PyResult;
    use temporalio_sdk_core::{
        ExtensionError, WorkerExtension, WorkflowTaskCompletionInfo, WorkflowTaskInterceptError,
        WorkflowTaskInterceptor,
    };

    pub(super) const NAME: &str = "temporalio-test";

    pub(super) struct TestExtension {
        interceptor: Arc<CountingInterceptor>,
    }

    impl TestExtension {
        /// The config is empty, or `fail_before_complete` to fail every workflow task.
        pub(super) fn new(config: &[u8]) -> PyResult<Self> {
            let fail_before_complete = match config {
                b"" => false,
                b"fail_before_complete" => true,
                _ => return Err(PyValueError::new_err("Unknown test extension config")),
            };
            Ok(Self {
                interceptor: Arc::new(CountingInterceptor {
                    fail_before_complete,
                    before: AtomicU64::new(0),
                    after: AtomicU64::new(0),
                    accepted: AtomicU64::new(0),
                }),
            })
        }
    }

    #[async_trait::async_trait]
    impl WorkerExtension for TestExtension {
        fn name(&self) -> &str {
            NAME
        }

        fn workflow_task_interceptor(&self) -> Option<Arc<dyn WorkflowTaskInterceptor>> {
            Some(self.interceptor.clone())
        }

        fn call(&self, method: &str, input: &[u8]) -> Result<Vec<u8>, ExtensionError> {
            match method {
                "Echo" => Ok(input.to_vec()),
                "Fail" => Err(ExtensionError::new(tonic::Code::NotFound, "test failure")
                    .with_details(input.to_vec())),
                // Comma separated, so a test reads it with no proto.
                "Counts" => Ok(format!(
                    "{},{},{}",
                    self.interceptor.before.load(Ordering::SeqCst),
                    self.interceptor.after.load(Ordering::SeqCst),
                    self.interceptor.accepted.load(Ordering::SeqCst)
                )
                .into_bytes()),
                _ => Err(ExtensionError::unknown_method(method)),
            }
        }

        async fn call_async(
            &self,
            method: String,
            input: Vec<u8>,
        ) -> Result<Vec<u8>, ExtensionError> {
            tokio::task::yield_now().await;
            self.call(&method, &input)
        }
    }

    struct CountingInterceptor {
        fail_before_complete: bool,
        before: AtomicU64,
        after: AtomicU64,
        accepted: AtomicU64,
    }

    #[async_trait::async_trait]
    impl WorkflowTaskInterceptor for CountingInterceptor {
        async fn before_complete(
            &self,
            _task: &WorkflowTaskCompletionInfo,
        ) -> Result<(), WorkflowTaskInterceptError> {
            self.before.fetch_add(1, Ordering::SeqCst);
            if self.fail_before_complete {
                return Err(WorkflowTaskInterceptError::new(
                    "test extension refused the task",
                ));
            }
            Ok(())
        }

        async fn after_complete(&self, _task: &WorkflowTaskCompletionInfo, accepted: bool) {
            self.after.fetch_add(1, Ordering::SeqCst);
            if accepted {
                self.accepted.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}
