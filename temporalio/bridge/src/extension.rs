//! Worker extensions that this bridge build links, so Python can pass one to Core by its name.

use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use temporalio_sdk_core::WorkerExtension;

use crate::runtime;

/// An extension that a Worker config can carry. Python keeps it to reach the same instance that
/// Core calls.
#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
pub struct WorkerExtensionRef {
    pub(crate) extension: Arc<dyn WorkerExtension>,
}

#[pymethods]
impl WorkerExtensionRef {
    #[getter]
    fn name(&self) -> String {
        self.extension.name().to_string()
    }
}

/// Builds the extension with `name` from its encoded config. Only the extensions that this build
/// links are known.
// A release build links no extension yet, so it never reads the config.
#[cfg_attr(not(debug_assertions), allow(unused_variables))]
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
        #[cfg(debug_assertions)]
        test_extension::NAME => Arc::new(test_extension::TestExtension::new(&config)?),
        _ => {
            return Err(PyValueError::new_err(format!(
                "This build has no worker extension named {name}"
            )))
        }
    };
    Ok(WorkerExtensionRef { extension })
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
