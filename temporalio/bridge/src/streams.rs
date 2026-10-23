//! The Stream runtime of the temporalio-streams crate, when this build links it. A Stream plugin
//! builds one for each client from its config, and Python calls it by method name.

use pyo3::exceptions::PyException;
#[cfg(feature = "streams")]
use pyo3::prelude::*;

#[cfg(feature = "streams")]
use crate::runtime;

// Its args are the gRPC code, the message and the encoded details, as with `RPCError`.
pyo3::create_exception!(temporal_sdk_bridge, StreamCallError, PyException);

/// The crate's runtime object. Python keeps it on the plugin for the client that it serves.
#[cfg(feature = "streams")]
#[pyclass(frozen)]
pub struct StreamRuntimeRef {
    streams: std::sync::Arc<temporalio_streams::StreamExtension>,
    runtime: runtime::Runtime,
}

#[cfg(feature = "streams")]
#[pymethods]
impl StreamRuntimeRef {
    #[getter]
    fn name(&self) -> String {
        self.streams.name().to_string()
    }

    fn call<'p>(
        &self,
        py: Python<'p>,
        method: String,
        input: Vec<u8>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.runtime
            .assert_same_process("call the Stream runtime")?;
        let streams = self.streams.clone();
        self.runtime.future_into_py(py, async move {
            streams.call(&method, &input).await.map_err(call_error)
        })
    }

    /// Releases one handle of a writer with no wait. It needs no runtime, so a finalizer of a
    /// handle that Python drops can call it.
    fn free_writer(&self, writer_id: u64) -> PyResult<()> {
        self.streams.free_writer(writer_id).map_err(call_error)
    }
}

#[cfg(feature = "streams")]
fn call_error(err: temporalio_streams::StreamError) -> PyErr {
    StreamCallError::new_err((err.code() as u32, err.message.clone(), err.details()))
}

/// Builds the runtime on Redis, or on the memory Store with `memory`, from its encoded config.
#[cfg(feature = "streams")]
pub fn new_stream_runtime(
    runtime_ref: &runtime::RuntimeRef,
    memory: bool,
    config: Vec<u8>,
) -> PyResult<StreamRuntimeRef> {
    use pyo3::exceptions::PyValueError;

    let runtime = runtime_ref.runtime.clone();
    runtime.assert_same_process("create a Stream runtime")?;
    // The runtime may start tasks on the Tokio runtime that it lives on.
    let _guard = runtime.core.tokio_handle().enter();
    let meter = runtime.core.telemetry().get_temporal_metric_meter();
    let streams = if memory {
        temporalio_streams::MemoryStreamExtension::from_config_bytes(&config, meter)
    } else {
        temporalio_streams::StreamExtension::from_config_bytes(&config, meter)
    }
    .map_err(|err| PyValueError::new_err(err.to_string()))?;
    Ok(StreamRuntimeRef { streams, runtime })
}
