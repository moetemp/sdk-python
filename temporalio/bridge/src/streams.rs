//! Bindings to the Stream runtime of the temporalio-streams crate, on the Redis Store of
//! temporalio-streams-redis or on the memory Store. A Stream backend keeps one runtime for each
//! Core runtime, and the clients of that Core runtime share it and its connections. Python calls
//! the Stream service by method name, and holds each writer as an object.

use pyo3::exceptions::PyException;
#[cfg(feature = "streams")]
use pyo3::prelude::*;

#[cfg(feature = "streams")]
use crate::runtime;

// The exception args are the gRPC code, the message and the encoded details, as in `RPCError`.
pyo3::create_exception!(temporal_sdk_bridge, StreamCallError, PyException);

/// The crate's runtime object. Python keeps one for each Core runtime, which its clients share.
#[cfg(feature = "streams")]
#[pyclass(frozen)]
pub struct StreamRuntimeRef {
    streams: std::sync::Arc<temporalio_streams::StreamRuntime>,
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

    /// Opens a writer from an encoded `OpenWriterRequest`. The future gives a [StreamWriterRef].
    fn open_writer<'p>(&self, py: Python<'p>, input: Vec<u8>) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("open a Stream writer")?;
        let streams = self.streams.clone();
        let runtime = self.runtime.clone();
        self.runtime.future_into_py(py, async move {
            let writer = streams
                .open_writer_bytes(&input)
                .await
                .map_err(call_error)?;
            Ok(StreamWriterRef {
                writer: Some(std::sync::Arc::new(writer)),
                runtime,
            })
        })
    }
}

/// One writer of the crate. Dropping the last reference releases it.
#[cfg(feature = "streams")]
#[pyclass(frozen)]
pub struct StreamWriterRef {
    // An Option, so the drop can release the writer while it holds the Tokio handle.
    writer: Option<std::sync::Arc<temporalio_streams::StreamWriter>>,
    runtime: runtime::Runtime,
}

#[cfg(feature = "streams")]
impl StreamWriterRef {
    // Each call moves a clone into its future, so a write in flight outlives the Python object.
    fn writer(&self) -> std::sync::Arc<temporalio_streams::StreamWriter> {
        self.writer
            .clone()
            .expect("A Stream writer is only taken in its drop")
    }
}

#[cfg(feature = "streams")]
#[pymethods]
impl StreamWriterRef {
    /// Writes an encoded `WriteRecordsRequest`, and gives the encoded `WriteRecordsResponse`.
    fn write<'p>(&self, py: Python<'p>, input: Vec<u8>) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("write to a Stream")?;
        let writer = self.writer();
        self.runtime.future_into_py(py, async move {
            writer.write_bytes(&input).await.map_err(call_error)
        })
    }

    /// Closes the writer, and gives the encoded `CloseWriterResponse`.
    fn close<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        self.runtime.assert_same_process("close a Stream writer")?;
        let writer = self.writer();
        self.runtime.future_into_py(
            py,
            async move { writer.close_bytes().await.map_err(call_error) },
        )
    }
}

#[cfg(feature = "streams")]
impl Drop for StreamWriterRef {
    fn drop(&mut self) {
        // The last reference to the Store can drop with the writer, and a Store's drop can need
        // an entered Tokio runtime.
        let _guard = self.runtime.core.tokio_handle().enter();
        drop(self.writer.take());
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
    // Building the Stream runtime can spawn Tokio tasks, which need an entered Tokio runtime.
    let _guard = runtime.core.tokio_handle().enter();
    let meter = runtime.core.telemetry().get_temporal_metric_meter();
    let streams = if memory {
        temporalio_streams::MemoryStreamRuntime::from_config_bytes(&config, meter)
            .map_err(|err| PyValueError::new_err(err.to_string()))?
    } else {
        temporalio_streams_redis::RedisStreamRuntime::from_config_bytes(&config, meter)
            .map_err(|err| PyValueError::new_err(err.to_string()))?
    };
    Ok(StreamRuntimeRef { streams, runtime })
}
