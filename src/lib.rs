use pyo3::prelude::*;



mod core;
mod http;
mod sync;

/// A Python module implemented in Rust.
#[pymodule]
fn rust_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<core::future::PyFuture>()?;
    m.add_class::<core::event_loop::PyEventLoop>()?;
    m.add_class::<core::net::PyTcpListener>()?;
    m.add_class::<core::net::PyTcpStream>()?;

    let sync_mod = PyModule::new(m.py(), "sync")?;
    sync_mod.add_class::<sync::lock::PyLock>()?;
    sync_mod.add_class::<sync::semaphore::PySemaphore>()?;
    sync_mod.add_class::<sync::event::PyEvent>()?;
    m.add_submodule(&sync_mod)?;

    let http_mod = PyModule::new(m.py(), "http")?;
    http_mod.add_class::<http::request::BlazingRequest>()?;
    http_mod.add_class::<http::response::BlazingResponse>()?;
    http_mod.add_class::<http::router::PyRouter>()?;
    m.add_submodule(&http_mod)?;

    Ok(())
}
