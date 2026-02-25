use pyo3::prelude::*;

mod future;
mod task;
mod event_loop;


/// A Python module implemented in Rust.
#[pymodule]
fn rust_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<future::PyFuture>()?;
    m.add_class::<event_loop::PyEventLoop>()?;
    Ok(())
}
