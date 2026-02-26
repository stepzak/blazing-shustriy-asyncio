use crate::core::future::PyFuture;
use pyo3::{prelude::*, PyResult};

pub fn primitive_aenter<F>(acquire_fn: F, py: Python) -> PyResult<PyObject>
where
    F: FnOnce(Python) -> PyResult<PyFuture>,
{
    let fut = acquire_fn(py)?;
    let py_fut: PyFuture = fut.into();
    Ok(Py::new(py, py_fut)?.into_any())
}

pub fn primitive_aexit<F>(release_fn: F, py: Python) -> PyResult<Option<PyObject>>
where
    F: FnOnce(Python) -> PyResult<()>,
{
    release_fn(py)?;
    Ok(None)
}
