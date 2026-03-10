use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::{
    exceptions::{PyRuntimeError, PyStopIteration},
    prelude::*,
    IntoPyObjectExt,
};

pub type CallbackSuccess = Box<dyn FnOnce(Py<PyAny>)>;
type CallbackErr = Box<dyn FnOnce(PyErr)>;

enum FutureState {
    Pending,
    Success(Py<PyAny>),
    Failure(PyErr),
}

struct Future {
    state: FutureState,
    waiters_success: Option<Vec<CallbackSuccess>>,
    waiters_err: Option<Vec<CallbackErr>>,
}

impl Future {
    fn new() -> Self {
        Future {
            state: FutureState::Pending,
            waiters_success: Some(Vec::new()),
            waiters_err: Some(Vec::new()),
        }
    }

    fn add_callback<F>(&mut self, callback: F, py: Python)
    where
        F: FnOnce(Py<PyAny>) + 'static,
    {
        match &self.state {
            FutureState::Pending => {
                if let Some(vec) = self.waiters_success.as_mut() {
                    vec.push(Box::new(callback));
                }
            }

            FutureState::Success(res) => callback(res.clone_ref(py)),
            _ => {}
        }
    }

    fn add_err_callback<F>(&mut self, callback: F, py: Python)
    where
        F: FnOnce(PyErr) + 'static,
    {
        match &self.state {
            FutureState::Pending => {
                if let Some(vec) = self.waiters_err.as_mut() {
                    vec.push(Box::new(callback))
                }
            }

            FutureState::Failure(e) => {
                callback(e.clone_ref(py));
            }
            _ => {}
        }
    }

    fn set_result(&mut self, res: Py<PyAny>, py: Python) -> PyResult<()> {
        let callbacks = match &self.state {
            FutureState::Pending => {
                self.state = FutureState::Success(res.clone_ref(py));
                self.waiters_err.take();
                self.waiters_success.take().unwrap_or_default()
            }

            _ => {
                return Err(PyRuntimeError::new_err("Future already finished"));
            }
        };
        for cb in callbacks {
            cb(res.clone_ref(py));
        }
        Ok(())
    }

    fn set_exception(&mut self, exc: PyErr, py: Python) -> PyResult<()> {
        let callbacks = match &self.state {
            FutureState::Pending => {
                self.state = FutureState::Failure(exc.clone_ref(py));
                self.waiters_success.take();
                self.waiters_err.take().unwrap_or_default()
            }

            _ => {
                return Err(PyRuntimeError::new_err("Future already finished"));
            }
        };

        for cb in callbacks {
            cb(exc.clone_ref(py));
        }
        Ok(())
    }

    fn is_done(&self) -> bool {
        return !matches!(self.state, FutureState::Pending);
    }

    fn result(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match &self.state {
            FutureState::Pending => Err(PyRuntimeError::new_err("Future is not yet finished")),
            FutureState::Success(val) => Ok(Some(val.clone_ref(py))),
            FutureState::Failure(e) => Err(e.clone_ref(py)),
        }
    }
}

#[derive(Clone)]
pub struct RustFuture {
    inner: Arc<Mutex<Future>>,
}

impl RustFuture {
    pub fn new() -> Self {
        RustFuture {
            inner: Arc::new(Mutex::new(Future::new())),
        }
    }

    pub fn add_callback<F>(&self, callback: F, py: Python)
    where
        F: FnOnce(Py<PyAny>) + 'static,
    {
        self.inner.lock().add_callback(callback, py);
    }

    pub fn add_err_callback<F>(&self, callback: F, py: Python)
    where
        F: FnOnce(PyErr) + 'static,
    {
        self.inner.lock().add_err_callback(callback, py);
    }

    pub fn set_result(&self, res: Py<PyAny>, py: Python) -> PyResult<()> {
        self.inner.lock().set_result(res, py)
    }

    pub fn set_exception(&self, exc: PyErr, py: Python) -> PyResult<()> {
        self.inner.lock().set_exception(exc, py)
    }

    pub fn is_done(&self) -> bool {
        self.inner.lock().is_done()
    }

    pub fn result(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        self.inner.lock().result(py)
    }
}

#[pyclass(unsendable)]
pub struct PyFuture {
    pub future: RustFuture,
}

impl From<RustFuture> for PyFuture {
    fn from(value: RustFuture) -> Self {
        PyFuture { future: value }
    }
}

#[pymethods]
impl PyFuture {
    #[new]
    pub fn new() -> Self {
        PyFuture {
            future: RustFuture::new(),
        }
    }

    pub fn set_result(&self, py: Python, val: Py<PyAny>) -> PyResult<()> {
        self.future.set_result(val, py)
    }

    pub fn is_done(&self) -> bool {
        self.future.is_done()
    }

    pub fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>, py: Python) -> PyResult<Option<Py<PyAny>>> {
        if slf.is_done() {
            let res = slf.future.result(py)?;
            Err(PyStopIteration::new_err(res.unwrap_or_else(|| py.None())))
        } else {
            Ok(Some(slf.into_py_any(py)?))
        }
    }
}
