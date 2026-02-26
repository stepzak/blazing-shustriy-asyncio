use std::{cell::RefCell, sync::Arc};

use pyo3::{
    IntoPyObjectExt, exceptions::{PyRuntimeError, PyStopIteration}, prelude::*
};

pub type CallbackSuccess = Box<dyn FnOnce(PyObject)>;
type CallbackErr = Box<dyn FnOnce(PyErr)>;

enum FutureState {
    Pending,
    Success(PyObject),
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
        F: FnOnce(PyObject) + 'static,
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

    fn set_result(&mut self, res: PyObject, py: Python) -> PyResult<()> {
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

    fn result(&self, py: Python) -> PyResult<Option<PyObject>> {
        match &self.state {
            FutureState::Pending => Err(PyRuntimeError::new_err("Future is not yet finished")),
            FutureState::Success(val) => Ok(Some(val.clone_ref(py))),
            FutureState::Failure(e) => Err(e.clone_ref(py)),
        }
    }
}

#[derive(Clone)]
pub struct RustFuture {
    inner: Arc<RefCell<Future>>,
}

impl RustFuture {
    pub fn new() -> Self {
        RustFuture {
            inner: Arc::new(RefCell::new(Future::new())),
        }
    }

    pub fn add_callback<F>(&self, callback: F, py: Python)
    where
        F: FnOnce(PyObject) + 'static,
    {
        self.inner.borrow_mut().add_callback(callback, py);
    }

    pub fn add_err_callback<F>(&self, callback: F, py: Python)
    where
        F: FnOnce(PyErr) + 'static,
    {
        self.inner.borrow_mut().add_err_callback(callback, py);
    }

    pub fn set_result(&self, res: PyObject, py: Python) -> PyResult<()> {
        self.inner.borrow_mut().set_result(res, py)
    }

    pub fn set_exception(&self, exc: PyErr, py: Python) -> PyResult<()> {
        self.inner.borrow_mut().set_exception(exc, py)
    }

    pub fn is_done(&self) -> bool {
        self.inner.borrow().is_done()
    }

    pub fn result(&self, py: Python) -> PyResult<Option<PyObject>> {
        self.inner.borrow().result(py)
    }
}

#[pyclass(unsendable)]
pub struct PyFuture {
    pub future: RustFuture,
}

#[pymethods]
impl PyFuture {
    #[new]
    pub fn new() -> Self {
        PyFuture {
            future: RustFuture::new(),
        }
    }

    pub fn set_result(&self, py: Python, val: PyObject) -> PyResult<()> {
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

    pub fn __next__(slf: PyRef<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        if slf.is_done() {
            let res = slf.future.result(py)?;
            Err(PyStopIteration::new_err(res.unwrap_or_else(|| py.None())))
        } else {
            Ok(Some(slf.into_py_any(py)?))
        }

    }
}
