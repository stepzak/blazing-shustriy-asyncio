use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::asyncio::{CancelledError, InvalidStateError};
use pyo3::{exceptions::PyStopIteration, prelude::*, IntoPyObjectExt};

use crate::core::task::TaskId;

pub type CallbackSuccess = Box<dyn FnOnce(Py<PyAny>, Python)>;
type CallbackErr = Box<dyn FnOnce(PyErr, Python)>;

enum FutureState {
    Pending,
    Success(Py<PyAny>),
    Failure(PyErr),
    Cancelled,
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
        F: FnOnce(Py<PyAny>, Python) + 'static,
    {
        match &self.state {
            FutureState::Pending => {
                if let Some(vec) = self.waiters_success.as_mut() {
                    vec.push(Box::new(callback));
                }
            }

            FutureState::Success(res) => callback(res.clone_ref(py), py),
            _ => {}
        }
    }

    fn add_err_callback<F>(&mut self, callback: F, py: Python)
    where
        F: FnOnce(PyErr, Python) + 'static,
    {
        match &self.state {
            FutureState::Pending => {
                if let Some(vec) = self.waiters_err.as_mut() {
                    vec.push(Box::new(callback))
                }
            }

            FutureState::Failure(e) => {
                callback(e.clone_ref(py), py);
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
                return Err(InvalidStateError::new_err("Future already finished"));
            }
        };
        for cb in callbacks {
            cb(res.clone_ref(py), py);
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
                return Err(InvalidStateError::new_err("Future already finished"));
            }
        };

        for cb in callbacks {
            cb(exc.clone_ref(py), py);
        }
        Ok(())
    }

    fn cancel(&mut self, py: Python) -> bool {
        if !self.is_done() {
            self.state = FutureState::Cancelled;
            let err = CancelledError::new_err("Future cancelled");
            self.waiters_success.take();
            if let Some(cbs) = self.waiters_err.take() {
                for cb in cbs {
                    cb(err.clone_ref(py), py);
                }
            }
            return true;
        }
        false
    }

    fn cancelled(&self) -> bool {
        matches!(self.state, FutureState::Cancelled)
    }

    fn is_done(&self) -> bool {
        return !matches!(self.state, FutureState::Pending);
    }

    fn result(&self, py: Python) -> PyResult<Py<PyAny>> {
        match &self.state {
            FutureState::Pending => Err(InvalidStateError::new_err("Future is not yet finished")),
            FutureState::Success(val) => Ok(val.clone_ref(py)),
            FutureState::Failure(e) => Err(e.clone_ref(py)),
            FutureState::Cancelled => Err(CancelledError::new_err("Future was cancelled")),
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
        F: FnOnce(Py<PyAny>, Python) + 'static,
    {
        self.inner.lock().add_callback(callback, py);
    }

    pub fn add_err_callback<F>(&self, callback: F, py: Python)
    where
        F: FnOnce(PyErr, Python) + 'static,
    {
        self.inner.lock().add_err_callback(callback, py);
    }

    pub fn set_result(&self, res: Py<PyAny>, py: Python) -> PyResult<()> {
        self.inner.lock().set_result(res, py)
    }

    pub fn set_exception(&self, exc: PyErr, py: Python) -> PyResult<()> {
        self.inner.lock().set_exception(exc, py)
    }

    pub fn cancel(&self, py: Python) -> bool {
        self.inner.lock().cancel(py)
    }

    pub fn cancelled(&self) -> bool {
        self.inner.lock().cancelled()
    }

    pub fn is_done(&self) -> bool {
        self.inner.lock().is_done()
    }

    pub fn result(&self, py: Python) -> PyResult<Py<PyAny>> {
        self.inner.lock().result(py)
    }
}

#[pyclass(unsendable)]
pub struct PyFuture {
    pub future: RustFuture,
    pub task_id: Option<TaskId>,
}

impl From<RustFuture> for PyFuture {
    fn from(value: RustFuture) -> Self {
        PyFuture {
            future: value,
            task_id: None,
        }
    }
}

#[pymethods]
impl PyFuture {
    #[new]
    pub fn new() -> Self {
        PyFuture {
            future: RustFuture::new(),
            task_id: None,
        }
    }

    pub fn set_result(&self, py: Python, val: Py<PyAny>) -> PyResult<()> {
        self.future.set_result(val, py)
    }

    pub fn set_task_id(&mut self, id: TaskId) {
        self.task_id = Some(id);
    }

    pub fn add_callback(&self, py: Python, cb: Py<PyAny>) -> PyResult<()> {
        let callback = move |res: Py<PyAny>, py: Python| {
            let _ = cb.call1(py, (res,));
        };
        self.future.add_callback(callback, py);
        Ok(())
    }

    pub fn set_exception(&self, py: Python, exc: Bound<'_, PyAny>) -> PyResult<()> {
        let err = PyErr::from_value(exc);
        self.future.set_exception(err, py)
    }

    pub fn add_err_callback(&self, py: Python, cb: Py<PyAny>) -> PyResult<()> {
        let callback = move |res: PyErr, py: Python| {
            let _ = cb.call1(py, (res,));
        };
        self.future.add_err_callback(callback, py);
        Ok(())
    }

    pub fn add_done_callback(&self, py: Python, cb: Py<PyAny>) -> PyResult<()> {
        self.add_callback(py, cb.clone_ref(py))?;
        self.add_err_callback(py, cb)
    }

    pub fn exception(&self, py: Python) -> Option<PyErr> {
        match &self.future.inner.lock().state {
            FutureState::Failure(err) => Some(err.clone_ref(py)),
            FutureState::Cancelled => Some(CancelledError::new_err("Future cancelled")),
            _ => None,
        }
    }

    pub fn cancel(&self, py: Python) -> bool {
        self.future.cancel(py)
    }

    pub fn cancelled(&self) -> bool {
        self.future.cancelled()
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
            Err(PyStopIteration::new_err(res))
        } else {
            Ok(Some(slf.into_py_any(py)?))
        }
    }
}
