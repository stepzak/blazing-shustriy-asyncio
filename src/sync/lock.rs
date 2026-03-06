use super::helpers;
use crate::core::future::{PyFuture, RustFuture};
use pyo3::prelude::*;
use std::{cell::RefCell, collections::VecDeque, sync::Arc};

struct Lock {
    locked: bool,
    waiters: VecDeque<RustFuture>,
}

impl Lock {
    fn new() -> Self {
        Lock {
            locked: false,
            waiters: VecDeque::new(),
        }
    }

    fn acquire(&mut self, py: Python) -> PyResult<RustFuture> {
        let fut = RustFuture::new();
        if !self.locked {
            fut.set_result(py.None(), py)?;
            self.locked = true;
            return Ok(fut);
        } else {
            self.waiters.push_back(fut.clone());
            return Ok(fut);
        }
    }

    fn release(&mut self, py: Python) -> PyResult<()> {
        if self.waiters.len() > 0 {
            self.waiters
                .pop_front()
                .unwrap()
                .set_result(py.None(), py)?;
        } else {
            self.locked = false;
        }
        Ok(())
    }

    fn locked(&self) -> bool {
        self.locked
    }
}

pub struct RustLock {
    lock: Arc<RefCell<Lock>>,
}

impl RustLock {
    pub fn new() -> Self {
        RustLock {
            lock: Arc::new(RefCell::new(Lock::new())),
        }
    }

    pub fn acquire(&self, py: Python) -> PyResult<RustFuture> {
        self.lock.borrow_mut().acquire(py)
    }

    pub fn release(&self, py: Python) -> PyResult<()> {
        self.lock.borrow_mut().release(py)
    }

    pub fn locked(&self) -> bool {
        self.lock.borrow().locked()
    }
}

#[pyclass(unsendable)]
pub struct PyLock {
    pub lock: RustLock,
}

#[pymethods]
impl PyLock {
    #[new]
    pub fn new() -> Self {
        PyLock {
            lock: RustLock::new(),
        }
    }

    pub fn acquire(&self, py: Python) -> PyResult<PyFuture> {
        let fut = self.lock.acquire(py)?;
        let pyfut: PyFuture = fut.into();
        return Ok(pyfut);
    }

    pub fn release(&self, py: Python) -> PyResult<()> {
        self.lock.release(py)
    }

    pub fn locked(&self) -> bool {
        self.lock.locked()
    }

    fn __aenter__(slf: PyRef<'_, Self>, py: Python) -> PyResult<Py<PyAny>> {
        helpers::primitive_aenter(|p| slf.acquire(p), py)
    }

    pub fn __aexit__(
        &self,
        py: Python,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<Py<PyAny>>> {
        helpers::primitive_aexit(|p| self.release(p), py)
    }
}
