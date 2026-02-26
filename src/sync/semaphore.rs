use super::helpers;
use crate::core::future::{PyFuture, RustFuture};
use pyo3::prelude::*;
use std::{cell::RefCell, collections::VecDeque, sync::Arc};

struct Semaphore {
    max_busy: usize,
    cur_busy: usize,
    waiters: VecDeque<RustFuture>,
}

impl Semaphore {
    fn new(max_busy: usize) -> Self {
        Semaphore {
            max_busy,
            cur_busy: 0,
            waiters: VecDeque::new(),
        }
    }

    fn acquire(&mut self, py: Python) -> PyResult<RustFuture> {
        let fut = RustFuture::new();
        if self.cur_busy >= self.max_busy {
            self.waiters.push_back(fut.clone());
            return Ok(fut);
        }

        self.cur_busy += 1;
        fut.set_result(py.None(), py)?;
        Ok(fut)
    }

    fn release(&mut self, py: Python) -> PyResult<()> {
        if self.waiters.len() > 0 {
            self.waiters
                .pop_front()
                .unwrap()
                .set_result(py.None(), py)?;
        } else {
            self.cur_busy -= 1;
        }

        Ok(())
    }
}

pub struct RustSemaphore {
    semaphore: Arc<RefCell<Semaphore>>,
}

impl RustSemaphore {
    pub fn new(max_busy: usize) -> Self {
        RustSemaphore {
            semaphore: Arc::new(RefCell::new(Semaphore::new(max_busy))),
        }
    }

    pub fn acquire(&self, py: Python) -> PyResult<RustFuture> {
        self.semaphore.borrow_mut().acquire(py)
    }

    pub fn release(&self, py: Python) -> PyResult<()> {
        self.semaphore.borrow_mut().release(py)
    }
}

#[pyclass(unsendable)]
pub struct PySemaphore {
    pub semaphore: RustSemaphore,
}

#[pymethods]
impl PySemaphore {
    #[new]
    pub fn new(max_busy: usize) -> Self {
        PySemaphore {
            semaphore: RustSemaphore::new(max_busy),
        }
    }

    pub fn acquire(&self, py: Python) -> PyResult<PyFuture> {
        let fut = self.semaphore.acquire(py)?;
        Ok(fut.into())
    }

    pub fn release(&self, py: Python) -> PyResult<()> {
        self.semaphore.release(py)
    }

    pub fn __aenter__(slf: PyRef<'_, Self>) -> PyResult<PyObject> {
        let py = slf.py();
        helpers::primitive_aenter(|p| slf.acquire(p), py)
    }

    pub fn __aexit__(
        slf: PyRef<'_, Self>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<PyObject>> {
        let py = slf.py();
        helpers::primitive_aexit(|p| slf.semaphore.release(p), py)
    }
}
