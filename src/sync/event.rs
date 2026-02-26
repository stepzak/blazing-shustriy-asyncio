use std::{cell::RefCell, collections::VecDeque, sync::Arc};

use crate::core::future::{PyFuture, RustFuture};
use pyo3::prelude::*;

struct Event {
    set: bool,
    waiters: VecDeque<RustFuture>,
}

impl Event {
    fn new() -> Self {
        Event {
            set: false,
            waiters: VecDeque::new(),
        }
    }

    fn wait(&mut self, py: Python) -> PyResult<RustFuture> {
        let fut = RustFuture::new();
        if !self.set {
            self.waiters.push_back(fut.clone());
            return Ok(fut);
        }
        fut.set_result(py.None(), py)?;
        Ok(fut)
    }

    fn set(&mut self, py: Python) -> PyResult<()> {
        if !self.set {
            self.set = true;
            let waiters_to_wake = std::mem::take(&mut self.waiters);

            for waiter in waiters_to_wake {
                let _ = waiter.set_result(py.None(), py);
            }
        }
        Ok(())
    }

    fn clear(&mut self) {
        self.set = false;
    }

    fn is_set(&self) -> bool {
        self.set
    }
}

pub struct RustEvent {
    event: Arc<RefCell<Event>>,
}

impl RustEvent {
    pub fn new() -> Self {
        RustEvent {
            event: Arc::new(RefCell::new(Event::new())),
        }
    }

    pub fn wait(&self, py: Python) -> PyResult<RustFuture> {
        self.event.borrow_mut().wait(py)
    }

    pub fn set(&self, py: Python) -> PyResult<()> {
        self.event.borrow_mut().set(py)
    }

    pub fn clear(&self) {
        self.event.borrow_mut().clear();
    }

    pub fn is_set(&self) -> bool {
        self.event.borrow().is_set()
    }
}

#[pyclass(unsendable)]
pub struct PyEvent {
    event: RustEvent,
}

#[pymethods]
impl PyEvent {
    #[new]
    pub fn new() -> Self {
        PyEvent {
            event: RustEvent::new(),
        }
    }

    pub fn wait(slf: PyRef<'_, Self>) -> PyResult<PyFuture> {
        let py = slf.py();
        let fut = slf.event.wait(py)?;
        let py_fut: PyFuture = fut.into();
        Ok(py_fut)
    }

    pub fn set(slf: PyRef<'_, Self>) -> PyResult<()> {
        slf.event.set(slf.py())
    }

    pub fn clear(slf: PyRef<'_, Self>) {
        slf.event.clear()
    }

    #[getter]
    pub fn is_set(slf: PyRef<'_, Self>) -> bool {
        slf.event.is_set()
    }
}
