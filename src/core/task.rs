use pyo3::{exceptions::PyStopIteration, prelude::*};
use std::{cell::RefCell, sync::Arc};

pub type TaskId = usize;

struct Task {
    coro: Py<PyAny>,
}

pub enum StepResult {
    Yield(Py<PyAny>),
    Success(Py<PyAny>),
    Failure(PyErr),
}

impl Task {
    fn new(coro: &Bound<'_, PyAny>) -> Self {
        if !coro.hasattr("send").unwrap_or(false) {
            panic!("Function must be a generator");
        }
        Task {
            coro: coro.clone().unbind(),
        }
    }

    fn call_method<F>(&self, method: F, py: Python) -> StepResult
    where
        F: FnOnce(&Bound<'_, PyAny>) -> PyResult<Py<PyAny>>,
    {
        let coro = self.coro.bind(py);

        match method(coro) {
            Ok(res) => StepResult::Yield(res),
            Err(e) => {
                if e.is_instance_of::<PyStopIteration>(py) {
                    let exc_value = e.value(py);

                    let res_obj: Py<PyAny> = exc_value
                        .getattr("value")
                        .map(|b| b.into())
                        .unwrap_or_else(|_| py.None())
                        .into();
                    StepResult::Success(res_obj)
                } else {
                    StepResult::Failure(e)
                }
            }
        }
    }

    fn throw(&self, err: PyErr, py: Python) -> StepResult {
        self.call_method(
            |coro| {
                let throw_method = coro.getattr("throw")?;
                Ok(throw_method.call1((err.value(py),))?.into())
            },
            py,
        )
    }

    fn step(&self, val: Option<Py<PyAny>>, py: Python) -> StepResult {
        self.call_method(
            |coro| {
                let send_method = coro.getattr("send")?;
                let val_send = match val {
                    Some(x) => x,
                    None => py.None(),
                };
                Ok(send_method.call1((val_send,))?.into())
            },
            py,
        )
    }
}

#[derive(Clone)]
pub struct RustTask {
    inner: Arc<RefCell<Task>>,
}

impl RustTask {
    pub fn new(coro: &Bound<'_, PyAny>) -> Self {
        RustTask {
            inner: Arc::new(RefCell::new(Task::new(coro))),
        }
    }

    pub fn step(&self, val: Option<Py<PyAny>>, py: Python) -> StepResult {
        self.inner.borrow().step(val, py)
    }

    pub fn throw(&self, err: PyErr, py: Python) -> StepResult {
        self.inner.borrow().throw(err, py)
    }
}
