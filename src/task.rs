use std::{cell::RefCell, sync::Arc};
use pyo3::{exceptions::PyStopIteration, prelude::*};

pub type TaskId = usize;

struct Task {
    coro: Py<PyAny>,
    task_id: TaskId
}

pub enum StepResult {
    Yield(PyObject),
    Success(PyObject),
    Failure(PyErr),
}

impl Task {
    fn new(coro: &Bound<'_, PyAny>, task_id: TaskId) -> Self {
        if !coro.hasattr("send").unwrap_or(false) {
            panic!("Function must be a generator");
        }
        Task {
            coro: coro.clone().unbind(),
            task_id: task_id
        }
    }

    fn step(&self, val: Option<PyObject>, py: Python) -> StepResult {
        let coro = self.coro.bind(py);
        let send_attr = match coro.getattr("send") {
            Err(e) => return StepResult::Failure(e),
            Ok(s) => s
        };
        let to_send = match val {
            None => py.None(),
            Some(x) => x
        };
        match send_attr.call1((to_send,)) {
            Ok(res) => {
                StepResult::Yield(res.into())
            }
            Err(err) => {
                if err.is_instance_of::<PyStopIteration>(py) {
                    let exc_value = err.value(py);
                    
                    let result_obj: PyObject = exc_value
                        .getattr("value")
                        .map(|b| b.into())
                        .unwrap_or_else(|_| py.None());
                    StepResult::Success(result_obj.into())
                }
                else {
                    StepResult::Failure(err)
                }
            }

        }
    }
}

#[derive(Clone)]
pub struct RustTask {
    inner: Arc<RefCell<Task>>
}

impl RustTask {
    pub fn new(coro: &Bound<'_, PyAny>, task_id: TaskId) -> Self {
        RustTask {
            inner: Arc::new(
                RefCell::new(
                    Task::new(coro, task_id)
                )
            )
        }
    }

    pub fn step(&self, val: Option<PyObject>, py: Python) -> StepResult {
        self.inner.borrow().step(val, py)
    }
}