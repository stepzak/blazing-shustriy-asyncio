use parking_lot::Mutex;
use pyo3::{exceptions::PyStopIteration, prelude::*};
use std::sync::Arc;

pub type TaskId = usize;

struct Task {
    context: Py<PyAny>,
    send_method: Py<PyAny>,
    throw_method: Py<PyAny>,
}

pub enum StepResult {
    Yield(Py<PyAny>),
    Success(Py<PyAny>),
    Failure(PyErr),
}

impl Task {
    fn new(coro: &Bound<'_, PyAny>, context: Py<PyAny>) -> Self {
        let send = coro.getattr("send").expect("Not a generator");
        let throw = coro.getattr("throw").expect("Not a generator");

        Task {
            context,
            send_method: send.unbind(),
            throw_method: throw.unbind(),
        }
    }

    fn call_method<F>(&self, method: F, py: Python) -> StepResult
    where
        F: FnOnce() -> PyResult<Py<PyAny>>,
    {
        let r = method();
        match r {
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
            || {
                let result =
                    self.context
                        .call_method1(py, "run", (self.throw_method.clone_ref(py), err))?;
                Ok(result)
            },
            py,
        )
    }

    fn step(&self, val: Option<Py<PyAny>>, py: Python) -> StepResult {
        self.call_method(
            || {
                let val_send = match val {
                    Some(x) => x,
                    None => py.None(),
                };
                let result = self.context.call_method1(
                    py,
                    "run",
                    (self.send_method.clone_ref(py), val_send),
                )?;
                Ok(result)
            },
            py,
        )
    }

    fn step_direct(&self, val: Option<Py<PyAny>>, py: Python) -> StepResult {
        let val_send = match val {
            Some(x) => x,
            None => py.None(),
        };
        match self.send_method.call1(py, (val_send,)) {
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

    fn throw_direct(&self, err: PyErr, py: Python) -> StepResult {
        match self.throw_method.call1(py, (err,)) {
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

    fn needs_context_switch(&self, current_ctx: &Bound<'_, PyAny>, py: Python) -> bool {
        !self.context.bind(py).is(current_ctx)
    }
}

#[derive(Clone)]
pub struct RustTask {
    inner: Arc<Mutex<Task>>,
}

impl RustTask {
    pub fn new(coro: &Bound<'_, PyAny>, context: Py<PyAny>) -> Self {
        RustTask {
            inner: Arc::new(Mutex::new(Task::new(coro, context))),
        }
    }

    pub fn step(&self, val: Option<Py<PyAny>>, direct: bool, py: Python) -> StepResult {
        if direct {
            self.inner.lock().step_direct(val, py)
        } else {
            self.inner.lock().step(val, py)
        }
    }

    pub fn throw(&self, err: PyErr, direct: bool, py: Python) -> StepResult {
        if direct {
            self.inner.lock().throw_direct(err, py)
        } else {
            self.inner.lock().throw(err, py)
        }
    }

    pub fn needs_context_switch(&self, current_ctx: &Bound<'_, PyAny>, py: Python) -> bool {
        self.inner.lock().needs_context_switch(current_ctx, py)
    }
}
