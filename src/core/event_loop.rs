use std::{
    cell::RefCell,
    collections::{BinaryHeap, HashMap, VecDeque},
    iter,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    time::{Duration, Instant},
};

use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyList, PyTuple},
};
use tokio::runtime::Runtime;

use crate::{
    core::future::{PyFuture, RustFuture},
    core::task::{RustTask, StepResult, TaskId},
};

struct Timer {
    when: Instant,
    task_id: TaskId,
}

impl Eq for Timer {}
impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when
    }
}

impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.when.partial_cmp(&self.when)
    }
}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.when.cmp(&self.when)
    }
}

struct GatherState {
    total: usize,
    completed: usize,
    results: Option<Vec<Option<PyObject>>>,
}

impl GatherState {
    fn new(total: usize) -> Self {
        GatherState {
            total,
            completed: 0,
            results: Some(
                iter::repeat_with(|| None as Option<PyObject>)
                    .take(total)
                    .collect(),
            ),
        }
    }
    fn add_result(&mut self, idx: usize, res: PyObject) -> Option<Vec<PyObject>> {
        if let Some(vec) = self.results.as_mut() {
            vec[idx] = Some(res);
            self.completed += 1;
            if self.completed == self.total {
                let owned_vec = self.results.take().unwrap();
                return Some(owned_vec.into_iter().map(|x| x.unwrap()).collect());
            }
        }
        None
    }
}

struct TaskEntry {
    task: RustTask,
    future: RustFuture,
    pending_val: Option<PyObject>,
    pending_err: Option<PyErr>,
}

struct EventLoop {
    tasks_hm: HashMap<TaskId, TaskEntry>,
    task_deq: VecDeque<TaskId>,
    timers: BinaryHeap<Timer>,
    next_id: TaskId,
    sleep_type: Option<Py<PyAny>>,
    gather_type: Option<Py<PyAny>>,
    future_type: Option<Py<PyAny>>,
    wake_rx: Receiver<(TaskId, Result<PyObject, PyErr>)>,
    wake_tx: Sender<(TaskId, Result<PyObject, PyErr>)>,
    tokio_runtime: Runtime
}

impl EventLoop {
    fn new(py: Python) -> PyResult<Self> {
        let module = PyModule::import(py, "blazing_shustriy_asyncio")?;
        let sleep_t = module.getattr("_AsyncSleep")?.unbind();
        let gather_t = module.getattr("_AsyncGather")?.unbind();
        let future_t = module
            .getattr("PyFuture")
            .ok()
            .or_else(|| {
                PyModule::import(py, "blazing_shustriy_asyncio.rust_core")
                    .ok()?
                    .getattr("PyFuture")
                    .ok()
            })
            .unwrap()
            .unbind();

        let (tx, rx) = mpsc::channel();

        Ok(EventLoop {
            tasks_hm: HashMap::new(),
            task_deq: VecDeque::new(),
            timers: BinaryHeap::new(),
            next_id: 0,
            sleep_type: Some(sleep_t),
            gather_type: Some(gather_t),
            future_type: Some(future_t),
            wake_rx: rx,
            wake_tx: tx,
            tokio_runtime: Runtime::new().unwrap()
        })
    }

    fn create_task(&mut self, gen: &Bound<'_, PyAny>, py: Python) -> RustFuture {
        let coro = if gen.hasattr("__await__").unwrap_or(false) {
            match gen.call_method0("__await__") {
                Ok(x) => x,
                Err(exc) => {
                    let fut = RustFuture::new();
                    let _ = fut.set_exception(exc, py);
                    return fut;
                }
            }
        } else {
            gen.clone()
        };
        let id = self.next_id;
        self.next_id += 1;
        let task = RustTask::new(&coro);
        let fut = RustFuture::new();

        self.tasks_hm.insert(
            id,
            TaskEntry {
                task,
                future: fut.clone(),
                pending_val: None,
                pending_err: None,
            },
        );

        fut
    }

    fn schedule_task(&mut self, id: TaskId) {
        if self.tasks_hm.contains_key(&id) {
            self.task_deq.push_back(id);
        }
    }

    fn process_step(&mut self, id: TaskId, step_res: StepResult, py: Python) -> PyResult<()> {
        match step_res {
            StepResult::Yield(value) => self.handle_yield(id, value, py)?,
            StepResult::Success(result) => {
                if let Some(entry) = self.tasks_hm.remove(&id) {
                    entry.future.set_result(result, py)?;
                }
            }
            StepResult::Failure(err) => {
                if let Some(entry) = self.tasks_hm.remove(&id) {
                    entry.future.set_exception(err, py)?;
                }
            }
        }
        Ok(())
    }

    fn run_task(&mut self, py: Python, id: TaskId) -> PyResult<()> {
        if !self.tasks_hm.contains_key(&id) {
            return Ok(());
        }

        let entry = self.tasks_hm.get_mut(&id).unwrap();

        if let Some(err) = entry.pending_err.take() {
            let step_res = {
                let task_entry = self.tasks_hm.get(&id).unwrap();
                task_entry.task.throw(err, py)
            };
            return self.process_step(id, step_res, py);
        }

        let pending_val = entry.pending_val.take();

        let step_res = {
            let task_entry = self.tasks_hm.get(&id).unwrap();
            task_entry.task.step(pending_val, py)
        };

        self.process_step(id, step_res, py)
    }

    fn handle_yield(&mut self, id: TaskId, val: Py<PyAny>, py: Python) -> PyResult<()> {
        let val_binded = val.bind(py);
        if let Some(sleep_type) = &self.sleep_type {
            if val_binded.is_instance(sleep_type.bind(py))? {
                let duration: f64 = val_binded.getattr("duration")?.extract()?;
                let when = Instant::now() + Duration::from_secs_f64(duration);
                self.timers.push(Timer {
                    when: when,
                    task_id: id,
                });
                return Ok(());
            }
        }
        if let Some(gather_type) = &self.gather_type {
            if val_binded.is_instance(gather_type.bind(py))? {
                self.handle_gather(id, val_binded, py)?;
                return Ok(());
            }
        }

        if let Some(fut_val) = &self.future_type {
            if val_binded.is_instance(fut_val.bind(py)).unwrap_or(false) {
                if let Ok(py_future_ref) = val.extract::<PyRef<'_, PyFuture>>(py) {
                    let inner_future = py_future_ref.future.clone();
                    let tx_clone = self.wake_tx.clone();
                    let my_id = id;

                    inner_future.add_callback(
                        move |res: PyObject| {
                            tx_clone.send((my_id, Ok(res))).ok();
                        },
                        py,
                    );
                    let tx_err_callback = self.wake_tx.clone();
                    inner_future.add_err_callback(
                        move |err: PyErr| {
                            tx_err_callback.send((my_id, Err(err))).ok();
                        },
                        py,
                    );

                    return Ok(());
                }
            }
        }
        if val_binded.hasattr("send").unwrap_or(false) {
            let child_future = self.create_task(val_binded.downcast()?, py);
            let child_id = self.next_id - 1;
            self.schedule_task(child_id);

            let tx_clone: Sender<(usize, Result<Py<PyAny>, PyErr>)> = self.wake_tx.clone();
            let my_id = id;

            child_future.add_callback(
                move |res| {
                    tx_clone.send((my_id, Ok(res))).ok();
                },
                py,
            );
            let tx_err_callback = self.wake_tx.clone();
            child_future.add_err_callback(
                move |err| {
                    tx_err_callback.send((my_id, Err(err))).ok();
                },
                py,
            );

            return Ok(());
        }

        let entry = self.tasks_hm.get_mut(&id).unwrap();
        entry.pending_val = Some(val);
        self.task_deq.push_back(id);
        Ok(())
    }

    fn handle_gather(&mut self, id: TaskId, gather: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        let attr = gather.getattr("coros")?;
        let coros = attr.downcast::<PyTuple>()?;
        let total = coros.len();

        let state = Arc::new(RefCell::new(GatherState::new(total)));
        let tx_clone = self.wake_tx.clone();
        let my_id = id;

        for (idx, coro) in coros.iter().enumerate() {
            if !coro.hasattr("send").unwrap_or(false) {
                return Err(PyRuntimeError::new_err("All args must be generators"));
            }
            let child_fut = self.create_task(coro.downcast()?, py);
            let child_id = self.next_id - 1;
            self.schedule_task(child_id);

            let st = Arc::clone(&state);
            let tx_cb = tx_clone.clone();
            let my_idx = idx;

            child_fut.add_callback(
                move |res| {
                    let maybe_list = {
                        let mut s = st.borrow_mut();
                        s.add_result(my_idx, res)
                    };
                    if let Some(list) = maybe_list {
                        Python::with_gil(|py| {
                            let py_list = PyList::new(py, list).unwrap();
                            tx_cb.send((my_id, Ok(py_list.into()))).ok();
                        });
                    }
                },
                py,
            );
            let err_cb = tx_clone.clone();
            child_fut.add_err_callback(
                move |err| {
                    err_cb.send((my_id, Err(err))).ok();
                },
                py,
            );
        }
        Ok(())
    }
    fn run_forever(&mut self, py: Python) -> PyResult<()> {
        loop {
            while let Ok((id, res_result)) = self.wake_rx.try_recv() {
                if let Some(entry) = self.tasks_hm.get_mut(&id) {
                    match res_result {
                        Ok(val) => entry.pending_val = Some(val),
                        Err(err) => entry.pending_err = Some(err),
                    }
                    self.task_deq.push_back(id);
                }
            }

            if let Some(id) = self.task_deq.pop_front() {
                self.run_task(py, id)?;
                continue;
            }

            if let Some(next_timer) = self.timers.peek() {
                let now = Instant::now();
                if next_timer.when <= now {
                    let timer = self.timers.pop().unwrap();
                    self.task_deq.push_back(timer.task_id);
                    continue;
                } else {
                    let duration = next_timer.when.duration_since(now);
                    py.allow_threads(|| std::thread::sleep(duration));
                    continue;
                }
            }

            if self.tasks_hm.is_empty() {
                break;
            }
            py.check_signals()?;
        }
        Ok(())
    }

    fn run_until_complete(&mut self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.create_task(coro, py);
        let id = self.next_id - 1;
        self.schedule_task(id);
        self.run_forever(py)
    }
}

pub struct RustEventLoop {
    event_loop: Arc<RefCell<EventLoop>>,
}
impl RustEventLoop {
    pub fn new(py: Python) -> PyResult<Self> {
        Ok(Self {
            event_loop: Arc::new(RefCell::new(EventLoop::new(py)?)),
        })
    }
    pub fn create_task(&self, gen: &Bound<'_, PyAny>, py: Python) -> RustFuture {
        self.event_loop.borrow_mut().create_task(gen, py)
    }
    pub fn run_forever(&self, py: Python) -> PyResult<()> {
        self.event_loop.borrow_mut().run_forever(py)
    }
    pub fn run_until_complete(&self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.event_loop.borrow_mut().run_until_complete(coro, py)
    }
}

#[pyclass(unsendable)]
pub struct PyEventLoop {
    loop_impl: RustEventLoop,
}
#[pymethods]
impl PyEventLoop {
    #[new]
    fn new(py: Python) -> PyResult<Self> {
        Ok(Self {
            loop_impl: RustEventLoop::new(py)?,
        })
    }
    fn create_task(slf: &Bound<'_, Self>, gen: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let fut = slf.borrow().loop_impl.create_task(gen, py);
        Ok(Py::new(py, PyFuture { future: fut })?.into_any())
    }

    fn run_forever(&self, py: Python) -> PyResult<()> {
        self.loop_impl.run_forever(py)
    }
    fn run_until_complete(&self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.loop_impl.run_until_complete(coro, py)
    }
}
