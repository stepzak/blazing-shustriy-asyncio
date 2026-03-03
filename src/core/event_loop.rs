use pyo3::{
    exceptions::{PyConnectionError, PyOSError, PyRuntimeError},
    prelude::*,
    types::{PyByteArray, PyList, PyTuple},
    IntoPyObjectExt,
};
use std::{
    cell::RefCell,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    iter,
    net::SocketAddr,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener as TokioListener, TcpStream as TokioStream},
    runtime::Runtime,
};

use crate::core::{
    future::{PyFuture, RustFuture},
    net::{AcceptIo, BindIo, ConnectIo, PyTcpListener, PyTcpStream, ReadIo, WriteIo},
    task::{RustTask, StepResult, TaskId},
    timers::Timer,
};

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
    pending_io: HashSet<TaskId>,
    tokio_runtime: Runtime,
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
            pending_io: HashSet::new(),
            tokio_runtime: Runtime::new().unwrap(),
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

    fn spawn(&mut self, gen: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        let id = self.next_id;
        self.create_task(gen, py);
        self.schedule_task(id);
        Ok(())
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

    fn handle_bind(
        &mut self,
        id: TaskId,
        addr: SocketAddr,
        pyclass: Py<PyTcpListener>,
    ) -> PyResult<()> {
        let tx = self.wake_tx.clone();
        self.pending_io.insert(id);
        self.tokio_runtime.spawn(async move {
            let listener = TokioListener::bind(addr).await;
            match listener {
                Ok(socket) => {
                    let result = Python::with_gil(|py| {
                        let mut py_listener = pyclass.bind(py).borrow_mut();
                        py_listener.set_listener(Arc::new(socket));

                        (pyclass, addr.to_string()).into_py_any(py).unwrap()
                    });

                    let _ = tx.send((id, Ok(result)));
                }
                Err(e) => {
                    let err = Python::with_gil(|_| {
                        PyConnectionError::new_err(format!("Bind error: {}", e))
                    });
                    let _ = tx.send((id, Err(err)));
                }
            }
        });
        Ok(())
    }

    fn handle_accept(&mut self, id: TaskId, arc: Arc<TokioListener>) -> PyResult<()> {
        let tx = self.wake_tx.clone();
        self.pending_io.insert(id);
        self.tokio_runtime.spawn(async move {
            let res = arc.accept().await;
            match res {
                Ok((stream, addr)) => {
                    let result = Python::with_gil(|py| {
                        let py_stream = PyTcpStream {
                            inner: Some(Arc::new(stream)),
                            addr: Some(addr),
                            closed: false,
                        };
                        let bound = py_stream.into_bound_py_any(py).unwrap();
                        (bound, addr.to_string()).into_py_any(py).unwrap()
                    });

                    let _ = tx.send((id, Ok(result)));
                }
                Err(e) => {
                    let err = Python::with_gil(|_| {
                        PyConnectionError::new_err(format!("Accept error: {}", e))
                    });

                    let _ = tx.send((id, Err(err)));
                }
            }
        });
        Ok(())
    }

    fn handle_connect(&mut self, id: TaskId, connect_io: Py<ConnectIo>) -> PyResult<()> {
        let tx = self.wake_tx.clone();
        self.pending_io.insert(id);
        self.tokio_runtime.spawn(async move {
            let (addr, pyclass) = Python::with_gil(|py| {
                let connect = connect_io.bind(py).borrow();
                (connect.addr, connect.pyclass.clone_ref(py))
            });

            match TokioStream::connect(addr).await {
                Ok(stream) => {
                    let result = Python::with_gil(|py| {
                        let mut py_stream = pyclass.bind(py).borrow_mut();
                        py_stream.set_stream(Arc::new(stream));
                        py_stream.set_addr(addr);
                        pyclass.into_py_any(py).unwrap()
                    });
                    let _ = tx.send((id, Ok(result)));
                }
                Err(e) => {
                    let err = Python::with_gil(|_| {
                        PyConnectionError::new_err(format!("Failed to connect: {}", e))
                    });
                    let _ = tx.send((id, Err(err)));
                }
            }
        });

        Ok(())
    }

    fn handle_read(&mut self, id: TaskId, read_io: Py<ReadIo>) -> PyResult<()> {
        let tx = self.wake_tx.clone();
        self.pending_io.insert(id);
        self.tokio_runtime.spawn(async move {
            let (stream, size) = Python::with_gil(|py| {
                let read = read_io.bind(py).borrow();
                let stream = read.stream.bind(py).borrow();
                (stream.inner.clone(), read.size)
            });
            let unwrapped = match stream {
                Some(x) => x,
                None => {
                    let err = Python::with_gil(|_| PyOSError::new_err("Socket not yet bound"));
                    let _ = tx.send((id, Err(err)));
                    return;
                }
            };
            match unwrapped.readable().await {
                Ok(_) => {
                    let mut buf = vec![0u8; size];
                    match unwrapped.try_read(&mut buf) {
                        Ok(0) => {
                            let res = Python::with_gil(|py| py.None().into_py_any(py).unwrap());
                            let _ = tx.send((id, Ok(res)));
                        }
                        Ok(n) => {
                            buf.truncate(n);
                            let res = Python::with_gil(|py| {
                                PyByteArray::new(py, &buf).into_py_any(py).unwrap()
                            });
                            let _ = tx.send((id, Ok(res)));
                        }
                        Err(e) => {
                            let err = Python::with_gil(|_| {
                                PyConnectionError::new_err(format!("Read error: {}", e))
                            });
                            let _ = tx.send((id, Err(err)));
                        }
                    }
                }
                Err(e) => {
                    let err = Python::with_gil(|_| {
                        PyConnectionError::new_err(format!("Read error: {}", e))
                    });
                    let _ = tx.send((id, Err(err)));
                }
            }
        });
        Ok(())
    }

    fn handle_write(&mut self, id: TaskId, write_io: Py<WriteIo>) -> PyResult<()> {
        let tx = self.wake_tx.clone();
        self.pending_io.insert(id);

        self.tokio_runtime.spawn(async move {
        let timeout = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(timeout);
        let (stream, data, written) = Python::with_gil(|py| {
            let write = write_io.bind(py).borrow();
            let stream_ref = write.stream.bind(py).borrow();
            (stream_ref.inner.clone(), write.data.clone(), write.written)
        });

        let stream = match stream {
            Some(s) => s,
            None => {
                let _ = tx.send((id, Err(Python::with_gil(|_| {
                    PyOSError::new_err("Socket not connected")
                }))));
                return;
            }
        };

        tokio::select! {
            _ = timeout => {
                let err = Python::with_gil(|_| {
                    PyConnectionError::new_err("Write timeout")
                });
                let _ = tx.send((id, Err(err)));
            }
            result = async {
                let mut total_written = written;
                let data_len = data.len();
                while total_written < data_len {
                    match stream.writable().await {
                        Ok(()) => match stream.try_write(&data[total_written..]) {
                            Ok(n) => {
                                total_written += n;
                                let _ = Python::with_gil(|py| {
                                    if let Ok(mut write) = write_io.bind(py).try_borrow_mut() {
                                        write.written = total_written;
                                    }
                                });
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                tokio::time::sleep(Duration::from_micros(100)).await;
                                continue;
                            }
                            Err(e) => return Err(PyConnectionError::new_err(format!("Write error: {}", e))),
                        },
                        Err(e) => return Err(PyConnectionError::new_err(format!("Write error: {}", e))),
                    }
                }
                Ok(total_written)
            } => {
                match result {
                    Ok(n) => {
                        let res = Python::with_gil(|py| n.into_py_any(py).unwrap());
                        let _ = tx.send((id, Ok(res)));
                    }
                    Err(e) => {
                        let _ = tx.send((id, Err(e)));
                    }
                }
            }
        }
    });

        Ok(())
    }

    fn handle_yield(&mut self, id: TaskId, val: Py<PyAny>, py: Python) -> PyResult<()> {
        let val_binded = val.bind(py);

        if let Ok(bind) = val_binded.extract::<Py<BindIo>>() {
            let binded = bind.bind(py).borrow_mut();

            return self.handle_bind(id, binded.addr, binded.pyclass.clone_ref(py));
        }

        if let Ok(accept) = val_binded.extract::<Py<AcceptIo>>() {
            let bound = accept.bind(py).borrow_mut();
            return self.handle_accept(id, bound.listener_arc.clone());
        }

        if let Ok(connect) = val_binded.extract::<Py<ConnectIo>>() {
            return self.handle_connect(id, connect);
        }

        if let Ok(read) = val_binded.extract::<Py<ReadIo>>() {
            return self.handle_read(id, read);
        }

        if let Ok(write) = val_binded.extract::<Py<WriteIo>>() {
            return self.handle_write(id, write);
        }

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
                self.pending_io.remove(&id);
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

                    if !self.pending_io.is_empty() {
                        let sleep_duration = std::cmp::min(duration, Duration::from_millis(10));
                        py.allow_threads(|| std::thread::sleep(sleep_duration));
                    } else {
                        py.allow_threads(|| std::thread::sleep(duration));
                    }
                    continue;
                }
            }

            if !self.pending_io.is_empty() {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }

            if self.tasks_hm.is_empty() {
                break;
            }
            py.check_signals()?;
        }
        self.shutdown();
        Ok(())
    }

    fn run_until_complete(&mut self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.create_task(coro, py);
        let id = self.next_id - 1;
        self.schedule_task(id);
        self.run_forever(py)
    }

    fn shutdown(&mut self) {
        for id in self.pending_io.drain() {
            if let Some(entry) = self.tasks_hm.get_mut(&id) {
                entry.pending_err = Some(PyRuntimeError::new_err("Event loop shutting down"));
                self.task_deq.push_back(id);
            }
        }
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
    pub fn spawn(&self, gen: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.event_loop.borrow_mut().spawn(gen, py)
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

    fn spawn(slf: &Bound<'_, Self>, gen: &Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        let _ = slf.borrow().loop_impl.spawn(gen, py);
        Ok(())
    }

    fn run_forever(&self, py: Python) -> PyResult<()> {
        self.loop_impl.run_forever(py)
    }
    fn run_until_complete(&self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.loop_impl.run_until_complete(coro, py)
    }
}
