use crossbeam_channel::{self as cb, Receiver, Sender, select};
use pyo3::{
    exceptions::{PyConnectionError, PyRuntimeError},
    prelude::*,
    types::{PyByteArray, PyList, PyTuple},
    IntoPyObjectExt,
};
use std::{
    cell::RefCell,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    iter,
    net::SocketAddr,
    sync::Arc,
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

enum IoResult {
    Bind {
        listener_id: usize,
        listener: Arc<TokioListener>,
        addr: SocketAddr,
    },
    Accept {
        stream_id: usize,
        stream: Arc<TokioStream>,
        addr: SocketAddr,
    },
    Connect {
        stream_id: usize,
        stream: Arc<TokioStream>,
        addr: SocketAddr,
    },
    Read {
        data: Option<Vec<u8>>,
    },
    Write {
        n: usize,
    },
    Error(PyErr),
}

enum Command {
    Spawn(Py<PyAny>),
    Stop,
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
    pending_io: HashSet<TaskId>,
    tokio_runtime: Runtime,
    io_rx: Receiver<(usize, IoResult)>,
    io_tx: Sender<(usize, IoResult)>,
    cmd_rx: Receiver<Command>,
    pending_listeners: HashMap<usize, Py<PyTcpListener>>,
    listeners: HashMap<usize, (Arc<TokioListener>, SocketAddr, Py<PyTcpListener>)>,
    next_listener_id: usize,
    pending_streams: HashMap<usize, Py<PyTcpStream>>,
    streams: HashMap<usize, (Arc<TokioStream>, SocketAddr, Py<PyTcpStream>)>,
    next_stream_id: usize,
}

impl EventLoop {
    fn new(
        py: Python,
        wake_rx: Receiver<(TaskId, Result<PyObject, PyErr>)>,
        wake_tx: Sender<(TaskId, Result<PyObject, PyErr>)>,
        io_rx: Receiver<(usize, IoResult)>,
        io_tx: Sender<(usize, IoResult)>,
        cmd_rx: Receiver<Command>,
    ) -> PyResult<Self> {
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

        Ok(EventLoop {
            tasks_hm: HashMap::new(),
            task_deq: VecDeque::new(),
            timers: BinaryHeap::new(),
            next_id: 0,
            sleep_type: Some(sleep_t),
            gather_type: Some(gather_t),
            future_type: Some(future_t),
            wake_rx,
            wake_tx,
            pending_io: HashSet::new(),
            tokio_runtime: Runtime::new().unwrap(),
            io_rx,
            io_tx,
            cmd_rx,
            pending_listeners: HashMap::new(),
            listeners: HashMap::new(),
            streams: HashMap::new(),
            next_stream_id: 0,
            next_listener_id: 0,
            pending_streams: HashMap::new(),
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

    fn handle_bind(
        &mut self,
        id: TaskId,
        addr: SocketAddr,
        pyclass: Py<PyTcpListener>,
    ) -> PyResult<()> {
        let tx = self.io_tx.clone();
        self.pending_io.insert(id);
        let listener_id = self.next_listener_id;
        self.next_listener_id += 1;
        self.pending_listeners.insert(listener_id, pyclass);
        self.tokio_runtime.spawn(async move {
            let listener = TokioListener::bind(addr).await;
            match listener {
                Ok(socket) => {
                    let _ = tx.send((
                        id,
                        IoResult::Bind {
                            listener_id,
                            listener: Arc::new(socket),
                            addr,
                        },
                    ));
                }
                Err(e) => {
                    let err = PyConnectionError::new_err(format!("Bind error: {}", e));
                    let _ = tx.send((id, IoResult::Error(err)));
                }
            }
        });
        Ok(())
    }

    fn handle_accept(&mut self, id: TaskId, listener_id: usize) -> PyResult<()> {
        let tx = self.io_tx.clone();
        self.pending_io.insert(id);
        let stream_id = self.next_stream_id;
        self.next_stream_id += 1;

        if let Some(entry) = self.listeners.get(&listener_id) {
            let (listener_arc, _, _) = entry;
            let listener = Arc::clone(listener_arc);
            self.tokio_runtime.spawn(async move {
                let res = listener.accept().await;
                match res {
                    Ok((stream, addr)) => {
                        let _ = tx.send((
                            id,
                            IoResult::Accept {
                                stream_id,
                                stream: Arc::new(stream),
                                addr,
                            },
                        ));
                    }
                    Err(e) => {
                        let err = PyConnectionError::new_err(format!("Accept error: {}", e));
                        let _ = tx.send((id, IoResult::Error(err)));
                    }
                }
            });
        } else {
            let err = PyRuntimeError::new_err("Listener not found");
            self.pending_io.remove(&id);
            let _ = tx.send((id, IoResult::Error(err)));
        }

        Ok(())
    }

    fn handle_connect(
        &mut self,
        id: TaskId,
        addr: SocketAddr,
        py_stream: Py<PyTcpStream>,
    ) -> PyResult<()> {
        let tx = self.io_tx.clone();
        self.pending_io.insert(id);
        let stream_id = self.next_stream_id;
        self.next_stream_id += 1;
        self.pending_streams.insert(stream_id, py_stream);

        self.tokio_runtime.spawn(async move {
            match TokioStream::connect(addr).await {
                Ok(stream) => {
                    let _ = tx.send((
                        id,
                        IoResult::Connect {
                            stream_id,
                            stream: Arc::new(stream),
                            addr,
                        },
                    ));
                }
                Err(e) => {
                    let err = PyConnectionError::new_err(format!("Failed to connect: {}", e));
                    let _ = tx.send((id, IoResult::Error(err)));
                }
            }
        });

        Ok(())
    }

    fn handle_read(&mut self, id: TaskId, stream_id: usize, size: usize) -> PyResult<()> {
        let tx = self.io_tx.clone();
        self.pending_io.insert(id);

        if let Some((stream_orig, _, _)) = self.streams.get(&stream_id) {
            let stream = stream_orig.clone();
            self.tokio_runtime.spawn(async move {
                match stream.readable().await {
                    Ok(_) => {
                        let mut buf = vec![0u8; size];
                        match stream.try_read(&mut buf) {
                            Ok(0) => {
                                let _ = tx.send((
                                    id,
                                    IoResult::Read {
                                        data: None,
                                    },
                                ));
                            }
                            Ok(n) => {
                                buf.truncate(n);
                                let _ = tx.send((
                                    id,
                                    IoResult::Read {
                                        data: Some(buf),
                                    },
                                ));
                            }
                            Err(e) => {
                                let err = PyConnectionError::new_err(format!("Read error: {}", e));
                                let _ = tx.send((id, IoResult::Error(err)));
                            }
                        }
                    }
                    Err(e) => {
                        let err = PyConnectionError::new_err(format!("Read error: {}", e));
                        let _ = tx.send((id, IoResult::Error(err)));
                    }
                }
            });
        } else {
            let err = PyRuntimeError::new_err("Stream not found");
            self.pending_io.remove(&id);
            let _ = tx.send((id, IoResult::Error(err)));
        }

        Ok(())
    }

    fn handle_write(&mut self, id: TaskId, stream_id: usize, data: Vec<u8>) -> PyResult<()> {
        let tx = self.io_tx.clone();
        self.pending_io.insert(id);
        
        if let Some((stream_orig, _, _)) = self.streams.get(&stream_id) {
            let stream = stream_orig.clone();
            let data_len = data.len();
            
            self.tokio_runtime.spawn(async move {
                let timeout = tokio::time::sleep(Duration::from_secs(30));
                tokio::pin!(timeout);

                tokio::select! {
                    _ = timeout => {
                        let err = PyConnectionError::new_err("Write timeout");
                        let _ = tx.send((id, IoResult::Error(err)));
                    }
                    result = async {
                        let mut total_written = 0;
                        while total_written < data_len {
                            match stream.writable().await {
                                Ok(()) => match stream.try_write(&data[total_written..]) {
                                    Ok(n) => {
                                        total_written += n;
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
                                let _ = tx.send((id, IoResult::Write { n }));
                            }
                            Err(e) => {
                                let _ = tx.send((id, IoResult::Error(e)));
                            }
                        }
                    }
                }
            });
        } else {
            let err = PyRuntimeError::new_err("Stream not found");
            self.pending_io.remove(&id);
            let _ = tx.send((id, IoResult::Error(err)));
        }

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
            return self.handle_accept(id, bound.listener_id);
        }

        if let Ok(connect) = val_binded.extract::<Py<ConnectIo>>() {
            let bound = connect.bind(py).borrow_mut();
            let addr = bound.addr;
            let py_stream = bound.pyclass.clone_ref(py);
            return self.handle_connect(id, addr, py_stream);
        }

        if let Ok(read) = val_binded.extract::<Py<ReadIo>>() {
            let bound = read.bind(py).borrow();
            let size = bound.size;
            let stream_id = bound.stream_id;
            return self.handle_read(id, stream_id, size);
        }

        if let Ok(write) = val_binded.extract::<Py<WriteIo>>() {
            let bound = write.bind(py).borrow();
            let data = bound.data.clone();
            let stream_id = bound.stream_id;
            return self.handle_write(id, stream_id, data);
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

            let tx_clone = self.wake_tx.clone();
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

    fn handle_wake(
        &mut self,
        id: TaskId,
        res_result: Result<PyObject, PyErr>,
    ) -> PyResult<()> {
        self.pending_io.remove(&id);
        if let Some(entry) = self.tasks_hm.get_mut(&id) {
            match res_result {
                Ok(val) => entry.pending_val = Some(val),
                Err(err) => entry.pending_err = Some(err),
            }
            self.task_deq.push_back(id);
        }
        Ok(())
    }

    fn handle_io(&mut self, id: TaskId, result: IoResult, py: Python) -> PyResult<()> {
        match result {
            IoResult::Bind {
                listener_id,
                listener,
                addr,
            } => {
                if let Some(pyclass) = self.pending_listeners.remove(&listener_id) {
                    if let Ok(mut py_listener) = pyclass.bind(py).try_borrow_mut() {
                        py_listener.set_listener_id(listener_id);
                    }
                    self.listeners
                        .insert(listener_id, (listener, addr, pyclass.clone_ref(py)));
                    if let Some(entry) = self.tasks_hm.get_mut(&id) {
                        entry.pending_val =
                            Some((pyclass, addr.to_string()).into_py_any(py).unwrap());
                        self.task_deq.push_back(id);
                    }
                }
            }
            IoResult::Accept {
                stream_id,
                stream,
                addr,
            } => {
                let res = PyTcpStream {
                    stream_id: Some(stream_id),
                    addr: Some(addr),
                    closed: false,
                };
                let obj = Py::new(py, res)?;
                self.streams
                    .insert(stream_id, (stream, addr, obj.clone_ref(py)));

                if let Some(entry) = self.tasks_hm.get_mut(&id) {
                    let result = (obj, addr.to_string()).into_py_any(py).unwrap();
                    entry.pending_val = Some(result);
                    self.task_deq.push_back(id);
                }
            }
            IoResult::Connect {
                stream_id,
                stream,
                addr,
            } => {
                if let Some(pyclass) = self.pending_streams.remove(&stream_id) {
                    if let Ok(mut pyref) = pyclass.bind(py).try_borrow_mut() {
                        pyref.set_addr(addr);
                        pyref.set_stream_id(stream_id);
                    }
                    self.streams
                        .insert(stream_id, (stream, addr, pyclass.clone_ref(py)));
                    if let Some(entry) = self.tasks_hm.get_mut(&id) {
                        entry.pending_val =
                            Some((pyclass, addr.to_string()).into_py_any(py).unwrap());
                        self.task_deq.push_back(id);
                    }
                }
            }
            IoResult::Read { data } => {
                let result = data
                    .map(|buf| PyByteArray::new(py, &buf).into_py_any(py).unwrap())
                    .unwrap_or_else(|| py.None().into_py_any(py).unwrap());

                if let Some(entry) = self.tasks_hm.get_mut(&id) {
                    entry.pending_val = Some(result);
                    self.task_deq.push_back(id);
                }
            }
            IoResult::Write { n } => {
                let result = n.into_py_any(py).unwrap();
                if let Some(entry) = self.tasks_hm.get_mut(&id) {
                    entry.pending_val = Some(result);
                    self.task_deq.push_back(id);
                }
            }
            IoResult::Error(err) => {
                if let Some(entry) = self.tasks_hm.get_mut(&id) {
                    entry.pending_err = Some(err);
                    self.task_deq.push_back(id);
                }
            }
        }
        Ok(())
    }

    fn handle_command(&mut self, cmd: Command, py: Python) -> PyResult<()> {
        match cmd {
            Command::Spawn(gen) => {
                let gen_bound = gen.bind(py);
                self.create_task(gen_bound, py);
                let id = self.next_id - 1;
                self.schedule_task(id);
            }
            Command::Stop => {
                self.shutdown();
            }
        }
        Ok(())
    }

    fn check_timers(&mut self) -> PyResult<()> {
        if let Some(next_timer) = self.timers.peek() {
            let now = Instant::now();
            if next_timer.when <= now {
                let timer = self.timers.pop().unwrap();
                self.task_deq.push_back(timer.task_id);
            }
        }
        Ok(())
    }

    fn run_forever(&mut self, py: Python) -> PyResult<()> {
        loop {
            select! {
                recv(self.wake_rx) -> msg => {
                    if let Ok((id, res_result)) = msg {
                        self.handle_wake(id, res_result)?;
                    }
                }
                recv(self.io_rx) -> msg => {
                    if let Ok((id, result)) = msg {
                        self.handle_io(id, result, py)?;
                    }
                }
                recv(self.cmd_rx) -> msg => {
                    if let Ok(cmd) = msg {
                        self.handle_command(cmd, py)?;
                    }
                }
                default(Duration::from_millis(10)) => {
                    self.check_timers()?;
                }
            }

            while let Some(id) = self.task_deq.pop_front() {
                self.run_task(py, id)?;
            }

            if self.tasks_hm.is_empty() && self.pending_io.is_empty() {
                if self.cmd_rx.is_empty() {
                    break;
                }
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
    event_loop: RefCell<EventLoop>,
    cmd_tx: Sender<Command>,
}

impl RustEventLoop {
    pub fn new(py: Python) -> PyResult<Self> {
        let (cmd_tx, cmd_rx) = cb::unbounded();
        let (wake_tx, wake_rx) = cb::unbounded();
        let (io_tx, io_rx) = cb::unbounded();

        

        let event_loop = EventLoop::new(py, wake_rx, wake_tx.clone(), io_rx, io_tx.clone(), cmd_rx)?;

        Ok(Self {
            event_loop: RefCell::new(event_loop),
            cmd_tx,
        })
    }

    pub fn create_task(&self, gen: &Bound<'_, PyAny>, py: Python) -> RustFuture {
        self.event_loop.borrow_mut().create_task(gen, py)
    }

    pub fn spawn(&self, gen: &Bound<'_, PyAny>) -> PyResult<()> {
        let unbound = gen.clone().unbind();
        self.cmd_tx.send(Command::Spawn(unbound)).map_err(|_| {
            PyRuntimeError::new_err("Event loop command channel closed")
        })?;
        Ok(())
    }

    pub fn run_forever(&self, py: Python) -> PyResult<()> {
        self.event_loop.borrow_mut().run_forever(py)
    }

    pub fn run_until_complete(&self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.event_loop.borrow_mut().run_until_complete(coro, py)
    }

    pub fn stop(&self) -> PyResult<()> {
        self.cmd_tx.send(Command::Stop).map_err(|_| {
            PyRuntimeError::new_err("Event loop command channel closed")
        })?;
        Ok(())
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
        slf.borrow().loop_impl.spawn(gen)
    }

    fn run_forever(&self, py: Python) -> PyResult<()> {
        self.loop_impl.run_forever(py)
    }

    fn run_until_complete(&self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.loop_impl.run_until_complete(coro, py)
    }

    fn stop(&self) -> PyResult<()> {
        self.loop_impl.stop()
    }
}