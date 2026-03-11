use crossbeam_channel::{self as cb, Receiver, Sender};
use parking_lot::Mutex;
use pyo3::{
    exceptions::{PyConnectionError, PyRuntimeError},
    prelude::*,
    types::{PyByteArray, PyList, PyTuple},
    IntoPyObjectExt,
};
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque},
    iter,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener as TokioListener, TcpStream as TokioStream},
    runtime::Runtime,
    sync::mpsc,
};

use crate::{
    core::{
        commands::Command,
        con_worker::{connection_worker, WorkerMode},
        future::PyFuture,
        io_enums::{ConnectionWorker, IoResult, WorkerCommand},
        net::{
            AcceptIo, BindIo, ConnectIo, ListenerMode, PyTcpListener, PyTcpStream, ReadIo, WriteIo,
        },
        task::{RustTask, StepResult, TaskId},
    },
    http::{
        helpers::convert_to_response,
        request::BlazingRequest,
        router::{PyRouter, RustRouter},
    },
};

struct GatherState {
    total: usize,
    completed: usize,
    results: Option<Vec<Option<Py<PyAny>>>>,
}

impl GatherState {
    fn new(total: usize) -> Self {
        GatherState {
            total,
            completed: 0,
            results: Some(
                iter::repeat_with(|| None as Option<Py<PyAny>>)
                    .take(total)
                    .collect(),
            ),
        }
    }
    fn add_result(&mut self, idx: usize, res: Py<PyAny>) -> Option<Vec<Py<PyAny>>> {
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
    future: Py<PyFuture>,
    pending_val: Option<Py<PyAny>>,
    pending_err: Option<PyErr>,
}

impl TaskEntry {
    fn new(task: RustTask, future: Py<PyFuture>) -> Self {
        TaskEntry {
            task,
            future,
            pending_val: None,
            pending_err: None,
        }
    }

    fn set_result(&self, py: Python, result: Py<PyAny>) -> PyResult<()> {
        self.future.borrow(py).future.set_result(result, py)
    }

    fn set_exception(&self, py: Python, err: PyErr) -> PyResult<()> {
        self.future.borrow(py).future.set_exception(err, py)
    }

    fn add_callback<F>(&self, py: Python, cb: F) -> PyResult<()>
    where
        F: FnOnce(Py<PyAny>, Python) + 'static,
    {
        Ok(self.future.borrow(py).future.add_callback(cb, py))
    }

    fn add_err_callback<F>(&self, py: Python, cb: F) -> PyResult<()>
    where
        F: FnOnce(PyErr, Python) + 'static,
    {
        Ok(self.future.borrow(py).future.add_err_callback(cb, py))
    }

    fn cancel(&self, py: Python) -> bool {
        self.future.borrow(py).future.cancel(py)
    }

    fn cancelled(&self, py: Python) -> bool {
        self.future.borrow(py).future.cancelled()
    }

}

struct ListenerEntry {
    listener: Arc<TokioListener>,
    _addr: SocketAddr,
    _py_obj: Py<PyTcpListener>,
    mode: ListenerMode,
}

struct Callback {
    func: Py<PyAny>,
    args: Py<PyTuple>,
    context: Option<Py<PyAny>>,
}

impl Callback {
    fn call(&self, py: Python) -> PyResult<()> {
        if let Some(ctx) = &self.context {
            ctx.call_method1(
                py,
                "run",
                (self.func.clone_ref(py), self.args.clone_ref(py)),
            )?;
            return Ok(());
        } else {
            self.func.call1(py, (self.args.clone_ref(py),))?;
            return Ok(());
        }
    }
}

struct ScheduledCallback {
    when: Instant,
    callback: Callback,
}

impl Ord for ScheduledCallback {
    fn cmp(&self, other: &Self) -> Ordering {
        other.when.cmp(&self.when)
    }
}

impl PartialOrd for ScheduledCallback {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScheduledCallback {
    fn eq(&self, other: &Self) -> bool {
        self.when == other.when
    }
}

impl Eq for ScheduledCallback {}

struct EventLoop {
    tasks_hm: HashMap<TaskId, TaskEntry>,
    task_deq: VecDeque<TaskId>,
    timers: BTreeMap<Instant, Vec<TaskId>>,
    next_timer: Option<Instant>,
    next_id: TaskId,
    sleep_type: Option<Py<PyAny>>,
    gather_type: Option<Py<PyAny>>,
    future_type: Option<Py<PyAny>>,
    wake_rx: Receiver<(TaskId, Result<Py<PyAny>, PyErr>)>,
    wake_tx: Sender<(TaskId, Result<Py<PyAny>, PyErr>)>,
    pending_io: HashSet<TaskId>,
    tokio_runtime: Runtime,
    io_rx: Receiver<(usize, IoResult)>,
    io_tx: Sender<(usize, IoResult)>,
    cmd_tx: Sender<Command>,
    cmd_rx: Receiver<Command>,
    pending_listeners: HashMap<usize, Py<PyTcpListener>>,
    listeners: HashMap<usize, ListenerEntry>,
    next_listener_id: usize,
    pending_streams: HashMap<usize, Py<PyTcpStream>>,
    next_stream_id: usize,
    connection_workers: HashMap<usize, ConnectionWorker>,
    callbacks: VecDeque<Callback>,
    scheduled_callbacks: BinaryHeap<ScheduledCallback>,
    copy_context_method: Py<PyAny>,
}

impl EventLoop {
    fn new(
        py: Python,
        wake_rx: Receiver<(TaskId, Result<Py<PyAny>, PyErr>)>,
        wake_tx: Sender<(TaskId, Result<Py<PyAny>, PyErr>)>,
        io_rx: Receiver<(usize, IoResult)>,
        io_tx: Sender<(usize, IoResult)>,
        cmd_tx: Sender<Command>,
        cmd_rx: Receiver<Command>,
    ) -> PyResult<Self> {
        let module = PyModule::import(py, "blazing_shustriy_asyncio")?;
        let sleep_t = module.getattr("_AsyncSleep")?.unbind();
        let gather_t = module.getattr("_AsyncGather")?.unbind();
        let cv = py
            .import("contextvars")
            .expect("Failed to import contextvars");
        let copy_context = cv
            .getattr("copy_context")
            .expect("Failed to get copy_context");
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
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        Ok(EventLoop {
            copy_context_method: copy_context.unbind(),
            tasks_hm: HashMap::with_capacity(1024),
            task_deq: VecDeque::with_capacity(1024),
            timers: BTreeMap::new(),
            next_timer: None,
            next_id: 0,
            sleep_type: Some(sleep_t),
            gather_type: Some(gather_t),
            future_type: Some(future_t),
            wake_rx,
            wake_tx,
            pending_io: HashSet::new(),
            tokio_runtime: rt,
            io_rx,
            io_tx,
            cmd_tx,
            cmd_rx,
            pending_listeners: HashMap::new(),
            listeners: HashMap::new(),
            next_stream_id: 0,
            next_listener_id: 0,
            pending_streams: HashMap::new(),
            connection_workers: HashMap::new(),
            callbacks: VecDeque::with_capacity(1024),
            scheduled_callbacks: BinaryHeap::new(),
        })
    }

    fn add_timer(&mut self, when: Instant, task_id: TaskId) {
        self.timers
            .entry(when)
            .or_insert_with(Vec::new)
            .push(task_id);
        self.next_timer = self.timers.keys().next().copied();
    }

    fn check_timers(&mut self) -> Vec<TaskId> {
        let now = Instant::now();
        let mut ready = Vec::new();

        while let Some(&when) = self.next_timer.as_ref() {
            if when <= now {
                if let Some(tasks) = self.timers.remove(&when) {
                    ready.extend(tasks);
                }
                self.next_timer = self.timers.keys().next().copied();
            } else {
                break;
            }
        }

        ready
    }

    fn pop_next_task(&mut self) -> Option<TaskId> {
        self.task_deq.pop_front()
    }

    fn cancel(&mut self, py: Python, id: TaskId) -> bool {
        if let Some(entry) = self.tasks_hm.get_mut(&id) {
            entry.cancel(py)
        } else {
            false
        }
    }

    fn create_task(
        &mut self,
        gen: &Bound<'_, PyAny>,
        py: Python,
        t_id: Option<TaskId>,
    ) -> PyResult<Py<PyFuture>> {
        let coro = if gen.hasattr("__await__").unwrap_or(false) {
            match gen.call_method0("__await__") {
                Ok(x) => x,
                Err(exc) => {
                    let fut = PyFuture::new();
                    let _ = fut.set_exception(py, exc.into_bound_py_any(py).unwrap());
                    return Ok(Py::new(py, fut)?);
                }
            }
        } else {
            gen.clone()
        };
        let id = match t_id {
            Some(x) => x,
            None => {
                self.next_id += 1;
                self.next_id - 1
            }
        };
        let context = self.copy_context_method.bind(py).call0()?.unbind();
        let task = RustTask::new(&coro, context);
        let mut fut = PyFuture::new();
        fut.set_task_id(id);
        let pyfut = Py::new(py, fut)?;
        
        let entry = TaskEntry::new(task, pyfut.clone_ref(py));
        self.tasks_hm.insert(id, entry);

        Ok(pyfut)
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
                    entry.set_result(py, result)?;
                }
            }
            StepResult::Failure(err) => {
                if let Some(entry) = self.tasks_hm.remove(&id) {
                    entry.set_exception(py, err.clone_ref(py))?;
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn run_task(&mut self, py: Python, id: TaskId) -> PyResult<()> {
        let entry = if let Some(x) = self.tasks_hm.get_mut(&id) {
            x
        }
        else {
            return Ok(());
        };
        if entry.cancelled(py) {
            self.pending_io.remove(&id);
            self.tasks_hm.remove(&id);
            return Ok(());
            
        }
        let current_ctx = self.copy_context_method.call0(py)?;
        let current_ctx_bound = current_ctx.bind(py);

        let needs_switch = entry.task.needs_context_switch(current_ctx_bound, py);
        if let Some(err) = entry.pending_err.take() {
            let step_res = { entry.task.throw(err, !needs_switch, py) };
            return self.process_step(id, step_res, py);
        }

        let pending_val = entry.pending_val.take();

        let step_res = { entry.task.step(pending_val, !needs_switch, py) };

        self.process_step(id, step_res, py)
    }

    fn handle_bind(
        &mut self,
        id: TaskId,
        addr: SocketAddr,
        pyclass: Py<PyTcpListener>,
        router: Option<Arc<RustRouter>>,
    ) -> PyResult<()> {
        let mut mode = ListenerMode::Raw;
        if let Some(r) = router {
            mode = ListenerMode::Http(r);
        }
        let tx = self.io_tx.clone();
        self.pending_io.insert(id);
        let listener_id = self.next_listener_id;
        self.next_listener_id += 1;
        self.pending_listeners.insert(listener_id, pyclass);
        self.tokio_runtime.spawn(async move {
            let result = (|| -> Result<TokioListener, Box<dyn std::error::Error>> {
                use socket2::{Domain, Protocol, Socket, Type};

                let domain = if addr.is_ipv4() {
                    Domain::IPV4
                } else {
                    Domain::IPV6
                };
                let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;

                socket.set_reuse_address(true)?;
                #[cfg(unix)]
                socket.set_reuse_port(true)?;

                socket.bind(&addr.into())?;
                socket.listen(1024 * 1024)?;
                socket.set_tcp_nodelay(true)?;

                socket.set_nonblocking(true)?;

                let std_listener: std::net::TcpListener = socket.into();
                Ok(TokioListener::from_std(std_listener)?)
            })();

            match result {
                Ok(listener) => {
                    let _ = tx.send((
                        id,
                        IoResult::Bind {
                            listener_id,
                            listener,
                            addr,
                            mode,
                        },
                    ));
                }
                Err(e) => {
                    let err = PyConnectionError::new_err(format!("Socket error: {}", e));
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
            let listener = Arc::clone(&entry.listener);
            let mode = entry.mode.clone();
            self.tokio_runtime.spawn(async move {
                let res = listener.accept().await;
                match res {
                    Ok((stream, addr)) => {
                        let _ = tx.send((
                            id,
                            IoResult::Accept {
                                stream_id,
                                stream: stream,
                                addr,
                                mode,
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
                            stream: stream,
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

        if let Some(worker) = self.connection_workers.get(&stream_id) {
            match worker
                .cmd_tx
                .send(WorkerCommand::Read { size, task_id: id })
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let err = PyConnectionError::new_err(format!("Read error: {e}"));
                    let _ = tx.send((id, IoResult::Error(err)));
                }
            };
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

        if let Some(worker) = self.connection_workers.get(&stream_id) {
            match worker
                .cmd_tx
                .send(WorkerCommand::Write { data, task_id: id })
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let err = PyConnectionError::new_err(format!("Write error: {e}"));
                    let _ = tx.send((id, IoResult::Error(err)));
                }
            };
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
            let binded = bind.bind(py).borrow();
            let pyroute = binded.router.as_ref();
            let mut router = None;
            if let Some(r) = pyroute {
                let bound = r.bind(py);
                let arc = PyRouter::get_router_arc(&bound)?;
                router = Some(arc);
            }
            return self.handle_bind(id, binded.addr, binded.pyclass.clone_ref(py), router);
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
                self.add_timer(when, id);
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
                        move |res: Py<PyAny>, _| {
                            tx_clone.send((my_id, Ok(res))).ok();
                        },
                        py,
                    );
                    let tx_err_callback = self.wake_tx.clone();
                    inner_future.add_err_callback(
                        move |err: PyErr, _| {
                            tx_err_callback.send((my_id, Err(err))).ok();
                        },
                        py,
                    );

                    return Ok(());
                }
            }
        }
        if val_binded.hasattr("send").unwrap_or(false) {
            let child_id = self.next_id;
            self.next_id += 1;

            match self.create_task(val_binded.cast()?, py, Some(child_id)) {
                Ok(_) => {
                    self.schedule_task(child_id);

                    if let Some(entry) = self.tasks_hm.get(&child_id) {
                        let tx_clone = self.wake_tx.clone();
                        let my_id = id;

                        entry.add_callback(py, move |res, _| {
                            let _ = tx_clone.send((my_id, Ok(res)));
                        })?;

                        let tx_err_callback = self.wake_tx.clone();
                        entry.add_err_callback(py, move |err, _| {
                            let _ = tx_err_callback.send((my_id, Err(err)));
                        })?;
                    }
                }
                Err(e) => {
                    let entry = self.tasks_hm.get_mut(&id).unwrap();
                    entry.pending_err = Some(e);
                    self.task_deq.push_back(id);
                }
            }
        }
        return Ok(());
    }

    fn handle_gather(&mut self, id: TaskId, gather: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        let attr = gather.getattr("coros")?;
        let coros = attr.cast::<PyTuple>()?;
        let total = coros.len();

        let state = Arc::new(RefCell::new(GatherState::new(total)));
        let tx_clone = self.wake_tx.clone();
        let my_id = id;

        for (idx, coro) in coros.iter().enumerate() {
            if !coro.hasattr("send").unwrap_or(false) {
                return Err(PyRuntimeError::new_err("All args must be generators"));
            }
            let child_id = self.next_id;
            self.next_id += 1;
            let _ = self.create_task(coro.cast()?, py, Some(child_id))?;
            self.schedule_task(child_id);
            let entry = self.tasks_hm.get(&child_id).unwrap();
            let st = Arc::clone(&state);
            let tx_cb = tx_clone.clone();
            let my_idx = idx;

            entry.add_callback(py, move |res, _| {
                let maybe_list = {
                    let mut s = st.borrow_mut();
                    s.add_result(my_idx, res)
                };
                if let Some(list) = maybe_list {
                    Python::attach(|py| {
                        let py_list = PyList::new(py, list).unwrap();
                        tx_cb.send((my_id, Ok(py_list.into()))).ok();
                    });
                }
            })?;
            let err_cb = tx_clone.clone();
            entry.add_err_callback(py, move |err, _| {
                err_cb.send((my_id, Err(err))).ok();
            })?;
        }
        Ok(())
    }

    fn handle_wake(&mut self, id: TaskId, res_result: Result<Py<PyAny>, PyErr>) -> PyResult<()> {
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
        self.pending_io.remove(&id);
        match result {
            IoResult::Bind {
                listener_id,
                listener,
                addr,
                mode,
            } => {
                if let Some(pyclass) = self.pending_listeners.remove(&listener_id) {
                    if let Ok(mut py_listener) = pyclass.bind(py).try_borrow_mut() {
                        py_listener.set_listener_id(listener_id);
                    }
                    self.listeners.insert(
                        listener_id,
                        ListenerEntry {
                            listener: Arc::new(listener),
                            _addr: addr,
                            _py_obj: pyclass.clone_ref(py),
                            mode,
                        },
                    );
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
                mode,
            } => {
                let pystream = PyTcpStream {
                    stream_id: Some(stream_id),
                    addr: Some(addr),
                    closed: false,
                };
                let obj = Py::new(py, pystream)?;

                let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

                self.connection_workers.insert(
                    stream_id,
                    ConnectionWorker {
                        _pystream: obj.clone_ref(py),
                        cmd_tx,
                    },
                );
                if let Some(entry) = self.tasks_hm.get_mut(&id) {
                    let result = (obj, addr.to_string()).into_py_any(py).unwrap();
                    entry.pending_val = Some(result);
                    self.task_deq.push_back(id);
                }
                let io_tx = self.io_tx.clone();
                let cmd_tx = self.cmd_tx.clone();
                let wrk_mode = match mode {
                    ListenerMode::Raw => WorkerMode::Default { cmd_rx, io_tx },
                    ListenerMode::Http(r) => WorkerMode::Http { cmd_tx, router: r },
                };
                self.tokio_runtime
                    .spawn(async move { connection_worker(wrk_mode, stream).await });
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

                    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

                    self.connection_workers.insert(
                        stream_id,
                        ConnectionWorker {
                            cmd_tx: cmd_tx,
                            _pystream: pyclass.clone_ref(py),
                        },
                    );
                    let io_tx = self.io_tx.clone();
                    self.tokio_runtime.spawn(async move {
                        connection_worker(WorkerMode::Default { cmd_rx, io_tx }, stream).await
                    });

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
            Command::Spawn { coro, id } => {
                let gen_bound = coro.bind(py);
                self.create_task(gen_bound, py, Some(id))?;
                self.schedule_task(id);
            }
            Command::CallSoon {
                callback,
                args,
                context,
            } => {
                self.callbacks.push_back(Callback {
                    func: callback,
                    args,
                    context,
                });
            }
            Command::CallLater {
                when,
                callback,
                args,
                context,
            } => {
                self.scheduled_callbacks.push(ScheduledCallback {
                    when,
                    callback: Callback {
                        func: callback,
                        args,
                        context,
                    },
                });
            }
            Command::ExecuteHttp {
                handler_id,
                arc_router,
                method,
                path,
                raw_headers,
                query,
                body,
                response_tx,
            } => {
                let handler = arc_router.get_handler(handler_id).unwrap().clone_ref(py);
                let pyreq = BlazingRequest::new(method, path, query, body, raw_headers);
                let shared_tx = Arc::new(Mutex::new(Some(response_tx)));
                let gen = match handler.call1(py, (pyreq,)) {
                    Ok(x) => x,
                    Err(_) => {
                        if let Some(tx) = shared_tx.lock().take() {
                            let _ = tx.send(Err(()));
                        }
                        return Err(PyRuntimeError::new_err(format!(
                            "{} is not a coroutine",
                            handler.getattr(py, "__name__").unwrap().to_string()
                        )));
                    }
                };
                let task_id = self.next_id;
                self.next_id += 1;

                let bound = gen.bind(py);
                let _ = self.create_task(bound, py, Some(task_id))?;
                let entry = self.tasks_hm.get(&task_id).unwrap();
                let tx_clone = shared_tx.clone();
                entry.add_callback(py, move |res: Py<PyAny>, _| {
                    Python::attach(move |py_| {
                        let response = convert_to_response(py_, res);
                        if let Some(tx) = tx_clone.lock().take() {
                            let _ = tx.send(Ok(response));
                        }
                    })
                })?;
                let tx_clone = shared_tx;
                entry.add_err_callback(py, move |_, _| {
                    if let Some(tx) = tx_clone.lock().take() {
                        let _ = tx.send(Err(()));
                    }
                })?;

                self.schedule_task(task_id);
            }
            Command::Stop => {
                self.shutdown();
            }
        }
        Ok(())
    }

    fn run_forever(&mut self, py: Python) -> PyResult<()> {
        loop {
            let mut had_events = false;
            let now = Instant::now();
            while let Some(task) = self.scheduled_callbacks.peek() {
                if task.when <= now {
                    let task = self.scheduled_callbacks.pop().unwrap();
                    self.callbacks.push_back(task.callback);
                } else {
                    break;
                }
            }
            while let Some(cb) = self.callbacks.pop_front() {
                if let Err(e) = cb.call(py) {
                    e.print(py);
                }
            }

            while let Ok((id, res_result)) = self.wake_rx.try_recv() {
                had_events = true;
                self.handle_wake(id, res_result)?;
            }

            while let Ok((id, result)) = self.io_rx.try_recv() {
                had_events = true;
                self.handle_io(id, result, py)?;
            }

            while let Ok(cmd) = self.cmd_rx.try_recv() {
                had_events = true;
                self.handle_command(cmd, py)?;
            }

            let ready_timers = self.check_timers();
            if !ready_timers.is_empty() {
                had_events = true;
                for task_id in ready_timers {
                    self.task_deq.push_back(task_id);
                }
            }

            while let Some(id) = self.pop_next_task() {
                had_events = true;
                self.run_task(py, id)?;
            }

            if self.tasks_hm.is_empty() && self.pending_io.is_empty() {
                if self.cmd_rx.is_empty() {
                    break;
                }
            }

            if !had_events {
                py.check_signals()?;
                let sleep_duration = match self.next_timer {
                    Some(when) => {
                        let now = Instant::now();
                        if when > now {
                            let duration = (when - now).min(Duration::from_millis(5));
                            if !self.pending_io.is_empty() {
                                Some(std::cmp::min(duration, Duration::from_micros(100)))
                            } else {
                                Some(duration)
                            }
                        } else {
                            None
                        }
                    }
                    None => {
                        if !self.pending_io.is_empty() {
                            Some(Duration::from_micros(100))
                        } else {
                            None
                        }
                    }
                };
                if let Some(duration) = sleep_duration {
                    py.detach(|| std::thread::sleep(duration));
                    continue;
                }
            }
        }

        self.shutdown();
        Ok(())
    }

    fn run_until_complete(&mut self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.create_task(coro, py, None)?;
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

        let event_loop = EventLoop::new(
            py,
            wake_rx,
            wake_tx.clone(),
            io_rx,
            io_tx.clone(),
            cmd_tx.clone(),
            cmd_rx,
        )?;

        Ok(Self {
            event_loop: RefCell::new(event_loop),
            cmd_tx,
        })
    }

    pub fn create_task(&self, gen: &Bound<'_, PyAny>, py: Python) -> PyResult<Py<PyFuture>> {
        self.event_loop.borrow_mut().create_task(gen, py, None)
    }

    pub fn call_soon(
        &self,
        callback: Py<PyAny>,
        args: Py<PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.cmd_tx
            .send(Command::CallSoon {
                callback,
                args,
                context,
            })
            .map_err(|_| PyRuntimeError::new_err("Event loop closed"))?;
        Ok(())
    }

    pub fn call_later(
        &self,
        delay: f64,
        callback: Py<PyAny>,
        args: Py<PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.cmd_tx
            .send(Command::CallLater {
                when: Instant::now() + Duration::from_secs_f64(delay),
                callback,
                args,
                context,
            })
            .map_err(|_| PyRuntimeError::new_err("Event loop closed"))?;
        Ok(())
    }

    pub fn spawn(&self, gen: &Bound<'_, PyAny>) -> PyResult<usize> {
        let unbound = gen.clone().unbind();
        let mut loop_ref = self.event_loop.borrow_mut();
        let task_id = loop_ref.next_id;
        loop_ref.next_id += 1;
        drop(loop_ref);
        self.cmd_tx
            .send(Command::Spawn {
                coro: unbound,
                id: task_id,
            })
            .map_err(|_| PyRuntimeError::new_err("Event loop command channel closed"))?;
        Ok(task_id)
    }

    pub fn run_forever(&self, py: Python) -> PyResult<()> {
        self.event_loop.borrow_mut().run_forever(py)
    }

    pub fn run_until_complete(&self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.event_loop.borrow_mut().run_until_complete(coro, py)
    }

    pub fn cancel(&self, py: Python, task_id: TaskId) -> bool {
        self.event_loop.borrow_mut().cancel(py, task_id)
    }

    pub fn stop(&self) -> PyResult<()> {
        self.cmd_tx
            .send(Command::Stop)
            .map_err(|_| PyRuntimeError::new_err("Event loop command channel closed"))?;
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

    fn create_task(slf: &Bound<'_, Self>, gen: &Bound<'_, PyAny>) -> PyResult<Py<PyFuture>> {
        let py = slf.py();
        let fut = slf.borrow().loop_impl.create_task(gen, py)?;
        Ok(fut)
    }

    fn call_soon(
        &self,
        callback: Py<PyAny>,
        args: Py<PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.loop_impl.call_soon(callback, args, context)
    }

    fn call_later(
        &self,
        delay: f64,
        callback: Py<PyAny>,
        args: Py<PyTuple>,
        context: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        self.loop_impl.call_later(delay, callback, args, context)
    }

    fn spawn(slf: &Bound<'_, Self>, gen: &Bound<'_, PyAny>) -> PyResult<()> {
        slf.borrow().loop_impl.spawn(gen)?;
        Ok(())
    }

    fn run_forever(&self, py: Python) -> PyResult<()> {
        self.loop_impl.run_forever(py)
    }

    fn cancel(&self, py: Python, task_id: TaskId) -> bool {
        self.loop_impl.cancel(py, task_id)
    }

    fn run_until_complete(&self, coro: &Bound<'_, PyAny>, py: Python) -> PyResult<()> {
        self.loop_impl.run_until_complete(coro, py)
    }

    fn stop(&self) -> PyResult<()> {
        self.loop_impl.stop()
    }
}
