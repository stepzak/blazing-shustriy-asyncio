use std::{collections::HashMap, sync::Arc, time::Instant};

use crate::{
    core::{future::PyFuture, task::TaskId},
    http::{response::BlazingResponse, router::RustRouter},
};
use pyo3::{prelude::*, types::PyTuple};
use tokio::sync::oneshot;

pub enum Command {
    Spawn {
        coro: Py<PyAny>,
        tx: oneshot::Sender<TaskId>,
    },
    CallSoon {
        callback: Py<PyAny>,
        args: Py<PyTuple>,
        context: Option<Py<PyAny>>,
        tx: oneshot::Sender<usize>,
    },
    CallLater {
        when: Instant,
        callback: Py<PyAny>,
        args: Py<PyTuple>,
        context: Option<Py<PyAny>>,
        tx: oneshot::Sender<usize>,
    },
    CancelHandle {
        handle_id: usize,
    },
    CreateTask {
        coro: Py<PyAny>,
        tx: oneshot::Sender<Py<PyFuture>>,
    },
    Stop,
    ExecuteHttp {
        handler_id: usize,
        arc_router: Arc<RustRouter>,
        method: String,
        path: String,
        raw_headers: Vec<u8>,
        query: HashMap<String, String>,
        body: Vec<u8>,
        response_tx: oneshot::Sender<Result<BlazingResponse, ()>>,
    },
}
