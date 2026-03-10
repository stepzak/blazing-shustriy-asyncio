use std::{collections::HashMap, sync::Arc};

use crate::{
    core::task::TaskId,
    http::{response::BlazingResponse, router::RustRouter},
};
use pyo3::{prelude::*, types::PyTuple};
use tokio::sync::oneshot;

pub enum Command {
    Spawn {
        coro: Py<PyAny>,
        id: TaskId,
    },
    CallSoon {
        callback: Py<PyAny>,
        args: Py<PyTuple>
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
