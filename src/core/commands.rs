use std::{collections::HashMap, sync::Arc};

use crate::{core::task::TaskId, http::router::RustRouter};
use bytes::Bytes;
use pyo3::prelude::*;
use tokio::sync::mpsc;

pub enum Command {
    Spawn {
        coro: Py<PyAny>,
        id: TaskId,
    },
    Stop,
    ExecuteHttp {
        handler_id: usize,
        arc_router: Arc<RustRouter>,
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        query: HashMap<String, String>,
        body: Vec<u8>,
        response_tx: mpsc::Sender<Bytes>,
    },
}
