use std::collections::HashMap;

use crate::core::task::TaskId;
use pyo3::prelude::*;
use tokio::sync::mpsc;

pub enum Command {
    Spawn {
        coro: Py<PyAny>,
        id: TaskId,
    },
    Stop,
    ExecuteHttp {
        handler: Py<PyAny>,
        method: String,
        path: String,
        headers: HashMap<String, String>,
        query: HashMap<String, String>,
        body: Vec<u8>,
        response_tx: mpsc::Sender<Vec<u8>>,
    },
}
