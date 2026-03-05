use std::net::SocketAddr;

use pyo3::{Py, PyErr};
use tokio::{
    net::{TcpListener as TokioListener, TcpStream as TokioStream},
    sync::mpsc,
};

use crate::core::{net::PyTcpStream, task::TaskId};

pub enum IoResult {
    Bind {
        listener_id: usize,
        listener: TokioListener,
        addr: SocketAddr,
    },
    Accept {
        stream_id: usize,
        stream: TokioStream,
        addr: SocketAddr,
    },
    Connect {
        stream_id: usize,
        stream: TokioStream,
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

pub enum WorkerCommand {
    Read { size: usize, task_id: TaskId },
    Write { data: Vec<u8>, task_id: TaskId },
    Close,
}

pub struct ConnectionWorker {
    pub cmd_tx: mpsc::UnboundedSender<WorkerCommand>,
    pub _pystream: Py<PyTcpStream>,
}
