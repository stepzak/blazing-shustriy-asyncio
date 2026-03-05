use crossbeam_channel::Sender;
use pyo3::exceptions::PyConnectionError;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream as TokioStream,
    sync::mpsc,
};

use crate::core::io_enums::{IoResult, WorkerCommand};

pub async fn connection_worker(
    mut stream: TokioStream,
    mut cmd_rx: mpsc::UnboundedReceiver<WorkerCommand>,
    io_tx: Sender<(usize, IoResult)>,
) {
    let mut buf = vec![0u8; 8192];

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            WorkerCommand::Read { size, task_id } => {
                if size > buf.len() {
                    buf.resize(size, 0u8);
                }
                let res = match stream.read(&mut buf[..size]).await {
                    Ok(0) => {
                        let _ = io_tx.send((task_id, IoResult::Read { data: None }));
                        break;
                    }
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        IoResult::Read { data: Some(data) }
                    }
                    Err(e) => {
                        let err = PyConnectionError::new_err(format!("Read error: {e}"));
                        IoResult::Error(err)
                    }
                };
                let _ = io_tx.send((task_id, res));
            }
            WorkerCommand::Write { data, task_id } => {
                match stream.write_all(&data).await {
                    Ok(_) => {
                        let res = IoResult::Write { n: data.len() };
                        let _ = io_tx.send((task_id, res));
                    }
                    Err(e) => {
                        let err = PyConnectionError::new_err(format!("Write error: {e}"));
                        let _ = io_tx.send((task_id, IoResult::Error(err)));
                        break;
                    }
                };
            }
            WorkerCommand::Close => break,
        }
    }
}
