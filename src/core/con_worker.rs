use std::sync::Arc;

use crossbeam_channel::Sender;
use pyo3::exceptions::PyConnectionError;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream as TokioStream,
    sync::mpsc,
};

use crate::{
    core::{
        commands::Command,
        io_enums::{IoResult, WorkerCommand},
    },
    http::{
        helpers::{self},
        router::{RouteMatch, RustRouter},
    },
};

pub enum WorkerMode {
    Default {
        cmd_rx: mpsc::UnboundedReceiver<WorkerCommand>,
        io_tx: Sender<(usize, IoResult)>,
    },
    Http {
        cmd_tx: Sender<Command>,
        router: Arc<RustRouter>,
    },
}

pub async fn connection_worker(mode: WorkerMode, stream: TokioStream) {
    match mode {
        WorkerMode::Default { cmd_rx, io_tx } => default_worker(stream, cmd_rx, io_tx).await,
        WorkerMode::Http { cmd_tx, router } => http_worker(stream, router, cmd_tx).await,
    };
}

async fn default_worker(
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

async fn http_worker(
    mut stream: TokioStream,
    router_lock: Arc<RustRouter>,
    cmd_tx: Sender<Command>,
) {
    let mut buf = vec![0u8; 8192];
    let mut buffered_bytes = 0;

    loop {
        let n = match stream.read(&mut buf[buffered_bytes..]).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        buffered_bytes += n;

        loop {
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut req = httparse::Request::new(&mut headers);

            let header_len = match req.parse(&buf[..buffered_bytes]) {
                Ok(httparse::Status::Complete(len)) => len,
                Ok(httparse::Status::Partial) => break,
                Err(_) => {
                    buffered_bytes = 0;
                    break;
                }
            };

            let mut body_len = 0;
            for h in req.headers.iter() {
                if h.name.eq_ignore_ascii_case("content-length") {
                    if let Ok(s) = std::str::from_utf8(h.value) {
                        body_len = s.parse::<usize>().unwrap_or(0);
                    }
                }
            }

            let total_len = header_len + body_len;
            if buffered_bytes < total_len {
                break;
            }

            let method = req.method.unwrap_or("GET");
            let route = req.path.unwrap_or("/");
            let raw_headers = buf[..header_len].to_vec();
            let body = buf[header_len..total_len].to_vec();

            match router_lock.find_route(method, route) {
                RouteMatch::Found { handler_id, params } => {
                    let (res_tx, res_rx) = tokio::sync::oneshot::channel();

                    let cmd = Command::ExecuteHttp {
                        handler_id,
                        arc_router: router_lock.clone(),
                        method: method.to_owned(),
                        path: route.to_owned(),
                        query: params,
                        body,
                        response_tx: res_tx,
                        raw_headers,
                    };

                    if cmd_tx.send(cmd).is_ok() {
                        if let Ok(Ok(res)) = res_rx.await {
                            let out_buf = helpers::format_http_response(&res);
                            if stream.write_all(&out_buf).await.is_err() {
                                break;
                            }
                        } else {
                            let out_buf = helpers::format_500_error();
                            if stream.write_all(&out_buf).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                RouteMatch::NotFound => {
                    let out_buf = helpers::format_404_error();
                    let _ = stream.write_all(&out_buf).await;
                }
                RouteMatch::MethodNotAllowed { allowed_methods } => {
                    let out_buf = helpers::format_405_error(&allowed_methods);
                    let _ = stream.write_all(&out_buf).await;
                }
            }

            if total_len < buffered_bytes {
                buf.copy_within(total_len..buffered_bytes, 0);
                buffered_bytes -= total_len;
            } else {
                buffered_bytes = 0;
                break;
            }
        }

        if buffered_bytes >= buf.len() {
            let _ = stream
                .write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n")
                .await;
            break;
        }
    }
}
