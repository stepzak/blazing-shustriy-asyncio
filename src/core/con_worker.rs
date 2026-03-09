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
        helpers::{self, format_500_error, format_http_response},
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
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        buffered_bytes += n;
        let (parse_res, header_len) = {
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut req = httparse::Request::new(&mut headers);
            match req.parse(&buf[..buffered_bytes]) {
                Ok(httparse::Status::Complete(res)) => {
                    let method = req.method.unwrap_or("GET");
                    let route = req.path.unwrap_or("/");
                    if route == "/test_raw" {
                        let response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK";

                        if stream.write_all(response).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    let header_list: Vec<(String, String)> = req
                        .headers
                        .iter()
                        .map(|h| {
                            (
                                h.name.to_string(),
                                String::from_utf8_lossy(h.value).into_owned(),
                            )
                        })
                        .collect();
                    (Some((method, route, header_list)), res)
                }
                Ok(httparse::Status::Partial) => (None, 0),
                Err(_) => (None, 0),
            }
        };
        if let Some((method, route, headers)) = parse_res {
            let body = buf[header_len..buffered_bytes].to_vec();
            let match_route = router_lock.find_route(&method, &route);

            match match_route {
                RouteMatch::Found { handler_id, params } => {
                    let (res_tx, res_rx) = tokio::sync::oneshot::channel();

                    let cmd = Command::ExecuteHttp {
                        handler_id,
                        arc_router: router_lock.clone(),
                        method: method.to_string(),
                        path: route.to_string(),
                        query: params,
                        body,
                        response_tx: res_tx,
                        headers,
                    };

                    if cmd_tx.send(cmd).is_ok() {
                        if let Ok(result) = res_rx.await {
                            let resp_bytes = match result {
                                Ok(res) => format_http_response(&res),
                                Err(_) => format_500_error(),
                            };
                            let _ = stream.write_all(&resp_bytes).await;
                        }
                    }
                }
                RouteMatch::NotFound => {
                    let _ = stream.write_all(&helpers::format_404_error()).await;
                }
                RouteMatch::MethodNotAllowed { allowed_methods } => {
                    let _ = stream
                        .write_all(&helpers::format_405_error(&allowed_methods))
                        .await;
                }
            }

            if header_len < buffered_bytes {
                buf.copy_within(header_len..buffered_bytes, 0);
                buffered_bytes -= header_len;
            } else {
                buffered_bytes = 0;
            }
        } else if buffered_bytes >= buf.len() {
            let _ = stream
                .write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n")
                .await;
            break;
        }
    }
}
