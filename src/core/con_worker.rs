use std::{collections::HashMap, sync::Arc};

use crossbeam_channel::Sender;
use pyo3::{exceptions::PyConnectionError, Python};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream as TokioStream,
    sync::mpsc,
};
use url::Url;

use crate::{
    core::{
        commands::Command,
        io_enums::{IoResult, WorkerCommand},
    },
    http::{
        helpers,
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

struct Route {
    path: String,
    query: HashMap<String, String>,
}

fn parse_url(url: String) -> Result<Route, url::ParseError> {
    let parsed_url = Url::parse(&url)?;
    Ok(Route {
        path: parsed_url.path().to_string(),
        query: parsed_url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    })
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
    router: Arc<RustRouter>,
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
                    let method = req.method.unwrap_or("GET").to_string();
                    let route = req.path.unwrap_or("/").to_string();
                    let header_map: HashMap<String, String> = req
                        .headers
                        .iter()
                        .map(|h| {
                            (
                                h.name.to_string(),
                                String::from_utf8_lossy(h.value).to_string(),
                            )
                        })
                        .collect();
                    (Some((method, route, header_map)), res)
                }
                Ok(httparse::Status::Partial) => (None, 0),
                Err(_) => (None, 0),
            }
        };

        if let Some((method, route, headers)) = parse_res {
            let parsed = match parse_url(route.clone()) {
                Ok(r) => r,
                Err(_) => break,
            };

            let body = buf[header_len..buffered_bytes].to_vec();

            match router.find_route(&method, &route) {
                RouteMatch::Found { handler_id, .. } => {
                    let handler = router.get_handler(handler_id).unwrap();
                    let handler_owned = Python::attach(|py| {
                        handler.clone_ref(py)
                    });
                    let (res_tx, mut res_rx) = tokio::sync::mpsc::channel(1);

                    let cmd = Command::ExecuteHttp {
                        handler: handler_owned,
                        method,
                        path: parsed.path,
                        query: parsed.query,
                        body,
                        response_tx: res_tx,
                        headers,
                    };

                    if cmd_tx.send(cmd).is_ok() {
                        if let Some(x) = res_rx.recv().await {
                            let _ = stream.write_all(&x).await;
                        }
                    }
                }
                RouteMatch::NotFound => {
                    let _ = stream.write_all(&helpers::format_404_error()).await;
                }
                RouteMatch::MethodNotAllowed { allowed_methods } => {
                    let _ = stream.write_all(&helpers::format_405_error(&allowed_methods)).await;
                }
            }

            if header_len < buffered_bytes {
                buf.copy_within(header_len..buffered_bytes, 0);
                buffered_bytes -= header_len;
            } else {
                buffered_bytes = 0;
            }
        } else if buffered_bytes >= buf.len() {
            let _ = stream.write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n").await;
            break;
        }
    }
}
