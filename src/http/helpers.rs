use pyo3::prelude::*;

use crate::http::response::BlazingResponse;

pub fn format_http_response(resp: &BlazingResponse) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(1024);

    let status_text = get_status_text(resp.status_code);
    buffer.extend_from_slice(b"HTTP/1.1 ");
    buffer.extend_from_slice(resp.status_code.to_string().as_bytes());
    buffer.extend_from_slice(b" ");
    buffer.extend_from_slice(status_text.as_bytes());
    buffer.extend_from_slice(b"\r\n");

    let mut headers = resp.headers.clone();
    if !headers.contains_key("content-type") {
        if let Some(body) = &resp.body {
            if !body.is_empty() {
                if body.starts_with(b"{") || body.starts_with(b"[") {
                    headers.insert("content-type".to_string(), "application/json".to_string());
                } else {
                    headers.insert(
                        "content-type".to_string(),
                        "text/plain; charset=utf-8".to_string(),
                    );
                }
            }
        }
    }

    for (key, value) in headers.iter() {
        buffer.extend_from_slice(key.as_bytes());
        buffer.extend_from_slice(b": ");
        buffer.extend_from_slice(value.as_bytes());
        buffer.extend_from_slice(b"\r\n");
    }

    let body_len = resp.body.as_ref().map(|b| b.len()).unwrap_or(0);
    buffer.extend_from_slice(b"Content-Length: ");
    buffer.extend_from_slice(body_len.to_string().as_bytes());
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(b"\r\n");

    if let Some(body) = &resp.body {
        buffer.extend_from_slice(body);
    }

    buffer
}

fn get_status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown Status",
    }
}

pub fn format_404_error() -> Vec<u8> {
    let body = b"Not Found";
    let mut res = Vec::with_capacity(150);
    res.extend_from_slice(b"HTTP/1.1 404 Not Found\r\n");
    res.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n");
    res.extend_from_slice(b"Content-Length: 9\r\n");
    res.extend_from_slice(b"Connection: close\r\n");
    res.extend_from_slice(b"\r\n");
    res.extend_from_slice(body);
    res
}

pub fn format_500_error() -> Vec<u8> {
    let body = b"Internal Server Error";
    let mut res = Vec::with_capacity(170);
    res.extend_from_slice(b"HTTP/1.1 500 Internal Server Error\r\n");
    res.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n");
    res.extend_from_slice(b"Content-Length: 21\r\n");
    res.extend_from_slice(b"Connection: close\r\n");
    res.extend_from_slice(b"\r\n");
    res.extend_from_slice(body);
    res
}

pub fn format_405_error(allowed_methods: &[String]) -> Vec<u8> {
    let body = b"Method Not Allowed";
    let allowed_header = allowed_methods.join(", ");

    let mut res = Vec::with_capacity(200);
    res.extend_from_slice(b"HTTP/1.1 405 Method Not Allowed\r\n");
    res.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n");
    res.extend_from_slice(format!("Allow: {}\r\n", allowed_header).as_bytes());
    res.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    res.extend_from_slice(b"Connection: close\r\n");
    res.extend_from_slice(b"\r\n");
    res.extend_from_slice(body);
    res
}

pub fn format_400_error() -> Vec<u8> {
    let body = b"Bad Request";
    let mut res = Vec::with_capacity(150);
    res.extend_from_slice(b"HTTP/1.1 400 Bad Request\r\n");
    res.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n");
    res.extend_from_slice(b"Content-Length: 11\r\n");
    res.extend_from_slice(b"Connection: close\r\n");
    res.extend_from_slice(b"\r\n");
    res.extend_from_slice(body);
    res
}

pub fn convert_to_response(py: Python<'_>, result_obj: Py<PyAny>) -> BlazingResponse {
    let bound = result_obj.bind(py);

    if let Ok(res_bound) = bound.cast::<BlazingResponse>() {
        return res_bound.extract().unwrap();
    }

    let is_dict = bound.is_instance_of::<pyo3::types::PyDict>();
    let is_list = bound.is_instance_of::<pyo3::types::PyList>();

    if is_dict || is_list {
        if let Ok(res) = BlazingResponse::json(200, None, &bound) {
            return res;
        }
    }
    let body_str = bound
        .str()
        .and_then(|s| s.extract::<String>())
        .unwrap_or_else(|_| "OK".to_string());

    BlazingResponse::new(
        200,
        Some(body_str.into_bytes()),
        std::collections::HashMap::new(),
    )
}
