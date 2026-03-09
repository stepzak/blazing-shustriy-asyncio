use bytes::Bytes;
use pyo3::prelude::*;

use crate::http::response::BlazingResponse;

pub fn format_http_response(resp: &BlazingResponse) -> Bytes {
    let body_len = resp.body.as_ref().map(|b| b.len()).unwrap_or(0);
    let mut buffer = Vec::with_capacity(512 + body_len);

    buffer.extend_from_slice(b"HTTP/1.1 ");
    let mut itoa_buf = itoa::Buffer::new();
    buffer.extend_from_slice(itoa_buf.format(resp.status_code).as_bytes());
    buffer.extend_from_slice(b" ");
    buffer.extend_from_slice(get_status_text(resp.status_code).as_bytes());
    buffer.extend_from_slice(b"\r\n");

    let mut has_content_type = false;
    for (key, value) in &resp.headers {
        if key.eq_ignore_ascii_case("content-type") {
            has_content_type = true;
        }
        buffer.extend_from_slice(key.as_bytes());
        buffer.extend_from_slice(b": ");
        buffer.extend_from_slice(value.as_bytes());
        buffer.extend_from_slice(b"\r\n");
    }

    if !has_content_type && body_len > 0 {
        if let Some(body) = &resp.body {
            buffer.extend_from_slice(b"Content-Type: ");
            if body.starts_with(b"{") || body.starts_with(b"[") {
                buffer.extend_from_slice(b"application/json\r\n");
            } else {
                buffer.extend_from_slice(b"text/plain; charset=utf-8\r\n");
            }
        }
    }
    buffer.extend_from_slice(b"Content-Length: ");
    buffer.extend_from_slice(itoa_buf.format(body_len).as_bytes());
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(b"\r\n");

    if let Some(body) = &resp.body {
        buffer.extend_from_slice(body);
    }

    buffer.into()
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

pub fn format_404_error() -> Bytes {
    let body = b"Not Found";
    let mut res = Vec::with_capacity(150);
    res.extend_from_slice(b"HTTP/1.1 404 Not Found\r\n");
    res.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n");
    res.extend_from_slice(b"Content-Length: 9\r\n");
    res.extend_from_slice(b"Connection: close\r\n");
    res.extend_from_slice(b"\r\n");
    res.extend_from_slice(body);
    res.into()
}

pub fn format_500_error() -> Bytes {
    let body = b"Internal Server Error";
    let mut res = Vec::with_capacity(170);
    res.extend_from_slice(b"HTTP/1.1 500 Internal Server Error\r\n");
    res.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n");
    res.extend_from_slice(b"Content-Length: 21\r\n");
    res.extend_from_slice(b"Connection: close\r\n");
    res.extend_from_slice(b"\r\n");
    res.extend_from_slice(body);
    res.into()
}

pub fn format_405_error(allowed_methods: &[String]) -> Bytes {
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
    res.into()
}

pub fn format_400_error() -> Bytes {
    let body = b"Bad Request";
    let mut res = Vec::with_capacity(150);
    res.extend_from_slice(b"HTTP/1.1 400 Bad Request\r\n");
    res.extend_from_slice(b"Content-Type: text/plain; charset=utf-8\r\n");
    res.extend_from_slice(b"Content-Length: 11\r\n");
    res.extend_from_slice(b"Connection: close\r\n");
    res.extend_from_slice(b"\r\n");
    res.extend_from_slice(body);
    res.into()
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
    let body_bytes = if let Ok(py_str) = bound.cast::<pyo3::types::PyString>() {
        py_str.to_str().unwrap_or("OK").as_bytes().to_vec()
    } else if let Ok(py_bytes) = bound.cast::<pyo3::types::PyBytes>() {
        py_bytes.as_bytes().to_vec()
    } else {
        b"OK".to_vec()
    };

    BlazingResponse::new(200, Some(body_bytes), Vec::new())
}
