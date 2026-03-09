use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

#[pyclass]
pub struct BlazingRequest {
    #[pyo3(get)]
    pub method: String,
    #[pyo3(get)]
    pub route: String,
    #[pyo3(get)]
    pub query: HashMap<String, String>,
    #[pyo3(get)]
    pub body: Vec<u8>,

    raw_headers: Vec<u8>,
    headers: Option<Py<PyDict>>,
}

#[pymethods]
impl BlazingRequest {
    #[new]
    pub fn new(
        method: String,
        route: String,
        query: HashMap<String, String>,
        body: Vec<u8>,
        raw_headers: Vec<u8>,
    ) -> Self {
        BlazingRequest {
            method,
            route,
            query,
            body,
            raw_headers,
            headers: None,
        }
    }

    #[getter]
    pub fn headers(&mut self, py: Python) -> PyResult<Py<PyDict>> {
        if let Some(ref h) = self.headers {
            return Ok(h.clone_ref(py));
        }

        let dict = PyDict::new(py);
        let mut headers_arr = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers_arr);

        if let Ok(httparse::Status::Complete(_)) = req.parse(&self.raw_headers) {
            for h in req.headers {
                let key = h.name;
                let val: Bound<'_, PyBytes> = PyBytes::new(py, h.value);
                dict.set_item(key, val)?;
            }
        }

        let py_dict = dict.unbind();
        self.headers = Some(py_dict.clone_ref(py));
        Ok(py_dict)
    }
}
