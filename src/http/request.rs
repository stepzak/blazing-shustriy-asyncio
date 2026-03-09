use std::collections::HashMap;

use pyo3::prelude::*;

#[pyclass]
pub struct BlazingRequest {
    #[pyo3(get)]
    pub method: String,

    #[pyo3(get)]
    pub route: String,

    #[pyo3(get)]
    pub query: HashMap<String, String>,

    #[pyo3(get)]
    pub headers: Vec<(String, String)>,

    #[pyo3(get)]
    pub body: Vec<u8>,
}

#[pymethods]
impl BlazingRequest {
    #[new]
    pub fn new(
        method: String,
        route: String,
        query: HashMap<String, String>,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Self {
        BlazingRequest {
            method,
            route,
            query,
            headers,
            body,
        }
    }
}
