use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError},
    prelude::*,
};
use pythonize::depythonize;
use serde_json::Value;
use std::collections::HashMap;

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct BlazingResponse {
    #[pyo3(get, set)]
    pub status_code: u16,

    #[pyo3(get, set)]
    pub body: Option<Vec<u8>>,

    #[pyo3(get, set)]
    pub headers: HashMap<String, String>,
}

#[pymethods]
impl BlazingResponse {
    #[new]
    pub fn new(status_code: u16, body: Option<Vec<u8>>, headers: HashMap<String, String>) -> Self {
        BlazingResponse {
            status_code,
            body,
            headers,
        }
    }

    #[staticmethod]
    pub fn json(
        status_code: u16,
        headers: Option<HashMap<String, String>>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let body_bytes = if data.is_instance_of::<pyo3::types::PyBytes>() {
            data.extract::<Vec<u8>>()?
        } else if data.is_instance_of::<pyo3::types::PyDict>()
            || data.is_instance_of::<pyo3::types::PyList>()
        {
            let serde_value: Value = depythonize(&data)
                .map_err(|e| PyTypeError::new_err(format!("Conversion error: {}", e)))?;

            serde_json::to_vec(&serde_value)
                .map_err(|e| PyRuntimeError::new_err(format!("JSON error: {}", e)))?
        } else if data.hasattr("model_dump_json")? {
            let json_str: String = data.call_method0("model_dump_json")?.extract()?;
            json_str.into_bytes()
        } else {
            data.str()?.extract::<String>()?.into_bytes()
        };

        let mut res_headers = headers.unwrap_or_default();
        res_headers
            .entry("Content-Type".to_string())
            .or_insert("application/json".to_string());
        Ok(BlazingResponse {
            status_code,
            body: Some(body_bytes),
            headers: res_headers,
        })
    }
}
