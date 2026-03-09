use pyo3::prelude::*;

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct BlazingResponse {
    #[pyo3(get, set)]
    pub status_code: u16,

    #[pyo3(get, set)]
    pub body: Option<Vec<u8>>,

    #[pyo3(get, set)]
    pub headers: Vec<(String, String)>,
}

#[pymethods]
impl BlazingResponse {
    #[new]
    pub fn new(status_code: u16, body: Option<Vec<u8>>, headers: Vec<(String, String)>) -> Self {
        BlazingResponse {
            status_code,
            body,
            headers,
        }
    }

    pub fn get_header(&self, header_name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(header_name))
            .map(|(_, v)| v.as_str())
    }

    #[staticmethod]
    pub fn json(
        py: Python<'_>,
        status_code: u16,
        headers: Option<Vec<(String, String)>>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let body_bytes = if let Ok(py_bytes) = data.cast::<pyo3::types::PyBytes>() {
            py_bytes.as_bytes().to_vec()
        } else if data.is_instance_of::<pyo3::types::PyDict>()
            || data.is_instance_of::<pyo3::types::PyList>()
        {
            let orjson = py.import("orjson").or_else(|_| py.import("json"))?;
            let bytes_obj: Bound<'_, pyo3::types::PyBytes> = if orjson.name()? == "orjson" {
                orjson.call_method1("dumps", (data,))?.cast_into()?
            } else {
                orjson
                    .call_method1("dumps", (data,))?
                    .call_method0("encode")?
                    .cast_into()?
            };
            bytes_obj.as_bytes().to_vec()
        } else if let Ok(json_method) = data.getattr("model_dump_json") {
            let t = json_method.call0()?;
            let s = t.cast().unwrap();
            s.to_str()?.as_bytes().to_vec()
        } else {
            data.str()?.to_str()?.as_bytes().to_vec()
        };

        let mut res_headers = headers.unwrap_or_else(|| Vec::with_capacity(2));
        if !res_headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("Content-Type"))
        {
            res_headers.push(("Content-Type".to_string(), "application/json".to_string()));
        }

        Ok(BlazingResponse {
            status_code,
            body: Some(body_bytes),
            headers: res_headers,
        })
    }
}
