use matchit::Router as MatchitRouter;
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

pub enum RouteMatch {
    Found {
        handler_id: usize,
        params: HashMap<String, String>,
    },
    NotFound,
    MethodNotAllowed {
        allowed_methods: Vec<String>,
    },
}

pub struct RustRouter {
    pub routers: HashMap<String, MatchitRouter<usize>>,
    pub handlers_registry: Vec<Py<PyAny>>,
}

impl RustRouter {
    pub fn new() -> Self {
        RustRouter {
            routers: HashMap::new(),
            handlers_registry: Vec::new(),
        }
    }

    pub fn add_route(&mut self, method: String, path: String, handler: Py<PyAny>) -> PyResult<()> {
        let handler_id = self.handlers_registry.len();
        self.handlers_registry.push(handler);

        let method_upper = method.to_uppercase();
        let router = self
            .routers
            .entry(method_upper)
            .or_insert_with(MatchitRouter::new);

        router
            .insert(path, handler_id)
            .map_err(|e| PyValueError::new_err(format!("Route error: {}", e)))?;

        Ok(())
    }

    pub fn find_route(&self, method: &str, path: &str) -> RouteMatch {
        let method_upper = method.to_uppercase();

        if let Some(router) = self.routers.get(&method_upper) {
            if let Ok(m) = router.at(path) {
                let params = m
                    .params
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();

                return RouteMatch::Found {
                    handler_id: *m.value,
                    params,
                };
            }
        }

        let mut allowed = Vec::new();
        for (m, r) in &self.routers {
            if r.at(path).is_ok() {
                allowed.push(m.clone());
            }
        }

        if !allowed.is_empty() {
            RouteMatch::MethodNotAllowed {
                allowed_methods: allowed,
            }
        } else {
            RouteMatch::NotFound
        }
    }

    pub fn get_handler(&self, handler_id: usize) -> Option<&Py<PyAny>> {
        self.handlers_registry.get(handler_id)
    }
}

#[pyclass]
pub struct PyRouter {
    pub inner: Arc<RwLock<RustRouter>>,
}

#[pymethods]
impl PyRouter {
    #[new]
    fn new() -> Self {
        PyRouter {
            inner: Arc::new(RwLock::new(RustRouter::new())),
        }
    }

    fn add_route(&mut self, method: String, path: String, handler: Py<PyAny>) -> PyResult<()> {
        self.inner
            .write()
            .map_err(|_| PyRuntimeError::new_err("Lock poisoned"))?
            .add_route(method, path, handler)
    }
}
