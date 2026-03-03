use std::{net::SocketAddr, sync::Arc};
use tokio::net::{TcpListener as TokioListener, TcpStream as TokioStream};

use pyo3::{
    exceptions::{PyConnectionError, PyOSError},
    prelude::*,
    IntoPyObjectExt,
};

#[pyclass]
pub struct BindIo {
    pub addr: SocketAddr,
    pub pyclass: Py<PyTcpListener>,
}

impl BindIo {
    fn new(addr: SocketAddr, pylistener: Py<PyTcpListener>) -> Self {
        BindIo {
            addr,
            pyclass: pylistener,
        }
    }
}

#[pymethods]
impl BindIo {
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<BindIoGen>> {
        let py = slf.py();
        let gen = BindIoGen {
            bind_io: slf.into(),
            yielded: false,
        };
        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct BindIoGen {
    bind_io: Py<BindIo>,
    yielded: bool,
}

#[pymethods]
impl BindIoGen {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        if !slf.yielded {
            let py = slf.py();
            slf.yielded = true;
            if let Ok(bind_op) = slf.bind_io.extract::<PyRef<'_, BindIo>>(py) {
                let addr = bind_op.addr;
                let listener_ref = bind_op.pyclass.clone_ref(py);

                let result = (listener_ref, addr.to_string()).into_py_any(py).unwrap();
                Some(result)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[pyclass]
pub struct ConnectIo {
    pub addr: SocketAddr,
    pub pyclass: Py<PyTcpStream>,
}

#[pymethods]
impl ConnectIo {
    #[new]
    pub fn new(addr: &str, parent: Py<PyTcpStream>) -> PyResult<Self> {
        let parsed = addr
            .parse::<SocketAddr>()
            .map_err(|e| PyOSError::new_err(format!("Invalid address: {}", e)))?;

        Ok(ConnectIo {
            addr: parsed,
            pyclass: parent,
        })
    }

    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<ConnectIoGen>> {
        let py = slf.py();
        let pyclass = slf.pyclass.clone_ref(py);
        let gen = ConnectIoGen {
            parent: pyclass,
            yielded: false,
        };

        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct ConnectIoGen {
    parent: Py<PyTcpStream>,
    yielded: bool,
}

#[pymethods]
impl ConnectIoGen {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        if !slf.yielded {
            slf.yielded = true;
            let py = slf.py();
            let r = slf.parent.clone_ref(py);
            let addr = r.borrow(py).addr.clone();
            let op = ConnectIo { addr, pyclass: r };

            Some(op.into_py_any(py).unwrap())
        } else {
            None
        }
    }
}

#[pyclass(unsendable)]
pub struct AcceptIo {
    pub listener_arc: Arc<TokioListener>,
}

impl From<Arc<TokioListener>> for AcceptIo {
    fn from(value: Arc<TokioListener>) -> Self {
        AcceptIo {
            listener_arc: value,
        }
    }
}

#[pymethods]
impl AcceptIo {
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<AcceptIoGen>> {
        let gen = AcceptIoGen {
            listener_arc: slf.listener_arc.clone(),
            yielded: false,
        };
        Py::new(slf.py(), gen)
    }
}

#[pyclass(unsendable)]
struct AcceptIoGen {
    listener_arc: Arc<TokioListener>,
    yielded: bool,
}

#[pymethods]
impl AcceptIoGen {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyObject> {
        let py = slf.py();
        if !slf.yielded {
            slf.yielded = true;
            let op = AcceptIo::from(slf.listener_arc.clone());
            Some(op.into_py_any(py).unwrap())
        } else {
            None
        }
    }
}

#[pyclass(unsendable)]
pub struct PyTcpListener {
    pub inner: Option<Arc<TokioListener>>,
    pub addr: String,
}

impl PyTcpListener {
    pub fn set_listener(&mut self, arc: Arc<TokioListener>) {
        self.inner = Some(arc);
    }
}

#[pymethods]
impl PyTcpListener {
    pub fn bind(slf: PyRef<'_, Self>, addr: &str) -> PyResult<BindIo> {
        let parsed = addr.parse::<SocketAddr>()?;
        Ok(BindIo::new(parsed, slf.into()))
    }

    pub fn accept(&self) -> PyResult<AcceptIo> {
        match &self.inner {
            Some(inner) => Ok(AcceptIo {
                listener_arc: inner.clone(),
            }),
            None => Err(PyConnectionError::new_err("Socket not yet bound")),
        }
    }
}

#[pyclass(unsendable)]
pub struct PyTcpStream {
    pub inner: Arc<TokioStream>,
    pub addr: SocketAddr,
}

impl PyTcpStream {
    pub fn set_stream(&mut self, arc: Arc<TokioStream>) {
        self.inner = arc;
    }

    pub fn set_addr(&mut self, addr: SocketAddr) {
        self.addr = addr;
    }
}

#[pymethods]
impl PyTcpStream {
    fn connect(slf: PyRef<'_, Self>) -> PyResult<ConnectIo> {
        Ok(ConnectIo {
            addr: slf.addr,
            pyclass: slf.into(),
        })
    }
}
