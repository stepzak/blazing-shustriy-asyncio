use mio;
use pyo3::{IntoPyObjectExt, exceptions::{PyRuntimeError, PyStopIteration}, prelude::*};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

#[pyclass(unsendable)]
pub struct AcceptOp {
    pub listener_arc: Arc<Mutex<mio::net::TcpListener>>,
}

#[pymethods]
impl AcceptOp {
    pub fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        Ok(Some(slf.into_py_any(py)?))
    }

    pub fn send(&self, _val: PyObject, py: Python) -> PyResult<()> {
        Err(PyStopIteration::new_err(py.None()))
    }
}

#[pyclass(unsendable)]
pub struct ConnectOp {
    pub addr: SocketAddr,
}

#[pymethods]
impl ConnectOp {
    pub fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __next__(slf: PyRef<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        Ok(Some(slf.into_py_any(py)?))
    }
}

#[pyclass(unsendable)]
pub struct ReadOp {
    pub stream_arc: Arc<Mutex<mio::net::TcpStream>>,
    pub size: usize,
}

#[pymethods]
impl ReadOp {
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    pub fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    pub fn __next__(slf: PyRef<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        Ok(Some(slf.into_py_any(py)?))
    }
}

#[pyclass(unsendable)]
pub struct WriteOp {
    pub stream_arc: Arc<Mutex<mio::net::TcpStream>>,
    pub data: Vec<u8>,
}

#[pymethods]
impl WriteOp {
    pub fn __await__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    pub fn __next__(slf: PyRef<'_, Self>, py: Python) -> PyResult<Option<PyObject>> {
        Ok(Some(slf.into_py_any(py)?))
    }
}

#[pyclass(unsendable)]
pub struct PyTcpListener {
    pub inner: Option<Arc<Mutex<mio::net::TcpListener>>>,
}

impl From<Arc<Mutex<mio::net::TcpListener>>> for PyTcpListener {
    fn from(value: Arc<Mutex<mio::net::TcpListener>>) -> Self {
        PyTcpListener { inner: Some(value) }
    }
}

#[pymethods]
impl PyTcpListener {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(PyTcpListener { inner: None })
    }

    pub fn bind(&mut self, addr: &str) -> PyResult<()> {
        let sock_addr = addr.parse::<SocketAddr>()?;
        let socket = Socket::new(
            Domain::for_address(sock_addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        socket.set_nonblocking(true)?;
        socket.bind(&sock_addr.into())?;
        socket.listen(1024)?;
        let mio_socket = mio::net::TcpListener::from_std(socket.into());
        self.inner = Some(Arc::new(Mutex::new(mio_socket)));
        Ok(())
    }

    pub fn accept(&self) -> PyResult<AcceptOp> {
        match &self.inner {
            Some(arc) => Ok(AcceptOp {
                listener_arc: arc.clone(),
            }),
            None => Err(PyRuntimeError::new_err("Socket not yet bound")),
        }
    }
}

#[pyclass(unsendable)]
pub struct PyTcpStream {
    pub inner: Option<Arc<Mutex<mio::net::TcpStream>>>,
}

impl From<mio::net::TcpStream> for PyTcpStream {
    fn from(value: mio::net::TcpStream) -> Self {
        PyTcpStream {
            inner: Some(Arc::new(Mutex::new(value))),
        }
    }
}

impl From<Arc<Mutex<mio::net::TcpStream>>> for PyTcpStream {
    fn from(value: Arc<Mutex<mio::net::TcpStream>>) -> Self {
        PyTcpStream { inner: Some(value) }
    }
}

impl PyTcpStream {
    pub fn is_connected(&self) -> bool {
        self.inner.is_some()
    }
}

#[pymethods]
impl PyTcpStream {
    #[new]
    pub fn new() -> Self {
        PyTcpStream { inner: None }
    }

    pub fn connect(&self, addr: &str) -> PyResult<ConnectOp> {
        let socket_addr = addr.parse::<SocketAddr>()?;
        Ok(ConnectOp { addr: socket_addr })
    }

    pub fn read(&self, size: usize) -> PyResult<ReadOp> {
        match &self.inner {
            Some(x) => Ok(ReadOp {
                size,
                stream_arc: x.clone(),
            }),
            _ => Err(PyRuntimeError::new_err("Stream is not connected")),
        }
    }

    pub fn write(&self, data: Vec<u8>) -> PyResult<WriteOp> {
        match &self.inner {
            Some(x) => Ok(WriteOp {
                stream_arc: x.clone(),
                data,
            }),
            _ => Err(PyRuntimeError::new_err("Stream is not connected")),
        }
    }
}
