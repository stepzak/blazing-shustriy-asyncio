use pyo3::{
    exceptions::{PyConnectionError, PyOSError, PyStopIteration},
    prelude::*,
    IntoPyObjectExt,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::{TcpListener as TokioListener, TcpStream as TokioStream};

macro_rules! impl_generator {
    ($name:ident, $field:ident, $type:ty) => {
        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
                if !slf.yielded {
                    slf.yielded = true;
                    let py = slf.py();
                    Ok(Some(slf.$field.clone_ref(py).into_py_any(py)?))
                } else {
                    Err(PyStopIteration::new_err(()))
                }
            }

            fn send(
                mut slf: PyRefMut<'_, Self>,
                _value: Option<PyObject>,
            ) -> PyResult<Option<PyObject>> {
                println!("Send");
                if !slf.yielded {
                    slf.yielded = true;
                    let py = slf.py();
                    Ok(Some(slf.$field.clone_ref(py).into_py_any(py)?))
                } else {
                    println!("Stopped iteration");
                    Err(PyStopIteration::new_err(()))
                }
            }

            fn throw(
                _slf: PyRefMut<'_, Self>,
                exc: Bound<'_, PyAny>,
            ) -> PyResult<Option<PyObject>> {
                let py_err = PyErr::from_value(exc);
                Err(py_err)
            }
        }
    };

    ($name:ident, $field:ident, $type:ty, arc) => {
        #[pymethods]
        impl $name {
            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PyObject>> {
                if !slf.yielded {
                    slf.yielded = true;
                    let py = slf.py();
                    let op = <$type>::from(slf.$field.clone());
                    Ok(Some(op.into_py_any(py)?))
                } else {
                    Err(PyStopIteration::new_err(()))
                }
            }

            fn send(
                mut slf: PyRefMut<'_, Self>,
                _value: Option<PyObject>,
            ) -> PyResult<Option<PyObject>> {
                if !slf.yielded {
                    slf.yielded = true;
                    let py = slf.py();
                    let op = <$type>::from(slf.$field.clone());
                    Ok(Some(op.into_py_any(py)?))
                } else {
                    Err(PyStopIteration::new_err(()))
                }
            }

            fn throw(
                _slf: PyRefMut<'_, Self>,
                exc: Bound<'_, PyAny>,
            ) -> PyResult<Option<PyObject>> {
                let py_err = PyErr::from_value(exc);
                Err(py_err)
            }
        }
    };
}

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

impl_generator!(BindIoGen, bind_io, BindIo);

#[pyclass]
pub struct ConnectIo {
    pub addr: SocketAddr,
    pub pyclass: Py<PyTcpStream>,
}

#[pymethods]
impl ConnectIo {
    #[new]
    pub fn new(addr: &str, py: Python) -> PyResult<Self> {
        let parsed = addr
            .parse::<SocketAddr>()
            .map_err(|e| PyOSError::new_err(format!("Invalid address: {}", e)))?;

        let py_stream = Py::new(py, PyTcpStream::empty())?;

        Ok(ConnectIo {
            addr: parsed,
            pyclass: py_stream,
        })
    }

    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<ConnectIoGen>> {
        let py = slf.py();
        let gen = ConnectIoGen {
            connect_io: slf.into(),
            yielded: false,
        };
        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct ConnectIoGen {
    connect_io: Py<ConnectIo>,
    yielded: bool,
}

impl_generator!(ConnectIoGen, connect_io, ConnectIo);

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

impl_generator!(AcceptIoGen, listener_arc, AcceptIo, arc);

#[pyclass(unsendable)]
pub struct ReadIo {
    pub size: usize,
    pub stream: Py<PyTcpStream>,
}

impl ReadIo {
    pub fn new(size: usize, stream: Py<PyTcpStream>) -> Self {
        ReadIo { size, stream }
    }
}

#[pymethods]
impl ReadIo {
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<ReadIoGen>> {
        let py = slf.py();
        let gen = ReadIoGen {
            read_io: slf.into(),
            yielded: false,
        };
        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct ReadIoGen {
    read_io: Py<ReadIo>,
    yielded: bool,
}

impl_generator!(ReadIoGen, read_io, ReadIo);

#[pyclass(unsendable)]
pub struct WriteIo {
    pub data: Vec<u8>,
    pub stream: Py<PyTcpStream>,
    pub written: usize,
    pub total: usize,
}

impl WriteIo {
    pub fn new(data: Vec<u8>, stream: Py<PyTcpStream>) -> Self {
        let total = data.len();
        WriteIo {
            data,
            stream,
            written: 0,
            total,
        }
    }

    pub fn remaining(&self) -> usize {
        self.total - self.written
    }

    pub fn is_complete(&self) -> bool {
        self.written >= self.total
    }
}

#[pymethods]
impl WriteIo {
    fn __await__(slf: PyRef<'_, Self>) -> PyResult<Py<WriteIoGen>> {
        let py = slf.py();
        let gen = WriteIoGen {
            write_io: slf.into(),
            yielded: false,
        };
        Ok(Py::new(py, gen)?)
    }
}

#[pyclass]
struct WriteIoGen {
    write_io: Py<WriteIo>,
    yielded: bool,
}

impl_generator!(WriteIoGen, write_io, WriteIo);

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
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(PyTcpListener {
            inner: None,
            addr: String::new(),
        })
    }

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
    pub inner: Option<Arc<TokioStream>>,
    pub addr: Option<SocketAddr>,
    pub closed: bool,
}

impl PyTcpStream {
    pub fn empty() -> Self {
        PyTcpStream {
            inner: None,
            addr: None,
            closed: false,
        }
    }

    pub fn set_stream(&mut self, arc: Arc<TokioStream>) {
        self.inner = Some(arc);
    }

    pub fn set_addr(&mut self, addr: SocketAddr) {
        self.addr = Some(addr);
    }
}

#[pymethods]
impl PyTcpStream {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(PyTcpStream::empty())
    }

    #[staticmethod]
    fn connect(addr: String, py: Python) -> PyResult<ConnectIo> {
        ConnectIo::new(&addr, py)
    }

    fn is_connected(&self) -> PyResult<bool> {
        Ok(self.addr.is_some())
    }

    fn peer_addr(&self) -> PyResult<String> {
        match self.addr {
            Some(addr) => Ok(addr.to_string()),
            None => Err(PyOSError::new_err("Not connected")),
        }
    }

    fn read(slf: PyRef<'_, Self>, size: usize) -> PyResult<ReadIo> {
        Ok(ReadIo {
            size,
            stream: slf.into(),
        })
    }

    fn write(slf: PyRef<'_, Self>, data: Vec<u8>) -> PyResult<WriteIo> {
        let l = data.len();
        Ok(WriteIo {
            data,
            stream: slf.into(),
            written: 0,
            total: l,
        })
    }
}
